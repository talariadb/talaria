// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package ingest

//appraise:disable metric/struct_length

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"sync/atomic"
	"time"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/grab/talaria/internal/monitor"
	"golang.org/x/sync/semaphore"
)

const (
	concurrency = 10
	ctxTag      = "ingest"
)

// Storage represents disk storage.
type Storage struct {
	run     int32      // The state flag.
	sqs     Reader     // The SQS reader to use.
	s3      Downloader // The S3 downloader to use.
	monitor monitor.Client
	cancel  context.CancelFunc  // The cancellation function to apply at the end.
	limit   *semaphore.Weighted // The limit of workers
}

// Downloader represents an object downloader
type Downloader interface {
	Download(ctx context.Context, bucket, key string) ([]byte, error)
}

// Reader represents a consumer for SQS
type Reader interface {
	io.Closer
	StartPolling(maxPerRead, sleepMs int64, attributeNames, messageAttributeNames []*string) <-chan *awssqs.Message
	DeleteMessage(msg *awssqs.Message) error
}

// New creates a new ingestion with SQS/S3 files.
func New(reader Reader, downloader Downloader, monitor monitor.Client) *Storage {
	return &Storage{
		sqs:     reader,
		s3:      downloader,
		monitor: monitor,
		limit:   semaphore.NewWeighted(concurrency),
	}
}

// IsConsuming returns whether the storage is consuming or not.
func (s *Storage) IsConsuming() bool {
	return atomic.LoadInt32(&s.run) == 1
}

// Range iterates through the queue, stops only if Close() is called or the f callback
// returns true.
func (s *Storage) Range(f func(v []byte) bool) {
	atomic.StoreInt32(&s.run, 1)

	// Create a cancellation context
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Start draining the queue, asynchronously
	queue := s.sqs.StartPolling(1, 100, nil, nil)
	go s.drain(ctx, queue, f)
}

// drains files from SQS
func (s *Storage) drain(ctx context.Context, queue <-chan *awssqs.Message, handler func(v []byte) bool) {
	const tag = "drain"

	for s.IsConsuming() {
		select {
		case msg := <-queue:
			if msg == nil || msg.Body == nil {
				continue
			}

			// Ack message received
			if err := s.acknowledge(msg); err != nil {
				s.monitor.ErrorWithStats(ctxTag, "ack_sqs", err.Error())
				continue
			}

			// Unmarshal the event
			var events events
			if err := json.Unmarshal([]byte(*msg.Body), &events); err != nil {
				s.monitor.ErrorWithStats(ctxTag, "unmarshal_msg", err.Error())
				continue // Ignore corrupt events
			}

			for _, event := range events.Records {
				bucket := event.S3.Bucket.Name
				key, err := url.QueryUnescape(event.S3.Object.Key)
				if err != nil {
					s.monitor.ErrorWithStats(ctxTag, "query_unescape", err.Error())
					continue
				}

				// Wait until we can proceed
				if err := s.limit.Acquire(ctx, 1); err != nil {
					s.monitor.ErrorWithStats(tag, "limit_acquire", err.Error())
					continue
				}

				go s.ingest(bucket, key, handler)
			}
		}
	}
}

// Acknowledge deletes the message from SQS
func (s *Storage) acknowledge(msg *awssqs.Message) error {
	if msg.ReceiptHandle == nil {
		return nil
	}

	return s.sqs.DeleteMessage(msg)
}

// Ingest downloads an object from S3 and applies a handler to the downloaded
// payload. Few of these can be executed in parallel.
func (s *Storage) ingest(bucket, key string, handler func(v []byte) bool) {
	defer s.monitor.Duration(ctxTag, "ingest", time.Now())

	data, err := s.s3.Download(context.Background(), bucket, key)
	defer s.limit.Release(1)
	if err != nil {
		s.monitor.ErrorWithStats(ctxTag, "[ingest] release", err.Error())
		return
	}

	s.monitor.Info(ctxTag, "[ingest] downloading %v", key)

	// Call the handler
	_ = handler(data)
}

// Close stops consuming
func (s *Storage) Close() {
	atomic.StoreInt32(&s.run, 0)
	s.cancel()
	s.sqs.Close()

	// Wait for ingestion to finish ...
	_ = s.limit.Acquire(context.Background(), concurrency)
	return
}

type events struct {
	Records []struct {
		EventVersion string    `json:"eventVersion"`
		EventSource  string    `json:"eventSource"`
		AwsRegion    string    `json:"awsRegion"`
		EventTime    time.Time `json:"eventTime"`
		EventName    string    `json:"eventName"`
		UserIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"userIdentity"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzRequestID string `json:"x-amz-request-id"`
			XAmzID2       string `json:"x-amz-id-2"`
		} `json:"responseElements"`
		S3 struct {
			S3SchemaVersion string `json:"s3SchemaVersion"`
			ConfigurationID string `json:"configurationId"`
			Bucket          struct {
				Name          string `json:"name"`
				OwnerIdentity struct {
					PrincipalID string `json:"principalId"`
				} `json:"ownerIdentity"`
				Arn string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int    `json:"size"`
				ETag      string `json:"eTag"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}
