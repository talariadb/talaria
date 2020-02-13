// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package sqs

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/grab/talaria/internal/config"
)

const (
	defaultBufferSize         = 10
	minMaxNumberOfMessages    = 1
	maxMaxNumberOfMessages    = 10
	minVisibilityTimeoutInSec = 0
	maxVisibilityTimeoutInSec = 12 * 60 * 60 // 12 hour
)

// NewReader returns a reader
func NewReader(c *config.S3SQS, region string) (*Reader, error) {
	const defaultVisibilityTimeout = time.Second * 30

	conf := aws.NewConfig().
		WithRegion(region).
		WithMaxRetries(c.Retries)

	// Create the session
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	consumer := sqs.New(sess)
	visibilityTimeout := defaultVisibilityTimeout
	if c.VisibilityTimeout != nil {
		visibilityTimeout = time.Second * time.Duration(*c.VisibilityTimeout)
	}

	return &Reader{
		sqs:               consumer,
		stopCh:            make(chan struct{}),
		visibilityTimeout: visibilityTimeout,
		queueURL:          c.Queue,
		waitTimeSeconds:   c.WaitTimeout,
	}, nil
}

// Reader represents an SQS reader
type Reader struct {
	sqs               *sqs.SQS
	stopCh            chan struct{}
	visibilityTimeout time.Duration
	queueURL          string
	waitTimeSeconds   int64 // The duration (in seconds) for which the call will wait for a message to arrive
}

// StartPolling messages from SQS. User defines
//  - message attributes to read
//  - max messages per read
//  - sleep time if no messages received
func (r *Reader) StartPolling(maxPerRead, sleepMs int64, attributeNames, messageAttributeNames []*string) <-chan *sqs.Message {
	msgCh := make(chan *sqs.Message, defaultBufferSize)

	go func() {
		for {
			select {
			case <-r.stopCh:
				return
			default:
				messages := r.receive(attributeNames, messageAttributeNames, maxPerRead)
				if len(messages) == 0 {
					<-time.After(time.Duration(sleepMs) * time.Millisecond)
				} else {
					for _, m := range messages {
						msgCh <- m
					}
				}
			}
		}
	}()

	return msgCh
}

// receive reads a message from an sqs queue
func (r *Reader) receive(attributeNames, messageAttributeNames []*string, maxPerRead int64) []*sqs.Message {
	maxPerRead = inRange(maxPerRead, minMaxNumberOfMessages, maxMaxNumberOfMessages)
	visibleFor := inRange(int64(r.visibilityTimeout.Seconds()), minVisibilityTimeoutInSec, maxVisibilityTimeoutInSec)

	input := &sqs.ReceiveMessageInput{
		AttributeNames:        attributeNames,
		MaxNumberOfMessages:   &maxPerRead,
		MessageAttributeNames: messageAttributeNames,
		QueueUrl:              &r.queueURL,
		VisibilityTimeout:     &visibleFor,
		WaitTimeSeconds:       &r.waitTimeSeconds,
	}

	resp, _ := r.sqs.ReceiveMessage(input)
	return resp.Messages
}

// DeleteMessage from SQS (ack)
func (r *Reader) DeleteMessage(msg *sqs.Message) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      &r.queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := r.sqs.DeleteMessage(input)
	return err
}

// Close the reader
func (r *Reader) Close() error {
	close(r.stopCh)
	return nil
}

// inRange checks if input integer is within the [min, max] range
// it returns min when input < min
//    returns max when input > max
func inRange(input, min, max int64) int64 {
	if input < min {
		return min
	}
	if input > max {
		return max
	}
	return input
}
