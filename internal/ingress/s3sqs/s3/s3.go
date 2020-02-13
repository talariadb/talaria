// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/errors"
)

// All the errors
var (
	// ErrNoSuchBucket is returned when the requested bucket does not exist
	ErrNoSuchBucket = errors.New("bucket does not exist")

	// ErrNoSuchKey is returned when the requested file does not exist
	ErrNoSuchKey = errors.New("key does not exist")
)

// Client interface to interact with S3
type Client interface {
	Upload(ctx context.Context, bucket, key string, body io.Reader, grantReadCanonicalID string) error
	Download(ctx context.Context, bucket, key string) ([]byte, error)
	DownloadLatest(ctx context.Context, bucket, prefix string) ([]byte, error)
	DownloadLatestFolder(ctx context.Context, bucket, prefix string) ([]byte, error)
}

// client represents the storage implementation.
type client struct {
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	awsClient  *s3.S3
	monitor    monitor.Monitor
}

// New a new S3 Client.
func New(region string, retries int, monitor monitor.Monitor, keys ...string) Client {
	conf := aws.NewConfig().
		WithRegion(region).
		WithMaxRetries(retries)

	// Create the session
	sess, err := session.NewSession(conf)
	if err != nil {
		panic(fmt.Errorf("unable to create AWS session: %s", err))
	}

	return NewFromSession(sess, monitor)
}

// NewFromSession a new S3 Client with the supplied AWS session
func NewFromSession(sess *session.Session, monitor monitor.Monitor) Client {
	return &client{
		uploader:   s3manager.NewUploader(sess, func(u *s3manager.Uploader) { u.Concurrency = 128 }),
		downloader: s3manager.NewDownloader(sess, func(d *s3manager.Downloader) { d.Concurrency = 128 }),
		awsClient:  s3.New(sess),
		monitor:    monitor,
	}
}

// Upload attempts to upload a file at a particular key.
func (s *client) Upload(ctx context.Context, bucket, key string, body io.Reader, grantReadCanonicalID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.monitor.Error(errors.Newf("s3: panic recovered. err: %v", r))
		}
	}()

	params := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	if grantReadCanonicalID != "" {
		params.GrantRead = aws.String("id=" + grantReadCanonicalID)
	}

	// Perform an upload.
	_, err = s.uploader.UploadWithContext(ctx, params)

	return // nolint: nakedret
}

// DownloadLatest attempts to download the latest file within a path
func (s *client) DownloadLatest(ctx context.Context, bucket, prefix string) ([]byte, error) {
	_, key, err := s.getLatestKey(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	return s.Download(ctx, bucket, key)
}

// getLatestKey returns latest uploaded key in given bucket
func (s *client) getLatestKey(ctx context.Context, bucket, prefix string) (*s3.ListObjectsV2Output, string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	list, err := s.awsClient.ListObjectsV2WithContext(ctx, input)
	if err != nil {
		s.monitor.Error(errors.Internal("s3: error while listing files/dir", err))
		return nil, "", convertError(err)
	}

	// get latest key
	objects := list.Contents
	var key string
	var latest time.Time
	for _, o := range objects {
		if aws.Int64Value(o.Size) > 0 && aws.TimeValue(o.LastModified).After(latest) {
			key = aws.StringValue(o.Key)
			latest = aws.TimeValue(o.LastModified)
		}
	}

	s.monitor.Debug("s3: latest key (%s) found", key)
	if key == "" {
		return nil, "", ErrNoSuchKey
	}
	return list, key, nil
}

// Download a specific key from the bucket
func (s *client) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	w := new(aws.WriteAtBuffer)
	n, err := s.downloader.DownloadWithContext(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		s.monitor.Error(errors.Internal("s3: error while downloading from s3", err))
		return nil, convertError(err)
	}
	return w.Bytes()[:n], nil
}

// DownloadLatestFolder attempts to download all files in the latest uploaded folder
func (s *client) DownloadLatestFolder(ctx context.Context, bucket, prefix string) ([]byte, error) {
	list, latestKey, err := s.getLatestKey(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	parent := getParentFolder(latestKey)
	// if no parent found, just download latest key
	if parent == "" {
		s.monitor.Debug("s3: no parent found. Downloading single file...", latestKey)
		return s.Download(ctx, bucket, latestKey)
	}

	// download all keys within the parent folder
	w := &aws.WriteAtBuffer{}
	objects := list.Contents
	for _, o := range objects {
		if found := strings.Contains(*o.Key, parent); found {
			s.monitor.Debug("s3: found another file in parent...", parent, *o.Key)
			if err := s.downloadWithWriter(ctx, w, bucket, *o.Key); err != nil {
				return nil, err
			}
		}
	}

	return w.Bytes(), nil
}

func (s *client) downloadWithWriter(ctx context.Context, w *aws.WriteAtBuffer, bucket, key string) error {
	lengths3 := int64(len(w.Bytes()))
	var b []byte
	var err error
	if b, err = s.Download(ctx, bucket, key); err != nil {
		return errors.Internal("s3: unable to download", err)
	}
	if _, err := w.WriteAt(b, lengths3); err != nil {
		return errors.Internal("s3: error while writing to main aws buffer at length", err)
	}

	return nil
}

func getParentFolder(key string) string {
	slice := strings.Split(key, "/")
	size := len(slice)
	// no parent folder to return
	if size == 0 || size == 1 {
		return ""
	}

	return slice[size-2]
}

// getCredentials constructs a static credentials
func getCredentials(keys []string) *credentials.Credentials {
	if len(keys) != 2 {
		panic("bad aws keys")
	}
	creds := credentials.NewStaticCredentials(keys[0], keys[1], "")
	_, err := creds.Get()
	if err != nil {
		panic(fmt.Errorf("bad AWS credentials: %s", err))
	}
	return creds
}

func convertError(err error) error {
	// convert AWS error to internal errors
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchBucket:
			return ErrNoSuchBucket

		case s3.ErrCodeNoSuchKey:
			return ErrNoSuchKey
		}
	}

	return err
}
