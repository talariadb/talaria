// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"bytes"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
)

const ctxTag = "s3"

// Uploader uploads to underlying backend
//go:generate mockery -name=S3Uploader -case underscore -testonly -inpkg
type Uploader interface {
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

// Writer represents a writer for Amazon S3 and compatible storages.
type Writer struct {
	*base.Writer
	monitor  monitor.Monitor
	uploader Uploader
	bucket   string
	prefix   string
	sse      string
}

// New initializes a new S3 writer.
func New(monitor monitor.Monitor, bucket, prefix, region, endpoint, sse, access, secret, filter, encoding string, concurrency int) (*Writer, error) {
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}

	baseWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, errors.Newf("s3: %v", err)
	}

	config := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(strings.HasPrefix(endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(endpoint != ""),
	}

	// Optionally set static credentials
	if access != "" && secret != "" {
		config.WithCredentials(credentials.NewStaticCredentials(access, secret, ""))
	}

	client := s3.New(session.New(), config)
	return &Writer{
		Writer:  baseWriter,
		monitor: monitor,
		uploader: s3manager.NewUploaderWithClient(client, func(u *s3manager.Uploader) {
			u.Concurrency = concurrency
		}),
		bucket: bucket,
		prefix: cleanPrefix(prefix),
		sse:    sse,
	}, nil
}

// Write writes creates object of S3 bucket prefix key in S3Writer bucket with value val
func (w *Writer) Write(key key.Key, blocks []block.Block) error {

	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return err
	}
	start := time.Now()
	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Body:   bytes.NewReader(buffer),
		Key:    aws.String(path.Join(w.prefix, string(key))),
	}

	// Optionally enable server-side encryption
	if w.sse != "" {
		uploadInput.ServerSideEncryption = aws.String(w.sse)
	}

	// Upload to S3
	if _, err := w.uploader.Upload(uploadInput); err != nil {
		w.monitor.Count1(ctxTag, "writeerror")
		return errors.Internal("s3: unable to write", err)
	}
	w.monitor.Histogram(ctxTag, "writelatency", float64(time.Since(start)))
	return nil
}

func cleanPrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}
