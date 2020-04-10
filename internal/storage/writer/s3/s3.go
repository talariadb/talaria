// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grab/talaria/internal/encoding/key"
	"runtime"
)

// Uploader uploads to underlying backend
//go:generate mockery -name=S3Uploader -case underscore -testonly -inpkg
type Uploader interface {
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

// Writer represents a writer for Amazon S3 and compatible storages.
type Writer struct {
	uploader Uploader
	bucket   string
}

// New initializes a new S3 writer.
func New(region, bucket string, concurrency int) (*Writer, error) {
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}

	sess, err := session.NewSession(aws.NewConfig().WithRegion(region))
	if err != nil {
		return nil, err
	}

	return &Writer{
		uploader: s3manager.NewUploader(sess, func(u *s3manager.Uploader) { u.Concurrency = concurrency }),
		bucket:   bucket,
	}, nil
}

// Write writes creates object of S3 bucket prefix key in S3Writer bucket with value val
func (w *Writer) Write(key key.Key, val []byte) error {
	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Body:   bytes.NewBuffer(val),
		Key:    aws.String(string(key)),
	}

	_, err := w.uploader.Upload(uploadInput)
	return err
}
