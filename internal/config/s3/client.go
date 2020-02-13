// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"time"

	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/kelindar/loader/s3"
)

type downloader interface {
	DownloadIf(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error)
}

type client struct {
	downloader downloader
}

// newClient a new S3 Client.
func newClient(dl downloader) (*client, error) {
	if dl != nil {
		return &client{
			downloader: dl,
		}, nil
	}

	c, err := s3.New("", 5)

	if err != nil {
		return nil, errors.Internal("unable to create loader client for s3", err)
	}

	return &client{
		downloader: c,
	}, nil
}

// Download a specific key from the bucket
func (s *client) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	downloadedAt := time.Now()
	downloadedAt.Add(-1 * time.Second * 90)
	data, err := s.downloader.DownloadIf(ctx, bucket, key, downloadedAt)
	if err != nil {
		return nil, err
	}

	return data, nil
}
