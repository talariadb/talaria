// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type downloadMock func(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error)

func (d downloadMock) DownloadIf(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
	return d(ctx, bucket, prefix, updatedSince)
}

func TestClient(t *testing.T) {
	var down downloadMock
	down = func(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
		return []byte("abc"), nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	assert.NotNil(t, cl)

	v, err := cl.Download(context.Background(), "an", "cd")
	assert.Nil(t, err)
	assert.Equal(t, string(v), "abc")
}
