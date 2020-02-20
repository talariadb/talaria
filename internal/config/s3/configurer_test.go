// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://dev-ap-southeast-1-go-app-configs.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	assert.Nil(t, c.Tables.Timeseries.Schema)

	var down downloadMock
	down = func(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
		assert.Contains(t, []string{"a/b/c/conf-server-conf-stg.json", "a/b/c/abc_schema.yaml"}, prefix, "incorrect prefix")
		assert.Equal(t, "dev-ap-southeast-1-go-app-configs", bucket, "incorrect bucket")
		return []byte("a: int64"), nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	s3C := Configurer{
		client: cl,
	}

	err = s3C.Configure(c)

	assert.Equal(t, typeof.Schema{
		"a": typeof.Int64,
	}, *c.Tables.Timeseries.Schema)
	assert.Nil(t, c.Storage.S3Compact)
	assert.Nil(t, err)
}

func TestConfigure_NilSchema(t *testing.T) {
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://dev-ap-southeast-1-go-app-configs.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	assert.Nil(t, c.Tables.Timeseries.Schema)

	var down downloadMock
	down = func(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
		if prefix == "a/b/c/abc_schema.yaml" {
			return nil, errors.New("not found")
		}
		return nil, nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	s3C := Configurer{
		client: cl,
	}

	err = s3C.Configure(c)

	assert.Nil(t, c.Tables.Timeseries.Schema)
	assert.Nil(t, err)
}
