// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"errors"
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	assert.Nil(t, c.Tables.Timeseries.Schema)

	var down downloadMock = func(ctx context.Context, uri string) ([]byte, error) {
		assert.Contains(t, []string{
			`s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json`,
			`s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/abc_schema.yaml`},
			uri, "incorrect uri")
		return []byte("a: int64"), nil
	}
	s3C := NewWith(down, logging.NewNoop())
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
	c.URI = "s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	assert.Nil(t, c.Tables.Timeseries.Schema)

	var down downloadMock = func(ctx context.Context, uri string) ([]byte, error) {
		if uri == "s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/abc_schema.yaml" {
			return nil, errors.New("not found")
		}
		return nil, nil
	}

	assert.Nil(t, err)
	err = NewWith(down, logging.NewNoop()).Configure(c)

	assert.Nil(t, c.Tables.Timeseries.Schema)
	assert.Nil(t, err)
}

type downloadMock func(ctx context.Context, uri string) ([]byte, error)

func (d downloadMock) Load(ctx context.Context, uri string) ([]byte, error) {
	return d(ctx, uri)
}
