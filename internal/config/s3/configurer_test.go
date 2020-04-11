// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
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

	assert.Nil(t, c.Storage.Compact)
	assert.Nil(t, err)
}

type downloadMock func(ctx context.Context, uri string) ([]byte, error)

func (d downloadMock) Load(ctx context.Context, uri string) ([]byte, error) {
	return d(ctx, uri)
}
func TestUpdateAppName(t *testing.T) {
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://config.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	var down downloadMock = func(ctx context.Context, uri string) ([]byte, error) {
		return []byte("appName: talaria-processor"), nil
	}

	assert.Nil(t, err)
	err = NewWith(down, logging.NewNoop()).Configure(c)
	assert.Equal(t, c.AppName, "talaria-processor")
	assert.Nil(t, err)
}
