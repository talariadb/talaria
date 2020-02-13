// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	// initialize config
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://dev-ap-southeast-1-go-app-configs.s3-ap-southeast-1.amazonaws.com/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	var down downloadMock
	down = func(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
		return []byte("a: b"), nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	s3C := Configurer{
		client: cl,
	}

	err = s3C.Configure(c)
	assert.Nil(t, err)

}
