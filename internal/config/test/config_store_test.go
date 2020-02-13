// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/env"
	"github.com/grab/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	// Usage of the config store.

	os.Setenv("TALARIA_CONF_STORAGE_DIR", "dir-1")
	cfg := config.Load(context.Background(), 1*time.Second, static.New(), env.New("TALARIA_CONF"))
	assert.Equal(t, cfg().Storage.Directory, "dir-1")

	os.Setenv("TALARIA_CONF_STORAGE_DIR", "dir-2")
	time.Sleep(2 * time.Second)

	assert.Equal(t, cfg().Storage.Directory, "dir-2")

	os.Setenv("TALARIA_CONF_TIMESERIES_NAME", "eventlog")
	time.Sleep(2 * time.Second)
	assert.Equal(t, cfg().Tables.Timeseries.Name, "eventlog")
}
