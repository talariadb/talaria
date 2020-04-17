// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package env

import (
	"os"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {

	c := &config.Config{}
	st := static.New()
	st.Configure(c)

	// set the env variables

	// direct string keys
	os.Setenv("TALARIA_CONF_URI", "ab.com")

	// readers
	os.Setenv("TALARIA_CONF_READERS_PRESTO_PORT", "123")

	// writers
	os.Setenv("TALARIA_CONF_WRITERS_GRPC_PORT", "100")
	os.Setenv("TALARIA_CONF_WRITERS_S3SQS_VISIBILITYTIMEOUT", "10")

	// storage
	os.Setenv("TALARIA_CONF_STORAGE_DIR", "dir")

	// tables
	os.Setenv("TALARIA_CONF_TABLES_TIMESERIES_NAME", "timeseries_eventlog")
	os.Setenv("TALARIA_CONF_TABLES_TIMESERIES_TTL", "10")

	// statsd
	os.Setenv("TALARIA_CONF_STATSD_HOST", "ab.com")

	// populate the config with the env variable
	e := New("TALARIA_CONF")
	e.Configure(c)

	// asserts
	assert.Equal(t, c.URI, "ab.com")
	assert.Equal(t, c.Readers.Presto.Port, int32(123))
	assert.Equal(t, c.Writers.GRPC.Port, int32(100))
	assert.Equal(t, c.Writers.S3SQS.VisibilityTimeout, int64(10))
	assert.Equal(t, c.Storage.Directory, "dir")
	assert.Equal(t, c.Tables.Timeseries.Name, "timeseries_eventlog")
	assert.Equal(t, c.Tables.Timeseries.TTL, int64(10))
	assert.Equal(t, c.Statsd.Host, "ab.com")
}

func TestConfigure_Full(t *testing.T) {

	c := &config.Config{}
	st := static.New()
	st.Configure(c)

	// Write the full config
	os.Setenv("TALARIA_CONF", `mode: staging
env: staging
domain: "ab.com"
readers:
  presto:
    schema: grab_x
    port: 8042
writers:
  grpc:
    port: 8085
storage:
  dir: "data/"
  compact: 
    interval: 300
    file:
      dir: "output/"
tables:
  timeseries:
    name: eventlog
    ttl: 3600
    hashBy: event
    sortBy: time
    schema: |
      event: string
      time: int64
      data: json
  log:
    name: logs
  nodes:
    name: nodes
statsd:
  host: "127.0.0.1"
  port: 8126
computed:
  - name: "data"
    type: json
    func: |
      local json = require("json")
      function main(input)
        return json.encode(input)
      end
`)

	// populate the config with the env variable
	e := New("TALARIA_CONF")
	e.Configure(c)

	// asserts
	assert.Equal(t, c.Storage.Compact.File.Directory, "output/")
}
