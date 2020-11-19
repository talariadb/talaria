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

	// direct string keys
	os.Setenv("TALARIA_URI", "ab.com")

	// readers
	os.Setenv("TALARIA_READERS_PRESTO_PORT", "123")

	// writers
	os.Setenv("TALARIA_WRITERS_GRPC_PORT", "100")
	os.Setenv("TALARIA_WRITERS_S3SQS_VISIBILITYTIMEOUT", "10")

	// storage
	os.Setenv("TALARIA_STORAGE_DIR", "dir")

	// statsd
	os.Setenv("TALARIA_STATSD_HOST", "statsd")

	// populate the config with the env variable
	e := New("TALARIA")
	assert.NoError(t, e.Configure(c))

	// asserts
	assert.Equal(t, "ab.com", c.URI)
	assert.Equal(t, int32(123), c.Readers.Presto.Port)
	assert.Equal(t, int32(100), c.Writers.GRPC.Port)
	assert.Equal(t, int64(10), c.Writers.S3SQS.VisibilityTimeout)
	assert.Equal(t, "dir", c.Storage.Directory)
	assert.Equal(t, "statsd", c.Statsd.Host)
}

func TestConfigure_Full(t *testing.T) {
	c := &config.Config{}
	st := static.New()
	st.Configure(c)

	// Write the full config
	os.Setenv("TALARIA", `mode: staging
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
tables:
  eventlog:
    ttl: 3600
    hashBy: event
    sortBy: time
    schema: |
      event: string
      time: int64
      data: json
    compact:
      interval: 300
      file:
        dir: "output/"
    streams:
    - pubsub:
        project: my-gcp-project
        topic: my-topic
        filter: "gcs://my-bucket/my-function.lua"
        encoder: json
    - pubsub:
        project: my-gcp-project
        topic: my-topic2
        encoder: json
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
	e := New("TALARIA")
	assert.NoError(t, e.Configure(c))

	// asserts
	assert.Len(t, c.Tables["eventlog"].Streams, 2)
	assert.Len(t, c.Computed, 1)
	assert.Len(t, c.Tables, 1)

	assert.Equal(t, "my-gcp-project", c.Tables["eventlog"].Streams[0].PubSub.Project)
	assert.Equal(t, "output/", c.Tables["eventlog"].Compact.File.Directory)
}
