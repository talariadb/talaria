// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package env

import (
	"os"
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

// BenchmarkEnv-12    	  100000	     15758 ns/op	    2896 B/op	      98 allocs/op
func BenchmarkEnv(b *testing.B) {

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

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// populate the config with the env variable
		e := New("TALARIA_CONF")
		e.Configure(c)

		// asserts
		assert.Equal(b, c.URI, "ab.com")

		assert.Equal(b, c.Readers.Presto.Port, int32(123))

		assert.Equal(b, c.Writers.GRPC.Port, int32(100))
		assert.Equal(b, *c.Writers.S3SQS.VisibilityTimeout, int64(10))

		assert.Equal(b, c.Storage.Directory, "dir")

		assert.Equal(b, c.Tables.Timeseries.Name, "timeseries_eventlog")
		assert.Equal(b, c.Tables.Timeseries.TTL, int64(10))

		assert.Equal(b, c.Statsd.Host, "ab.com")
	}
}
