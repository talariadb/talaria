// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

type noopMembership int

func (m noopMembership) Members() []string {
	return []string{"127.0.0.1"}
}

func (m noopMembership) Addr() string {
	return "127.0.0.1"
}

type mockConfigurer struct {
	dir string
}

func (m *mockConfigurer) Configure(c *config.Config) error {
	c.Storage.Directory = m.dir
	c.Tables.Log = &config.Log{
		TTL:  60,
		Name: "log",
	}
	return nil
}

func TestLog(t *testing.T) {
	dir, err := ioutil.TempDir(".", "testlog-")
	assert.NoError(t, err)

	defer func() { _ = os.RemoveAll(dir) }()

	cfg := config.Load(context.Background(), 60*time.Second, &mockConfigurer{
		dir: dir,
	})

	// Start the server and open the database
	monitor := monitor.NewNoop()
	logs := New(cfg, new(noopMembership), monitor)
	assert.NotNil(t, logs)
	assert.Equal(t, "log", logs.Name())
	defer logs.Close()

	// Append some files
	{

		assert.NoError(t, err)
		assert.NoError(t, logs.Append("hello world", logging.LevelInfo))
		assert.NoError(t, logs.Append("test message", logging.LevelWarning))
	}

	// Get the schema
	{
		schema, err := logs.Schema()
		assert.NoError(t, err)
		assert.Len(t, schema, 4)
	}

	// Get the splits
	{
		splits, err := logs.GetSplits([]string{}, newSplitQuery("log", "log"), 10000)
		assert.NoError(t, err)
		assert.Len(t, splits, 1)
		assert.Equal(t, "127.0.0.1", splits[0].Addrs[0])

		// Get the rows
		page, err := logs.GetRows(splits[0].Key, []string{"message", "time", "level"}, 1*1024*1024)
		assert.NotNil(t, page)
		assert.NoError(t, err)
		assert.Len(t, page.Columns, 3)

		assert.Equal(t, 2, page.Columns[0].Count())
		vd := page.Columns[0].AsThrift().VarcharData
		assert.Equal(t, "hello world", string(vd.Bytes[:vd.Sizes[0]]))
		assert.Equal(t, "test message", string(vd.Bytes[vd.Sizes[0]:vd.Sizes[0]+vd.Sizes[1]]))

		td := page.Columns[1].AsThrift().TimestampData
		assert.Len(t, td.Timestamps, 2)

		ld := page.Columns[2].AsThrift().VarcharData
		assert.Equal(t, "info", string(ld.Bytes[:ld.Sizes[0]]))
		assert.Equal(t, "warning", string(ld.Bytes[ld.Sizes[0]:ld.Sizes[0]+ld.Sizes[1]]))
	}
}

func newSplitQuery(eventName, colName string) *presto.PrestoThriftTupleDomain {
	return &presto.PrestoThriftTupleDomain{
		Domains: map[string]*presto.PrestoThriftDomain{
			colName: {
				ValueSet: &presto.PrestoThriftValueSet{
					RangeValueSet: &presto.PrestoThriftRangeValueSet{
						Ranges: []*presto.PrestoThriftRange{{
							Low: &presto.PrestoThriftMarker{
								Value: &presto.PrestoThriftBlock{
									VarcharData: &presto.PrestoThriftVarchar{
										Bytes: []byte(eventName),
										Sizes: []int32{int32(len(eventName))},
									},
								},
								Bound: presto.PrestoThriftBoundExactly,
							},
							High: &presto.PrestoThriftMarker{
								Value: &presto.PrestoThriftBlock{
									VarcharData: &presto.PrestoThriftVarchar{
										Bytes: []byte(eventName),
										Sizes: []int32{int32(len(eventName))},
									},
								},
								Bound: presto.PrestoThriftBoundExactly,
							},
						}},
					},
				},
			},
		},
	}
}
