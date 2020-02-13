// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/typeof"
	monitor2 "github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/timeseries"
	"github.com/stretchr/testify/assert"
)

const testFile2 = "../../../test/test2.orc"
const testFile3 = "../../../test/test3.orc"

type noopMembership int

func (m noopMembership) Members() []string {
	return []string{"127.0.0.1"}
}

type mockConfigurer struct {
	dir string
}

func (m *mockConfigurer) Configure(c *config.Config) error {

	c.Tables.Timeseries = &config.Timeseries{
		Name:   "eventlog",
		TTL:    3600,
		HashBy: "string1",
		SortBy: "int1",
	}

	c.Storage.Directory = m.dir
	return nil
}

func TestTimeseries(t *testing.T) {
	dir, _ := ioutil.TempDir(".", "testdata-")
	cfg := config.Load(context.Background(), 60*time.Second, &mockConfigurer{
		dir: dir,
	})
	defer func() { _ = os.RemoveAll(dir) }()

	schema := func() *typeof.Schema {
		return cfg().Tables.Timeseries.Schema
	}

	timeseriesCfg := timeseries.Config{
		HashBy: cfg().Tables.Timeseries.HashBy,
		SortBy: cfg().Tables.Timeseries.SortBy,
		Schema: schema,
		Name:   "eventlog",
		TTL:    cfg().Tables.Timeseries.TTL,
	}

	monitor := monitor2.NewNoop()
	store := disk.Open(cfg().Storage.Directory, timeseriesCfg.Name, monitor)

	// Start the server and open the database
	eventlog := timeseries.New(new(noopMembership), monitor, store, timeseriesCfg)
	assert.NotNil(t, eventlog)
	assert.Equal(t, "eventlog", eventlog.Name())
	defer eventlog.Close()

	// Append some files
	{
		b, err := ioutil.ReadFile(testFile3)
		assert.NoError(t, err)
		blocks, err := block.FromOrcBy(b, cfg().Tables.Timeseries.HashBy)
		assert.NoError(t, err)
		for _, block := range blocks {
			assert.NoError(t, eventlog.Append(block))
		}
	}

	// Get the schema
	{
		schema, err := eventlog.Schema()
		assert.NoError(t, err)
		assert.Len(t, schema, 2)
	}

	// Get the splits
	{
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", cfg().Tables.Timeseries.HashBy), 10000)
		assert.NoError(t, err)
		assert.Len(t, splits, 1)
		assert.Equal(t, "127.0.0.1", splits[0].Addrs[0])

		// Get the rows
		page, err := eventlog.GetRows(splits[0].Key, []string{"string1"}, 1*1024*1024)
		assert.NotNil(t, page)
		assert.NoError(t, err)
		assert.Len(t, page.Columns, 1)
		assert.Equal(t, 5, page.Columns[0].Count())
	}

	// Append a file with a different schema
	{
		b, err := ioutil.ReadFile(testFile2)
		assert.NoError(t, err)
		blocks, err := block.FromOrcBy(b, cfg().Tables.Timeseries.HashBy)
		assert.NoError(t, err)
		for _, block := range blocks {
			assert.NoError(t, eventlog.Append(block))
		}
	}

	// Get the schema
	{
		schema, err := eventlog.Schema()
		assert.NoError(t, err)
		assert.Len(t, schema, 8)
	}

	// Get the splits
	{
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", cfg().Tables.Timeseries.HashBy), 10000)
		assert.NoError(t, err)
		assert.Len(t, splits, 1)
		assert.Equal(t, "127.0.0.1", splits[0].Addrs[0])

		// Get the rows
		page, err := eventlog.GetRows(splits[0].Key, []string{"string1", "long1"}, 1*1024*1024)
		assert.NotNil(t, page)
		assert.NoError(t, err)
		assert.Len(t, page.Columns, 2)
		assert.Equal(t, 5, page.Columns[0].Count())
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
