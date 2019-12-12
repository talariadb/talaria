// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/monitor"
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

func TestTimeseries(t *testing.T) {
	dir, err := ioutil.TempDir(".", "testdata-")
	assert.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	cfg := config.Config{
		DataDir: dir,
		Storage: &config.Storage{
			TTLInSec:   3600,
			KeyColumn:  "string1",
			TimeColumn: "int1",
		},
	}

	// Start the server and open the database
	monitor := monitor.NewNoop()
	store := disk.New(monitor)
	assert.NoError(t, store.Open(cfg.DataDir))

	eventlog := timeseries.New("eventlog", cfg.Storage, store, new(noopMembership), monitor)
	assert.NotNil(t, eventlog)
	assert.Equal(t, "eventlog", eventlog.Name())
	defer eventlog.Close()

	// Append some files
	{
		b, err := ioutil.ReadFile(testFile3)
		assert.NoError(t, err)
		blocks, err := block.FromOrcBy(b, cfg.Storage.KeyColumn)
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
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", cfg.Storage.KeyColumn), 10000)
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
		blocks, err := block.FromOrcBy(b, cfg.Storage.KeyColumn)
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
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", cfg.Storage.KeyColumn), 10000)
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
