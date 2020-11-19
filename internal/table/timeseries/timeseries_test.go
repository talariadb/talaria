// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	monitor2 "github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/presto"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/kelindar/talaria/internal/storage/writer"
	"github.com/kelindar/talaria/internal/table/timeseries"
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
	c.Tables["eventlog"] = config.Table{
		TTL:    3600,
		HashBy: "string1",
		SortBy: "int1",
	}

	c.Storage.Directory = m.dir
	return nil
}

func TestTimeseries_DynamicSchema(t *testing.T) {
	dir, _ := ioutil.TempDir(".", "testdata-")
	defer func() { _ = os.RemoveAll(dir) }()

	const name = "eventlog"
	tableConf := config.Table{
		HashBy: "string1",
		SortBy: "int1",
		TTL:    3600,
	}

	monitor := monitor2.NewNoop()
	store := disk.Open(dir, name, monitor, config.Badger{})
	streams, _ := writer.ForStreaming(config.Streams{}, monitor, nil)

	// Start the server and open the database
	eventlog := timeseries.New(name, new(noopMembership), monitor, store, &tableConf, streams)
	assert.NotNil(t, eventlog)
	assert.Equal(t, name, eventlog.Name())
	defer eventlog.Close()

	// Append some files
	{
		b, err := ioutil.ReadFile(testFile3)
		assert.NoError(t, err)
		apply := block.Transform(nil)
		blocks, err := block.FromOrcBy(b, tableConf.HashBy, nil, apply)
		assert.NoError(t, err)
		for _, block := range blocks {
			assert.NoError(t, eventlog.Append(block))
		}
	}

	// Get the schema
	{
		schema, isStatic := eventlog.Schema()
		assert.False(t, isStatic)
		assert.Len(t, schema, 2)
	}

	// Get the splits
	{
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", tableConf.HashBy), 10000)
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
		apply := block.Transform(nil)
		blocks, err := block.FromOrcBy(b, tableConf.HashBy, nil, apply)
		assert.NoError(t, err)
		for _, block := range blocks {
			assert.NoError(t, eventlog.Append(block))
		}
	}

	// Get the schema
	{
		schema, isStatic := eventlog.Schema()
		assert.False(t, isStatic)
		assert.Len(t, schema, 8)
	}

	// Get the splits
	{
		splits, err := eventlog.GetSplits([]string{}, newSplitQuery("110010100101010010101000100001", tableConf.HashBy), 10000)
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

func TestTimeSeries_LoadStaticSchema(t *testing.T) {
	dir, _ := ioutil.TempDir(".", "testdata-")
	defer func() { _ = os.RemoveAll(dir) }()

	staticSchema := `string1: string
int1: int64
`
	const name = "eventlog"
	tableConf := config.Table{
		HashBy: "string1",
		SortBy: "int1",
		Schema: staticSchema,
		TTL:    3600,
	}

	monitor := monitor2.NewNoop()
	store := disk.Open(dir, name, monitor, config.Badger{})
	streams, _ := writer.ForStreaming(config.Streams{}, monitor, nil)

	// Start the server and open the database
	eventlog := timeseries.New(name, new(noopMembership), monitor, store, &tableConf, streams)
	defer eventlog.Close()

	actualSchema, isStatic := eventlog.Schema()
	assert.True(t, isStatic)
	expectedSchema := typeof.Schema{
		"string1": typeof.String,
		"int1":    typeof.Int64,
	}
	assert.Equal(t, expectedSchema, actualSchema)
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
