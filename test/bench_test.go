// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/timeseries"
	talaria "github.com/grab/talaria/proto"
)

const testFile2 = "./test1-zlib.orc"

type noopMembership int

func (m noopMembership) Members() []string {
	return []string{"127.0.0.1:9876"}
}

type benchMockConfigurer struct {
	dir string
}

func (m *benchMockConfigurer) Configure(c *config.Config) error {
	c.Readers.Presto = &config.Presto{
		Port: 9876,
	}

	c.Tables.Timeseries = &config.Timeseries{
		Name:   "eventlog",
		TTL:    3600,
		HashBy: "_col5",
		SortBy: "_col1",
	}

	c.Storage.Directory = m.dir
	return nil
}

// BenchmarkQuery runs a benchmark for a main GetRows function for querying
// To run it, go in the directory and do 'go test -benchmem -bench=. -benchtime=1s'
// BenchmarkQuery/query-8         	     163	   7225911 ns/op	39053102 B/op	    6171 allocs/op
func BenchmarkQuery(b *testing.B) {
	dir, err := ioutil.TempDir(".", "testdata-")
	noerror(err)
	defer func() { _ = os.RemoveAll(dir) }()
	cfg := config.Load(context.Background(), 60*time.Second, &benchMockConfigurer{dir: dir})

	// create monitor
	monitor := monitor.NewNoop()
	timeseriesCfg := timeseries.Config{
		Name:   "eventlog",
		TTL:    cfg().Tables.Timeseries.TTL,
		HashBy: cfg().Tables.Timeseries.HashBy,
		SortBy: cfg().Tables.Timeseries.SortBy,
		Schema: func() *typeof.Schema {
			return cfg().Tables.Timeseries.Schema
		},
	}
	store := disk.Open(cfg().Storage.Directory, timeseriesCfg.Name, monitor)

	// Start the server and open the database
	server := server.New(cfg, monitor,
		timeseries.New(new(noopMembership), monitor, store, timeseriesCfg),
	)
	defer server.Close()

	// Append some files
	orcfile, _ := ioutil.ReadFile(testFile2)
	for i := 0; i < 2; i++ {
		_, err := server.Ingest(context.Background(), &talaria.IngestRequest{Data: &talaria.IngestRequest_Orc{Orc: orcfile}})
		noerror(err)
	}

	// Cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start listen
	go func() {
		noerror(server.Listen(ctx, 9876, 9877))
	}()

	// Get a split ID for our query
	batch, err := server.PrestoGetSplits(&presto.PrestoThriftSchemaTableName{
		SchemaName: "talaria",
		TableName:  "eventlog",
	}, &presto.PrestoThriftNullableColumnSet{
		Columns: nil,
	}, newSplitQuery("Good"), 10000, nil)
	noerror(err)

	// Run the actual benchmark
	b.Run("query", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {

			// Create a split and a next token
			split := batch.Splits[0].SplitId
			token := new(presto.PrestoThriftNullableToken)

		repeat:
			result, _ := server.PrestoGetRows(split, []string{"_col5"}, 1*1024*1024, token)
			if result.NextToken != nil {
				token.Token = result.NextToken
				goto repeat
			}
		}
	})
}

func newSplitQuery(eventName string) *presto.PrestoThriftTupleDomain {
	return &presto.PrestoThriftTupleDomain{
		Domains: map[string]*presto.PrestoThriftDomain{
			"_col5": {
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
