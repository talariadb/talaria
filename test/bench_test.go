// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/presto"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/server"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/kelindar/talaria/internal/storage/writer"
	"github.com/kelindar/talaria/internal/table/timeseries"
	talaria "github.com/kelindar/talaria/proto"
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

	c.Tables = make(map[string]config.Table, 1)
	c.Tables[tableName] = config.Table{
		TTL:    3600,
		HashBy: "_col5",
		SortBy: "_col1",
	}

	c.Storage.Directory = m.dir
	return nil
}

// BenchmarkQuery runs a benchmark for a main GetRows function for querying
// To run it, go in the directory and do 'go test -benchmem -bench=. -benchtime=1s'
// BenchmarkQuery/query-8         	     194	   6067001 ns/op	34924064 B/op	    1504 allocs/op
func BenchmarkQuery(b *testing.B) {
	dir, err := ioutil.TempDir(".", "testdata-")
	noerror(err)
	defer func() { _ = os.RemoveAll(dir) }()
	cfg := config.Load(context.Background(), 60*time.Second, &benchMockConfigurer{dir: dir})

	// create monitor
	monitor := monitor.NewNoop()
	store := disk.Open(cfg().Storage.Directory, tableName, monitor, cfg().Storage.Badger)
	streams, _ := writer.ForStreaming(config.Streams{}, monitor, script.NewLoader(nil))

	// Start the server and open the database
	eventlog := timeseries.New(tableName, new(noopMembership), monitor, store, &config.Table{
		TTL:    cfg().Tables[tableName].TTL,
		HashBy: cfg().Tables[tableName].HashBy,
		SortBy: cfg().Tables[tableName].SortBy,
		Schema: "",
	}, streams)

	server := server.New(cfg, monitor, script.NewLoader(nil), eventlog)
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
		TableName:  tableName,
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
			result, err := server.PrestoGetRows(split, []string{"_col5"}, 1*1024*1024, token)
			noerror(err)

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
