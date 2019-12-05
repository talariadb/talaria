// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/timeseries"
)

const testFile2 = "./test1-zlib.orc"

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}

type noopMembership int

func (m noopMembership) Members() []string {
	return []string{"127.0.0.1:9876"}
}

// BenchmarkQuery runs a benchmark for a main GetRows function for querying
// To run it, go in the directory and do 'go test -benchmem -bench=. -benchtime=1s'
// BenchmarkQuery/query-8         	      44	  26204484 ns/op	180541305 B/op	   47378 allocs/op
func BenchmarkQuery(b *testing.B) {
	dir, err := ioutil.TempDir(".", "")
	noerror(err)
	defer func() { _ = os.RemoveAll(dir) }()

	cfg := config.Config{
		Port:    9876,
		DataDir: dir,
		Storage: &config.Storage{
			TTLInSec:   3600,
			KeyColumn:  "_col5",
			TimeColumn: "_col0",
		},
	}

	// Open the file
	monitor := monitor.NewNoop()
	store := disk.New(monitor)
	noerror(store.Open(cfg.DataDir))

	// Start the server and open the database
	server := server.New(cfg.Port, cfg.Presto, monitor,
		timeseries.New("eventlog", cfg.Storage, store, new(noopMembership), monitor),
	)
	defer server.Close()

	// Append some files
	f2, _ := ioutil.ReadFile(testFile2)
	for i := 0; i < 2; i++ {
		noerror(server.Append(f2))
	}

	// Cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start listen
	go func() {
		noerror(server.Listen(ctx))
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
