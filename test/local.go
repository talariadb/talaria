// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/monitor/statsd"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/server/cluster"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/nodes"
	"github.com/grab/talaria/internal/table/timeseries"
	talaria "github.com/grab/talaria/proto"
)

type mockConfigurer struct {
	dir string
}

func (m *mockConfigurer) Configure(c *config.Config) error {
	c.Storage.Directory = m.dir
	c.Readers.Presto = &config.Presto{
		Port:   8042,
		Schema: "talaria",
	}

	c.Writers.GRPC = &config.GRPC{
		Port: 8043,
	}

	c.Tables.Timeseries = &config.Timeseries{
		Name:   "eventlog",
		TTL:    3600,
		HashBy: "string1",
		SortBy: "int1",
	}
	return nil
}

func main() {

	dir, err := ioutil.TempDir(".", "testdata-")
	noerror(err)
	defer func() { _ = os.RemoveAll(dir) }()

	cfg := config.Load(context.Background(), 60*time.Second, &mockConfigurer{
		dir: dir,
	})

	// Create a logger
	monitor := monitor.New(logging.NewStandard(), statsd.NewNoop(), "talaria", "dev")

	// Init the gossip
	gossip := cluster.New(7946)
	gossip.JoinHostname("localhost")

	store := disk.Open(cfg().Storage.Directory, cfg().Tables.Timeseries.Name, monitor)

	// Start the server and open the database
	eventlog := timeseries.New(gossip, monitor, store, timeseries.Config{
		Name:   cfg().Tables.Timeseries.Name,
		TTL:    cfg().Tables.Timeseries.TTL,
		HashBy: cfg().Tables.Timeseries.HashBy,
		SortBy: cfg().Tables.Timeseries.SortBy,
		Schema: func() *typeof.Schema {
			return cfg().Tables.Timeseries.Schema
		},
	})
	server := server.New(cfg, monitor,
		eventlog,
		nodes.New(gossip),
	)
	defer server.Close()

	// onSignal will be called when a OS-level signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	onSignal(func(_ os.Signal) {
		cancel()
		gossip.Close()
		server.Close()
	})

	// Append some files
	orcfile, _ := ioutil.ReadFile("./test3.orc")
	_, err = server.Ingest(context.Background(), &talaria.IngestRequest{Data: &talaria.IngestRequest_Orc{Orc: orcfile}})
	noerror(err)

	// Print out the schema
	println("loaded data with the schema:")
	schema, _ := eventlog.Schema()
	for colName, colType := range schema {
		println(" - ", colName, colType.SQL())
	}

	println("try running the following query:")
	println("  select * from talaria.eventlog")
	println("  where string1 = '110010100101010010101000100001'")

	// Start listen
	println("start listening on", cfg().Readers.Presto.Port)
	noerror(server.Listen(ctx, cfg().Readers.Presto.Port, cfg().Writers.GRPC.Port))
}

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}

// onSignal hooks a callback for a signal.
func onSignal(callback func(sig os.Signal)) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range c {
			callback(sig)
		}
	}()
}
