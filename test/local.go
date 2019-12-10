// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/monitor/statsd"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/server/cluster"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/nodes"
	"github.com/grab/talaria/internal/table/timeseries"
)

func main() {
	dir, err := ioutil.TempDir(".", "testdata-")
	noerror(err)
	defer func() { _ = os.RemoveAll(dir) }()

	cfg := config.Config{
		Presto: &config.Presto{
			Port:   8042,
			Schema: "talaria",
			Table:  "eventlog",
		},
		GRPC: &config.GRPC{
			Port: 8043,
		},
		DataDir: dir,
		Storage: &config.Storage{
			TTLInSec:   3600,
			KeyColumn:  "string1",
			TimeColumn: "int1",
		},
	}

	// Create a logger
	monitor := monitor.New(logging.NewStandard(), statsd.NewNoop(), "talaria", "dev")

	// Open the file
	gossip := cluster.New(7946)
	store := disk.New(monitor)
	noerror(store.Open(cfg.DataDir))

	gossip.JoinHostname("localhost")

	// Start the server and open the database
	eventlog := timeseries.New(cfg.Presto.Table, cfg.Storage, store, gossip, monitor)
	server := server.New(cfg.Presto, monitor,
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
	// string1 can be (hi, bye)
	f2, _ := ioutil.ReadFile("./test3.orc")
	noerror(server.Append(f2))

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
	println("start listening on", cfg.Presto.Port)
	noerror(server.Listen(ctx, cfg.Presto.Port, cfg.GRPC.Port))
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
