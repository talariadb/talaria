// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/env"
	"github.com/grab/talaria/internal/config/s3"
	"github.com/grab/talaria/internal/config/static"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/ingress/s3sqs"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/server/cluster"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/compact/s3compact"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/log"
	"github.com/grab/talaria/internal/table/nodes"
	"github.com/grab/talaria/internal/table/timeseries"
	talaria "github.com/grab/talaria/proto"
)

const (
	logTag = "main"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//cfg := config.Load("TALARIA_CONF")
	cfg := config.Load(
		ctx,
		60*time.Second,
		static.New(),
		env.New("TALARIA_CONF"),
		s3.New(),
	)
	cfgVal := cfg()

	// Setup gossip
	gossip := cluster.New(7946)

	// StatsD
	s, err := statsd.New(cfgVal.Statsd.Host + ":" + strconv.FormatInt(cfgVal.Statsd.Port, 10))
	if err != nil {
		panic(err)
	}

	// Create a log table and a simple stdout monitor
	stdout := monitor.New(logging.NewStandard(), s, "talaria", cfgVal.Env)
	logTbl := log.New(cfg, gossip, stdout)

	// Setup a monitor with a table output
	monitor := monitor.New(logTbl, s, "talaria", cfgVal.Env)
	monitor.Info("starting the log table ...")

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Info("starting the server and database ...")

	startMonitor(monitor)

	sortBy := func() string {
		return cfg().Tables.Timeseries.SortBy
	}

	hashBy := func() string {
		return cfg().Tables.Timeseries.HashBy
	}

	schema := func() *typeof.Schema {
		return cfg().Tables.Timeseries.Schema
	}
	timeseriesCfg := timeseries.Config{
		HashBy:       hashBy,
		SortBy:       sortBy,
		StaticSchema: schema,
		Name:         cfgVal.Tables.Timeseries.Name,
		TTL:          cfgVal.Tables.Timeseries.TTL,
	}

	// create a store
	var ingestor *s3sqs.Ingress
	var store storage.Storage
	store = disk.Open(cfgVal.Storage.Directory, cfgVal.Tables.Timeseries.Name, monitor)

	// if compact store is enabled then use the compact store
	if cfgVal.Storage.S3Compact != nil {
		store = s3compact.New(cfgVal.Storage.S3Compact, monitor, store)
	}

	server := server.New(cfg, monitor,
		timeseries.New(gossip, monitor, store, timeseriesCfg), // The primary timeseries table
		nodes.New(gossip), // Cluster membership info table
		logTbl,
	)

	if cfgVal.Writers.S3SQS != nil {
		ingestor, err := s3sqs.New(cfgVal.Writers.S3SQS, cfgVal.Writers.S3SQS.Region, monitor)
		if err != nil {
			panic(err)
		}
		// Start ingesting
		monitor.Info("starting ingestion ...")
		ingestor.Range(func(v []byte) bool {
			if _, err := server.Ingest(context.Background(), &talaria.IngestRequest{
				Data: &talaria.IngestRequest_Orc{Orc: v},
			}); err != nil {
				monitor.Warning(err)
			}
			return false
		})
	}

	// onSignal will be called when a OS-level signal is received.
	onSignal(func(_ os.Signal) {
		cancel()       // Cancel the context
		gossip.Close() // Close the gossip layer
		if ingestor != nil {
			ingestor.Close() // First finish and stop ingesting ...
		}
		server.Close() // Close the server and database ...
	})

	// Join the cluster
	gossip.JoinHostname(cfgVal.Domain)

	// Start listen
	monitor.Info("starting server listener ...")
	if err := server.Listen(ctx, cfgVal.Readers.Presto.Port, cfgVal.Writers.GRPC.Port); err != nil {
		panic(err)
	}
}

func startMonitor(m monitor.Monitor) {
	m.TrackDiskSpace()
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
