// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
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
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/storage/s3compact"
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

	configure := config.Load(ctx, 60*time.Second, static.New(), env.New("TALARIA_CONF"), s3.New())
	conf := configure()

	// Setup gossip
	gossip := cluster.New(7946)

	// StatsD
	s, err := statsd.New(conf.Statsd.Host + ":" + strconv.FormatInt(conf.Statsd.Port, 10))
	if err != nil {
		panic(err)
	}

	// Create a log table and a simple stdout monitor
	stdLogger := logging.NewStandard()
	stdout := monitor.New(stdLogger, s, "talaria", conf.Env)
	logTbl := log.New(configure, gossip, stdout)
	compositeLogger := logging.NewComposite(logTbl, stdLogger)

	// Setup a monitor with a table output
	monitor := monitor.New(compositeLogger, s, "talaria", conf.Env)
	monitor.Info("starting the log table ...")

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Info("starting the server and database ...")

	startMonitor(monitor)

	// create a store
	var ingestor *s3sqs.Ingress
	store := storage.Storage(disk.Open(conf.Storage.Directory, conf.Tables.Timeseries.Name, monitor))

	// if compact store is enabled then use the compact store
	if conf.Storage.S3Compact != nil {
		store = s3compact.New(conf.Storage.S3Compact, monitor, store)
	}

	// Start the new server
	server := server.New(configure, monitor,
		timeseries.New(gossip, monitor, store, timeseries.Config{
			HashBy: conf.Tables.Timeseries.SortBy,
			SortBy: conf.Tables.Timeseries.HashBy,
			Name:   conf.Tables.Timeseries.Name,
			TTL:    conf.Tables.Timeseries.TTL,
			Schema: func() *typeof.Schema {
				return configure().Tables.Timeseries.Schema
			},
		}),
		nodes.New(gossip),
		logTbl,
	)

	if conf.Writers.S3SQS != nil {
		ingestor, err := s3sqs.New(conf.Writers.S3SQS, conf.Writers.S3SQS.Region, monitor)
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
	gossip.JoinHostname(conf.Domain)

	// Start listen
	monitor.Info("starting server listener ...")
	if err := server.Listen(ctx, conf.Readers.Presto.Port, conf.Writers.GRPC.Port); err != nil {
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
