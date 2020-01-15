// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/ingress/s3sqs"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/server/cluster"
	"github.com/grab/talaria/internal/table/log"
	"github.com/grab/talaria/internal/table/nodes"
	"github.com/grab/talaria/internal/table/timeseries"
	talaria "github.com/grab/talaria/proto"
)

const (
	logTag = "main"
)

func main() {
	cfg := config.Load("TALARIA_CONF")

	// Setup gossip
	gossip := cluster.New(7946)

	// StatsD
	s, err := statsd.New(cfg.Statsd.Host + ":" + strconv.FormatInt(cfg.Statsd.Port, 10))
	if err != nil {
		panic(err)
	}

	// Create a log table and a simple stdout monitor
	stdout := monitor.New(logging.NewStandard(), s, "talaria", cfg.Env)
	logTbl := log.New(cfg.Log, cfg.DataDir, gossip, stdout)

	// Setup a monitor with a table output
	monitor := monitor.New(logTbl, s, "talaria", cfg.Env)
	monitor.Info("starting the log table ...")

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Info("starting the server and database ...")

	startMonitor(monitor)

	// Start the server and open the database
	server := server.New(cfg, monitor,
		timeseries.New(cfg.Presto.Table, cfg.Storage, cfg.DataDir, gossip, monitor), // The primary timeseries table
		nodes.New(gossip), // Cluster membership info table
		logTbl,
	)

	// Create an S3 + SQS ingestion
	ingestor, err := s3sqs.New(cfg.Sqs, cfg.AwsRegion, monitor)
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

	// onSignal will be called when a OS-level signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	onSignal(func(_ os.Signal) {
		cancel()         // Cancel the context
		gossip.Close()   // Close the gossip layer
		ingestor.Close() // First finish and stop ingesting ...
		server.Close()   // Close the server and database ...
	})

	// Join the cluster
	gossip.JoinHostname(cfg.Domain)

	// Start listen
	monitor.Info("starting server listener ...")
	if err := server.Listen(ctx, cfg.Presto.Port, cfg.GRPC.Port); err != nil {
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
