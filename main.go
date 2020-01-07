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
	monitorpkg "github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/monitor/logging/table"
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
	logger := logging.NewStandard()

	// StatsD
	s, err := statsd.New(cfg.Statsd.Host + ":" + strconv.FormatInt(cfg.Statsd.Port, 10))
	if err != nil {
		panic(err)
	}

	monitorWithStdout := monitorpkg.New(logger, s, "talaria", cfg.Env)

	// Open the storage for the log table
	logTable := log.New(cfg.Log, cfg.DataDir, gossip, monitorWithStdout) // The log table
	monitor := monitorpkg.New(table.NewTable(logTable), s, "talaria", cfg.Env)
	monitor.Infof("starting the log table ...")

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Infof("starting the server and database ...")

	startMonitor(monitor)

	// Start the server and open the database
	server := server.New(cfg, monitor,
		timeseries.New(cfg.Presto.Table, cfg.Storage, cfg.DataDir, gossip, monitor), // The primary timeseries table
		nodes.New(gossip), // Cluster membership info table
		logTable,
	)

	// Create an S3 + SQS ingestion
	ingestor, err := s3sqs.New(cfg.Sqs, cfg.AwsRegion, monitor)
	if err != nil {
		panic(err)
	}

	// Start ingesting
	monitor.Infof("starting ingestion ...")
	ingestor.Range(func(v []byte) bool {
		if _, err := server.Ingest(context.Background(), &talaria.IngestRequest{
			Data: &talaria.IngestRequest_Orc{Orc: v},
		}); err != nil {
			monitor.WarnWithStats(logTag, "ingestor_append", "unable to append data from queue due to %s", err.Error())
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
	monitor.Infof("starting server listener ...")
	if err := server.Listen(ctx, cfg.Presto.Port, cfg.GRPC.Port); err != nil {
		panic(err)
	}
}

func startMonitor(m monitorpkg.Client) {
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
