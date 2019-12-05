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
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/server"
	"github.com/grab/talaria/internal/server/cluster"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/storage/ingest"
	"github.com/grab/talaria/internal/storage/s3"
	"github.com/grab/talaria/internal/storage/sqs"
	"github.com/grab/talaria/internal/table/nodes"
	"github.com/grab/talaria/internal/table/timeseries"
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

	monitor := monitor.New(logger, s, "talaria", cfg.Env)

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Infof("starting the server and database ...")

	startMonitor(monitor)

	// Open the storage for the main data
	store := disk.New(monitor)
	err = store.Open(cfg.DataDir)
	if err != nil {
		panic(err)
	}

	// Start the server and open the database
	server := server.New(cfg.Port, cfg.Presto, monitor,
		timeseries.New(cfg.Presto.Table, cfg.Storage, store, gossip, monitor), // The primary timeseries table
		nodes.New(gossip), // Cluster membership info table
	)

	// Start ingesting
	monitor.Infof("starting ingestion ...")
	ingestor := ingest.New(newQueueReader(cfg.Sqs, cfg.AwsRegion), s3.New(cfg.AwsRegion, 5, logger), monitor)
	ingestor.Range(func(v []byte) bool {
		if err := server.Append(v); err != nil {
			monitor.WarnWithStats(logTag, "ingestor_append", "[main][error:%s][v:%s] server failed to append data from queue", err, v)
		}

		return false
	})

	// onSignal will be called when a OS-level signal is received.
	onSignal(func(_ os.Signal) {
		ingestor.Close() // First finish and stop ingesting ...
		server.Close()   // Close the server and database ...
		os.Exit(0)       // Exit the process.
	})

	// Cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Join the cluster
	monitor.Infof("starting DNS and route53 ...")
	r53 := cluster.NewRoute53(cfg.AwsRegion, monitor)

	// Keep maintaining our DNS
	go gossip.JoinAndSync(ctx, r53, cfg.Route.Domain, cfg.Route.ZoneID)

	// Start listen
	monitor.Infof("starting server listener ...")
	if err := server.Listen(ctx); err != nil {
		panic(err)
	}
}

func startMonitor(m monitor.Client) {
	m.TrackDiskSpace()
}

// newQueueReader creates a new SQS reader
func newQueueReader(sqsCfg *config.SQS, region string) *sqs.Reader {
	reader, err := sqs.NewReader(&sqs.ReaderConfig{
		QueueURL:          sqsCfg.Endpoint,
		Region:            region,
		WaitTimeout:       sqsCfg.WaitTimeout,
		VisibilityTimeout: sqsCfg.VisibilityTimeout,
	})
	if err != nil {
		panic(err)
	}
	return reader
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
