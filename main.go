// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gitlab.myteksi.net/grab-x/talaria/internal/cluster"
	"gitlab.myteksi.net/grab-x/talaria/internal/config"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor/logging"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor/statsd"
	"gitlab.myteksi.net/grab-x/talaria/internal/server"
	"gitlab.myteksi.net/grab-x/talaria/internal/storage/disk"
	"gitlab.myteksi.net/grab-x/talaria/internal/storage/ingest"
	"gitlab.myteksi.net/grab-x/talaria/internal/storage/s3"
	"gitlab.myteksi.net/grab-x/talaria/internal/storage/sqs"
)

const (
	logTag = "main"
)

func main() {
	cfg := config.Load("X_TALARIA_CONF")

	// Setup gossip
	gossip := cluster.New(7946)
	logger := logging.NewStdOut()

	// StatsD
	s := statsd.NewNoop()

	monitor := monitor.New(logger, s, "x.talaria")

	monitor.Count1("system", "event", "type:start")
	defer monitor.Count1("system", "event", "type:stop")
	monitor.Info(logTag, "[main] starting the server and database ...")

	startMonitor(monitor)

	// Start the server and open the database
	store := disk.New(monitor)
	server := server.New(cfg.Port, gossip, store, cfg.Presto, cfg.Storage, monitor)
	err := store.Open(cfg.DataDir)
	if err != nil {
		panic(err)
	}

	monitor.Info(logTag, "[main] starting ingestion ...")
	// Start ingesting
	region := cfg.AwsRegion
	ingestor := ingest.New(newQueueReader(cfg.Sqs, region), s3.New(region, 5, logger), monitor)
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
	monitor.Info(logTag, "[main] starting DNS and route53 ...")
	r53 := cluster.NewRoute53(cfg.AwsRegion, monitor)

	// Keep maintaining our DNS
	go gossip.JoinAndSync(ctx, r53, cfg.Route.Domain, cfg.Route.ZoneID)

	// Start listen
	monitor.Info(logTag, "[main] starting server listener ...")
	if err := server.Listen(ctx); err != nil {
		panic(err)
	}
}

func startMonitor(m monitor.Client) {
	m.TrackDiskSpace()
}

// newQueueReader creates a new SQS reader
func newQueueReader(sqsCfg *config.SQSConfig, region string) *sqs.Reader {
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
