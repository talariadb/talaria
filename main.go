// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/s3"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/kelindar/talaria/internal/scripting"
	mlog "github.com/kelindar/talaria/internal/scripting/log"
	mstats "github.com/kelindar/talaria/internal/scripting/stats"
	"github.com/kelindar/talaria/internal/server"
	"github.com/kelindar/talaria/internal/server/cluster"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/kelindar/talaria/internal/storage/writer"
	"github.com/kelindar/talaria/internal/table/log"
	"github.com/kelindar/talaria/internal/table/nodes"
	"github.com/kelindar/talaria/internal/table/timeseries"
)

const (
	logTag = "main"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Configurer := s3.New(logging.NewStandard())
	configure := config.Load(ctx, 60*time.Second, static.New(), env.New("TALARIA_CONF"), s3Configurer)
	conf := configure()

	// Setup gossip
	gossip := cluster.New(7946)

	// Create a log table and a simple stdout monitor
	stats := statsd.New(conf.Statsd.Host, int(conf.Statsd.Port))
	logTable := log.New(configure, gossip, monitor.New(
		logging.NewStandard(), stats, conf.AppName, conf.Env), // Use stdout monitor
	)

	// Setup the final logger and a monitor
	logger := logging.NewComposite(logTable, logging.NewStandard())
	monitor := monitor.New(logger, stats, conf.AppName, conf.Env)

	// Updating the logger to use the composite logger. This is to make sure the logs from the config is sent to log table as well as stdout
	s3Configurer.SetLogger(logger)

	// Create a script loader
	loader := script.NewLoader([]lua.Module{
		mlog.New(monitor),
		mstats.New(monitor),
	})

	// Create a storage, if compact store is enabled then use the compact store
	store := storage.Storage(disk.Open(conf.Storage.Directory, conf.Tables.Timeseries.Name, monitor))
	if conf.Storage.Compact != nil {
		store = writer.New(conf.Storage.Compact, monitor, store, loader)
	}

	// Start the new server
	server := server.New(configure, monitor, loader,
		timeseries.New(gossip, monitor, store, timeseries.Config{
			HashBy: conf.Tables.Timeseries.HashBy,
			SortBy: conf.Tables.Timeseries.SortBy,
			Name:   conf.Tables.Timeseries.Name,
			TTL:    conf.Tables.Timeseries.TTL,
			Schema: conf.Tables.Timeseries.Schema,
		}),
		nodes.New(gossip),
		logTable,
	)

	// onSignal will be called when a OS-level signal is received.
	onSignal(func(_ os.Signal) {
		cancel()       // Cancel the context
		gossip.Close() // Close the gossip layer
		server.Close() // Close the server and database
	})

	// Join the cluster
	gossip.JoinHostname(conf.Domain)

	// Start listen
	monitor.Info("starting talaria server")
	monitor.Count1(logTag, "start")
	if err := server.Listen(ctx, conf.Readers.Presto.Port, conf.Writers.GRPC.Port); err != nil {
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
