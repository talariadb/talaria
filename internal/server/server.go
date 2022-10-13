// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/column/computed"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/ingress/s3sqs"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/presto"
	"github.com/kelindar/talaria/internal/server/cluster"
	"github.com/kelindar/talaria/internal/server/thriftlog"
	"github.com/kelindar/talaria/internal/table"
	talaria "github.com/kelindar/talaria/proto"
	"google.golang.org/grpc"
)

const (
	ctxTag  = "server"
	errTag  = "error"
	funcTag = "func"
)

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
}

// Storage represents an eventlog storage contract.
type Storage interface {
	io.Closer
	Append(key, value []byte, ttl time.Duration) error
	Range(seek, until []byte, f func(key, value []byte) bool) error
}

// ------------------------------------------------------------------------------------------------------------

// New creates a new talaria server.

func New(conf config.Func, monitor monitor.Monitor, cluster cluster.Membership, tables ...table.Table) *Server {
	// Default grpc message size is 32 MB
	if conf().Writers.GRPC.MaxSendMsgSize == 0 {
		conf().Writers.GRPC.MaxSendMsgSize = 32 * 1024 * 1024 // 32 MB
	}

	if conf().Writers.GRPC.MaxRecvMsgSize == 0 {
		conf().Writers.GRPC.MaxRecvMsgSize = 32 * 1024 * 1024 // 32 MB
	}

	server := &Server{
		server:  grpc.NewServer(grpc.MaxRecvMsgSize(conf().Writers.GRPC.MaxRecvMsgSize), grpc.MaxSendMsgSize(conf().Writers.GRPC.MaxSendMsgSize)),
		conf:    conf,
		monitor: monitor,
		cluster: cluster,
		tables:  make(map[string]table.Table),
	}

	// Load computed columns
	for _, c := range conf().Computed {
		col, err := computed.NewComputed(c.Name, c.FuncName, c.Type, c.Func, monitor)
		if err != nil {
			monitor.Error(err)
			continue
		}

		monitor.Info("server: loaded computed column %v of type %v", c.Name, c.Type)
		server.computed = append(server.computed, col)
	}

	// Register the gRPC servers
	talaria.RegisterIngressServer(server.server, server)
	talaria.RegisterQueryServer(server.server, server)

	// Build a registry of tables
	for _, table := range tables {
		monitor.Info("server: registered %s table...", table.Name())
		server.tables[table.Name()] = table
	}
	return server
}

// Server represents the talaria server which should implement presto thrift interface.
type Server struct {
	server   *grpc.Server           // The underlying gRPC server
	conf     config.Func            // The presto configuration
	monitor  monitor.Monitor        // The monitoring layer
	cluster  cluster.Membership     // The Gossip Cluster.
	cancel   context.CancelFunc     // The cancellation function for the server
	tables   map[string]table.Table // The list of tables
	computed []computed.Computed    // The set of computed columns
	s3sqs    *s3sqs.Ingress         // The S3SQS Ingress (optional)
}

// Listen starts listening on presto RPC & gRPC.
func (s *Server) Listen(ctx context.Context, prestoPort, grpcPort int32) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Asynchronously start ingresting from S3/SQS (if configured)
	if err := s.pollFromSQS(s.conf()); err != nil {
		return err
	}

	// Asynchronously start the gRPC listener
	async.Invoke(ctx, func(ctx context.Context) (interface{}, error) {
		s.monitor.Info("server: listening for grpc on :%d...", grpcPort)
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
		defer lis.Close()
		if err != nil {
			return nil, err
		}

		if err := s.server.Serve(lis); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// Serve presto and block
	s.monitor.Info("server: listening for thrift on :%d...", grpcPort)
	return presto.Serve(ctx, int32(prestoPort), &thriftlog.Service{
		Service: s,
		Monitor: s.monitor,
	})
}

// Optionally starts an S3 SQS ingress
func (s *Server) pollFromSQS(conf *config.Config) (err error) {
	if conf.Writers.S3SQS == nil {
		return nil
	}

	// Create a new ingestor
	s.s3sqs, err = s3sqs.New(conf.Writers.S3SQS, conf.Writers.S3SQS.Region, s.monitor)
	if err != nil {
		return err
	}

	// Start ingesting
	s.monitor.Info("server: starting ingestion from S3/SQS...")
	s.s3sqs.Range(func(v []byte) bool {
		if _, err := s.Ingest(context.Background(), &talaria.IngestRequest{
			Data: &talaria.IngestRequest_Orc{Orc: v},
		}); err != nil {
			s.monitor.Warning(err)
		}
		return false
	})
	return nil
}

// Members returns the nodes joining the talaria cluster.
func (s *Server) Members() []string {
	return s.cluster.Members()
}

// Close closes the server and related resources.
func (s *Server) Close(monitor monitor.Monitor) {
	s.server.GracefulStop()
	monitor.Info("GRPC GracefulStop done, it will wait all request finished")
	s.cancel()

	// Stop S3/SQS ingress
	if s.s3sqs != nil {
		s.s3sqs.Close()
		monitor.Info("Close S3SQS done, it will wait all ingestion finished")
	}

	// Close all the open tables
	for _, t := range s.tables {
		if err := t.Close(); err != nil {
			s.monitor.Error(err)
		}
		monitor.Info("Close table %s done, it will compact all if compact is enable", t.Name())
	}
}

// ------------------------------------------------------------------------------------------------------------

// handlePanic handles the panic and logs it out.
func (s *Server) handlePanic() {
	if r := recover(); r != nil {
		s.monitor.Error(errors.Newf("panic recovered: %s \n %s", r, debug.Stack()))
	}
}
