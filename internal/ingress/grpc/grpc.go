//go:generate protoc -I ../helloworld --go_out=plugins=grpc:../helloworld ../helloworld/helloworld.proto

package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/grab/async"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/monitor"
	talaria "github.com/grab/talaria/proto"
	"google.golang.org/grpc"
)

// Storage represents an underlying storage layer.
type Storage interface {
	io.Closer
	Append(key, value []byte, ttl time.Duration) error
	Range(seek, until []byte, f func(key, value []byte) bool) error
}

// Ingress represents a gRPC ingress layer.
type Ingress struct {
	server     *grpc.Server       // The underlying gRPC server
	port       int                // The port that serves the gRPC traffic
	store      Storage            // The underlying storage layer
	monitor    monitor.Client     // The monitor (logger & stats)
	cancel     context.CancelFunc // The cancellation function
	keyColumn  string             // The name of the key column
	timeColumn string             // The name of the time column
}

// New creates an ingress layer for gRPC.
func New(port int, cfg *config.Storage, store Storage, monitor monitor.Client) *Ingress {
	ingress := &Ingress{
		server:     grpc.NewServer(),
		monitor:    monitor,
		store:      store,
		keyColumn:  cfg.KeyColumn,
		timeColumn: cfg.TimeColumn,
	}

	talaria.RegisterIngressServer(ingress.server, ingress)
	return ingress
}

// Ingest implements ingress.IngressServer
func (s *Ingress) Ingest(ctx context.Context, request *talaria.IngestRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()
	const ttl = 10 * time.Minute // TODO: longer

	// Add the time
	now := time.Now()

	// Read blocks and repartition by the specified key
	blocks, err := block.FromRequestBy(request, s.keyColumn)
	if err != nil {
		return nil, err
	}

	for _, block := range blocks {
		b, err := block.Encode()
		if err != nil {
			return nil, err
		}

		if err := s.store.Append(key.New(string(block.Key), now), b, ttl); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// Close terminates the gRPC server and stops the ingress.
func (s *Ingress) Close() error {
	s.server.GracefulStop()
	s.cancel()
	return nil
}

// Range iterates through the queue, stops only if Close() is called or the f callback
// returns true.
func (s *Ingress) Range(f func(v []byte) bool) {

	// Create a cancellation context
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Asynchronously start the listener
	async.Invoke(ctx, func(ctx context.Context) (interface{}, error) {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
		defer lis.Close()
		if err != nil {
			return nil, err
		}

		if err := s.server.Serve(lis); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// TODO: range every x seconds so we can merge blocks together and write it to the primary log, and cleanup after

}

// handlePanic handles the panic and logs it out.
func (s *Ingress) handlePanic() {
	if r := recover(); r != nil {
		s.monitor.Errorf("panic recovered: %ss \n %s", r, debug.Stack())
	}
}
