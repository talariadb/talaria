// Copyright 2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"context"
	"sync"
	"time"

	"github.com/grab/async"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// onPing represents a sink function for the watcher
type onPing func(addr string, rtt time.Duration) error

type watcher struct {
	work  async.Task
	conns sync.Map
	sink  onPing
}

// newWatcher starts to watch on the list of connections.
func newWatcher(callback onPing, interval time.Duration) *watcher {
	w := &watcher{
		sink: callback,
	}

	w.work = async.Repeat(context.Background(), interval, w.pingAll)
	return w
}

// Watch adds an address to the watcher. It does nothing if the address is already
// being watched.
func (w *watcher) Watch(addr string) {
	if _, ok := w.conns.Load(addr); ok {
		return
	}

	conn, err := dial(addr, w.sink)
	if err != nil {
		grpclog.Errorf("latencyPicker: unable to start a probe, due to %v", err.Error())
		return
	}

	if _, loaded := w.conns.LoadOrStore(addr, conn); loaded {
		grpclog.Infof("latencyPicker: potential race condition in the watcher")
		conn.Close()
	}
}

// Unwatch stops watching a specific address and closes the underlying probe.
func (w *watcher) Unwatch(addr string) {
	if v, ok := w.conns.Load(addr); ok {
		w.conns.Delete(addr)
		if err := v.(*probe).Close(); err != nil {
			grpclog.Errorf("latencyPicker: error on removing the probe %v", err.Error())
		}
	}
}

// pingAll pings every member of the watcher
func (w *watcher) pingAll(ctx context.Context) (interface{}, error) {
	w.rangeConns(func(addr string, conn *probe) {
		if err := conn.Ping(ctx); err != nil {
			grpclog.Errorf("latencyPicker: unable to ping health check, due to %v", err.Error())
		}
	})
	return nil, nil
}

// Close closes the connection and stops pinging
func (w *watcher) Close() error {
	if w.work.State() == async.IsRunning {
		w.work.Cancel()
	}

	// Close all the connections
	w.rangeConns(func(addr string, conn *probe) {
		if err := conn.Close(); err != nil {
			grpclog.Errorf("latencyPicker: error on closing the probe %v", err.Error())
		}
	})
	return nil
}

// rangeConns ranges over all the connections
func (w *watcher) rangeConns(f func(string, *probe)) {
	w.conns.Range(func(k, v interface{}) bool {
		conn, ok := v.(*probe)
		if !ok {
			return true
		}

		f(k.(string), conn)
		return true
	})
}

// ------------------------------------------------------------------------------------------------------------

// Probe represents a single watcher for a client.
type probe struct {
	addr string
	sink onPing
	conn *grpc.ClientConn
	svc  healthpb.HealthClient
}

// Dial connects a pinger to a target.
func dial(addr string, callback onPing) (*probe, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Dial over grpc
	conn, err := grpc.DialContext(ctx, addr, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &probe{
		conn: conn,
		svc:  healthpb.NewHealthClient(conn),
		addr: addr,
		sink: callback,
	}, nil
}

// Ping pings an address
func (w *probe) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Ping and calculate the RTT
	start := time.Now()
	resp, err := w.svc.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
		return err
	}

	// Track the round trip time
	return w.sink(w.addr, time.Now().Sub(start))
}

// Close closes the connection and stops pinging
func (w *probe) Close() error {
	return w.conn.Close()
}
