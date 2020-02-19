// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestPinger(t *testing.T) {
	server, port := serve()

	var sample []time.Duration
	w := newWatcher(func(addr string, rtt time.Duration) error {
		sample = append(sample, rtt)
		return nil
	}, time.Millisecond)
	assert.NotNil(t, w)

	addr := fmt.Sprintf("127.0.0.1:%v", port)
	w.Watch(addr)
	w.Watch(addr)
	time.Sleep(500 * time.Millisecond)
	server.Stop()

	assert.NoError(t, w.Close())
	assert.NotEmpty(t, sample, 1)

	w.Unwatch(addr)
	_, found := w.conns.Load(addr)
	assert.False(t, found)
}

func serve() (*grpc.Server, int) {
	port := rand.Int31n(63000) + 2000
	server := grpc.NewServer()
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			panic(err)
		}

		defer lis.Close()
		healthpb.RegisterHealthServer(server, new(svc))
		if err := server.Serve(lis); err != nil {
			panic(err)
		}
		return
	}()
	return server, int(port)
}

type svc struct{}

func (*svc) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}
func (*svc) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return nil
}
