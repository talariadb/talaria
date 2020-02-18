// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestBalancer(t *testing.T) {

	// Start a few servers
	server1 := serve(3001)
	defer server1.Stop()
	server2 := serve(3002)
	defer server2.Stop()
	server3 := serve(3003)
	defer server3.Stop()

	b := &pickerBuilder{Name: Name}
	p := b.Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			newConn(1): {Address: resolver.Address{Addr: "127.0.0.1:3001"}},
			//	newConn(2): {Address: resolver.Address{Addr: "127.0.0.1:3002"}},
			//	newConn(3): {Address: resolver.Address{Addr: "127.0.0.1:3003"}},
		},
	})

	r, err := p.Pick(balancer.PickInfo{})
	assert.NotNil(t, r)
	assert.NoError(t, err)
	assert.Equal(t, newConn(1), r.SubConn)
}
