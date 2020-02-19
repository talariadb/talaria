// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestBalancer(t *testing.T) {

	// Start a few servers

	server1, port1 := serve()
	defer server1.Stop()
	server2, _ := serve()
	defer server2.Stop()
	server3, _ := serve()
	defer server3.Stop()

	addr1 := fmt.Sprintf("127.0.0.1:%v", port1)

	b := &pickerBuilder{Name: Name}
	p := b.Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			newConn(1): {Address: resolver.Address{Addr: addr1}},
			//	newConn(2): {Address: resolver.Address{Addr: "127.0.0.1:3002"}},
			//	newConn(3): {Address: resolver.Address{Addr: "127.0.0.1:3003"}},
		},
	})

	r, err := p.Pick(balancer.PickInfo{})
	assert.NotNil(t, r)
	assert.NoError(t, err)
	assert.Equal(t, newConn(1), r.SubConn)
}
