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

func TestRoundRobin(t *testing.T) {
	p := buildRoundRobin(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			newConn(1): {Address: resolver.Address{
				Addr: "127.0.0.1",
			}},
		},
	}, newRandom())

	r, err := p.Pick(balancer.PickInfo{})
	assert.NotNil(t, r)
	assert.NoError(t, err)
	assert.Equal(t, newConn(1), r.SubConn)
}

func newConn(i int) balancer.SubConn {
	v := subconn(i)
	return &v
}

type subconn int

func (*subconn) UpdateAddresses([]resolver.Address) {}
func (*subconn) Connect()                           {}
