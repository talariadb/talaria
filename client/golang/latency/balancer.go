// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"math"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

// Name is the name of the latency balancers.
const (
	Name = "latency"
)

// State represents the internal state, unfortunately had to be done as a singleton.
var state = newManager([]int{
	0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
	1250, 1500, 1750, 2000, 3000, 4000, 5000, 100000,
}, 3)

func init() {
	balancer.Register(newBuilder(Name))
}

// ------------------------------------------------------------------------------------------------------------

func newBuilder(name string) balancer.Builder {
	return base.NewBalancerBuilderV2(name, &pickerBuilder{Name: name}, base.Config{HealthCheck: true})
}

type pickerBuilder struct {
	Name string
}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.V2Picker {
	grpclog.Infof("latencyPicker: newPicker called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPickerV2(balancer.ErrNoSubConnAvailable)
	}

	// Create a new picker
	rand := newRandom()
	p := &picker{
		rand:  rand,
		fair:  buildRoundRobin(info, rand),
		conns: make(connections, 0, len(info.ReadySCs)),
	}

	// Push all connections to the table and make sure we're watching them
	for sc, tag := range info.ReadySCs {
		state.Watch(tag.Address.Addr)
		p.conns = append(p.conns, conn{
			Addr: tag.Address.Addr,
			Conn: sc,
		})
	}

	return p
}

// ------------------------------------------------------------------------------------------------------------

// Picker represents a picker implementation which picks the address based on
// the weight of success instead of round-robin.
type picker struct {
	last  int64             // The last time the update happened
	rand  *rng              // The source of random number generation
	fair  balancer.V2Picker // The fallback roundrobin load balancer
	conns connections       // The set of connections to load balance to
}

// Pick picks the connection for the request.
func (p *picker) Pick(b balancer.PickInfo) (balancer.PickResult, error) {
	p.tryUpdate(60 * time.Second)

	// Pick the the connection randomly
	r, sum := p.rand.Float64(), float64(0)
	for _, conn := range p.conns {
		if sum += conn.Weight(); r <= sum {
			return balancer.PickResult{SubConn: conn.Conn}, nil
		}
	}

	// Fallback to round-robin
	return p.fair.Pick(b)
}

// TryUpdate potentially triggers an update of the weights
func (p *picker) tryUpdate(interval time.Duration) {
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&p.last)
	if last+interval.Nanoseconds() > now && atomic.CompareAndSwapInt64(&p.last, last, now) {
		p.conns.Update()
	}
}

// ------------------------------------------------------------------------------------------------------------

type connections []conn

// Update updates the weights for all of the connections
func (c connections) Update() {

	// Get the weights for every connection
	weights, sum := make([]float64, len(c)), 0.0
	for i, conn := range c {
		weights[i] = state.WeightOf(conn.Addr)
		sum += weights[i]
	}

	// Normalize
	for i, conn := range c {
		conn.Update(weights[i] / sum)
	}
}

// Conn represents a connection for the load balancer
type conn struct {
	weight uint64           // The weight of the connection
	Addr   string           // The address associated with the connection
	Conn   balancer.SubConn // The sub connection
}

// Update updates the weight of the connection
func (c *conn) Update(v float64) {
	atomic.StoreUint64(&c.weight, math.Float64bits(v))
}

// Weight returns the weight of the connection
func (c *conn) Weight() float64 {
	return math.Float64frombits(atomic.LoadUint64(&c.weight))
}
