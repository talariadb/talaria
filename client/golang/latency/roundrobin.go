/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package latency

import (
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func buildRoundRobin(info base.PickerBuildInfo, rand *rng) balancer.V2Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPickerV2(balancer.ErrNoSubConnAvailable)
	}

	var scs []balancer.SubConn
	for sc := range info.ReadySCs {
		scs = append(scs, sc)
	}

	return &rrPicker{
		subConns: scs,
		// Start at a random index, as the same RR balancer rebuilds a new
		// picker when SubConn states change, and we don't want to apply excess
		// load to the first server in the list.
		next: rand.Intn(len(scs)),
	}
}

type rrPicker struct {
	// subConns is the snapshot of the roundrobin balancer when this picker was
	// created. The slice is immutable. Each Get() will do a round robin
	// selection from it and return the selected SubConn.
	subConns []balancer.SubConn

	mu   sync.Mutex
	next int
}

func (p *rrPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	sc := p.subConns[p.next]
	p.next = (p.next + 1) % len(p.subConns)
	p.mu.Unlock()
	return balancer.PickResult{SubConn: sc}, nil
}

// ------------------------------------------------------------------------------------------------------------

// RNG represents a thread-safe random number generator
type rng struct {
	sync.Mutex
	r rand.Rand
}

func newRandom() *rng {
	return &rng{
		r: *rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Int63n implements rand.Int63n on the grpcrand global source.
func (r *rng) Int63n(n int64) int64 {
	r.Lock()
	res := r.r.Int63n(n)
	r.Unlock()
	return res
}

// Intn implements rand.Intn on the grpcrand global source.
func (r *rng) Intn(n int) int {
	r.Lock()
	res := r.r.Intn(n)
	r.Unlock()
	return res
}

// Float64 implements rand.Float64 on the grpcrand global source.
func (r *rng) Float64() float64 {
	r.Lock()
	res := r.r.Float64()
	r.Unlock()
	return res
}
