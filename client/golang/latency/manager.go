// Copyright 2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/emitter-io/stats"
	"github.com/grab/async"
	"github.com/kelindar/kmeans"
	"github.com/kelindar/kmeans/distance"
	"google.golang.org/grpc/grpclog"
)

// Manager is used to keep all of the probing state and clusters the active connections. This is needed
// given that the picker is supposed to be stateless and immutable and we don't get notified whenever
// it is no longer needed.
type manager struct {
	rand    *rng           // The source of random number generation
	watch   *watcher       // The watcher associated which computes latencies
	monitor *stats.Monitor // The monitor which is used to compute histograms
	shape   []int          // The histogram shape
	weights sync.Map       // The weights for each address
	work    async.Task     // The task which continously updates the manager
}

// NewManager creates a new manager which pings and computes the stats.
func newManager(shape []int, k int) (m *manager) {
	m = &manager{
		rand:    newRandom(),
		monitor: stats.New(),
		shape:   shape,
	}

	m.watch = newWatcher(m.onPing, 500*time.Millisecond)
	m.work = async.Repeat(context.Background(), 5*time.Second, func(ctx context.Context) (interface{}, error) {
		m.update(k)
		return nil, nil
	})
	return
}

// WeightOf returns a weight of a particular address.
func (m *manager) WeightOf(addr string) float64 {
	if v, ok := m.weights.Load(addr); ok {
		return v.(float64)
	}
	return 0.0
}

// OnPing represents a callback for the watcher
func (m *manager) onPing(addr string, rtt time.Duration) error {
	v := int32(rtt.Nanoseconds() / 1000)
	if v == 0 { // Don't let it be zero, otherwise the update would fail
		v = 1
	}

	m.monitor.Measure(addr, v)
	return nil
}

func (m *manager) update(k int) {

	// Create a set of observations
	var points []kmeans.Observation
	m.monitor.Range(func(metric *stats.Metric) bool {
		points = append(points, makeObservation(metric.Name(), metric.Histogram(m.shape...)))
		return true
	})

	// Cluster the histograms
	result, err := kmeans.Cluster(points, k, distance.NormalizedIntersection, 20)
	if err != nil {
		grpclog.Errorf("latencyPicker: error on removing the probe %v", err.Error())
	}

	// Prepare the clusters
	clusters := make(clusters, len(result))
	for _, v := range result {
		clusters[v.Cluster].Index = v.Cluster
		clusters[v.Cluster].Addrs = append(clusters[v.Cluster].Addrs, v.Label)
	}

	// Compute average of the median per cluster, which we will then use to assign
	// the appropriate weight to the entire cluster.
	for k, c := range clusters {
		calc := stats.NewMetric(fmt.Sprintf("%v", k))
		for _, addr := range c.Addrs {
			if metric := m.monitor.Get(addr); metric != nil {
				calc.Update(int32(metric.Quantile(50)[0]))
			}
		}
		clusters[k].Dist = calc.Mean()
	}

	// Sort by distance
	sort.Sort(clusters)

	// Update the weights of all members
	sum := clusters.Sum()
	for _, c := range clusters {
		for _, addr := range c.Addrs {
			m.weights.Store(addr, (1 - (c.Dist / sum)))
		}
	}
}

// Watch adds an address to the watcher. It does nothing if the address is already
// being watched.
func (m *manager) Watch(addr string) {
	m.watch.Watch(addr)
}

// Unwatch stops watching a specific address and closes the underlying probe.
func (m *manager) Unwatch(addr string) {
	m.watch.Unwatch(addr)
}

// Close closes the manager and disposes of its resources.
func (m *manager) Close() error {
	m.work.Cancel()
	return m.watch.Close()
}

// ------------------------------------------------------------------------------------------------------------

// makeObservation makes an observation from a histogram
func makeObservation(addr string, hist []stats.Bin) kmeans.Observation {
	vector := make([]float64, 0, len(hist))
	for _, v := range hist {
		vector = append(vector, float64(v.Count))
	}

	return kmeans.Observation{
		Label: addr,
		Point: vector,
	}
}

// Cluster represents a clustered set of instances
type cluster struct {
	Index int      // The cluster number
	Addrs []string // The list of addresses in the cluster
	Dist  float64  // The distance (lower the closer) of the cluster
}

type clusters []cluster

func (c clusters) Len() int           { return len(c) }
func (c clusters) Less(i, j int) bool { return c[i].Dist < c[j].Dist }
func (c clusters) Swap(i, j int)      { c[i].Dist, c[j].Dist = c[j].Dist, c[i].Dist }
func (c clusters) Sum() float64 {
	sum := 0.0
	for _, v := range c {
		sum += v.Dist
	}
	return sum
}
