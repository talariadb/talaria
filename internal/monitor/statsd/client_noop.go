// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package statsd

import (
	"time"
)

// Client defines the statsD client exported interface
type Client interface {
	Timing(name string, value time.Duration, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
	Histogram(name string, value float64, tags []string, rate float64) error
	Count(name string, value int64, tags []string, rate float64) error
}

// NewNoop returns a new No op statsD client.
// This is expected to useful in testing and to adding "standard" instrumentation to shared packages.
// As such this client is safe to be used in init() functions in libraries (skipping a tonne of nil checks)
func NewNoop() Client {
	return &clientNoop{}
}

// clientNoop implements the Client interface and provides the no-op implementation
type clientNoop struct {
	// intentionally left blank
}

// Duration implements Client interface
func (client *clientNoop) Timing(name string, value time.Duration, tags []string, rate float64) error {
	return nil
}

// Gauge implements Client interface
func (client *clientNoop) Gauge(name string, value float64, tags []string, rate float64) error {
	return nil
}

// Histogram implements Client interface
func (client *clientNoop) Histogram(name string, value float64, tags []string, rate float64) error {
	return nil
}

// Count1 implements Client interface
func (client *clientNoop) Count(name string, value int64, tags []string, rate float64) error {
	return nil
}
