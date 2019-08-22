// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package statsd

import (
	"time"
)

// Client defines the statsD client exported interface
type Client interface {
	// Time tracking metrics
	Duration(contextTag, key string, start time.Time, tags ...string)

	// Measure a value over time
	Gauge(contextTag, key string, value float64, tags ...string)

	// Track the statistical distribution of a set of values
	Histogram(contextTag, key string, value float64, tags ...string)

	// Track the occurrence of something (this is equivalent to Count(key, 1, tags...)
	Count1(contextTag, key string, tags ...string)

	// Increase or Decrease the value of something over time
	Count(contextTag, key string, amount int64, tags ...string)

	// Make a value safe for sending to StatsD
	MakeSafe(in string) string
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
func (client *clientNoop) Duration(contextTag, key string, start time.Time, tags ...string) {
	// intentionally left blank
}

// Gauge implements Client interface
func (client *clientNoop) Gauge(contextTag, key string, value float64, tags ...string) {
	// intentionally left blank
}

// Histogram implements Client interface
func (client *clientNoop) Histogram(contextTag, key string, value float64, tags ...string) {
	// intentionally left blank
}

// Count1 implements Client interface
func (client *clientNoop) Count1(contextTag, key string, tags ...string) {
	// intentionally left blank
}

// Count implements Client interface
func (client *clientNoop) Count(contextTag, key string, value int64, tags ...string) {
	// intentionally left blank
}

// MakeSafe implements Client interface
func (client *clientNoop) MakeSafe(in string) string {
	return in
}
