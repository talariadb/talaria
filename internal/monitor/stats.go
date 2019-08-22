// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"time"
)

// Stats defines the statsD client exported interface
type Stats interface {
	// Duration is time tracking metrics
	Duration(contextTag, key string, start time.Time, tags ...string)

	// Gauge measures a value over time
	Gauge(contextTag, key string, value float64, tags ...string)

	// Histogram tracks the statistical distribution of a set of values
	Histogram(contextTag, key string, value float64, tags ...string)

	// Count1 tracks the occurrence of something (this is equivalent to Count(key, 1, tags...)
	Count1(contextTag, key string, tags ...string)

	// Count increases or Decrease the value of something over time
	Count(contextTag, key string, amount int64, tags ...string)

	// MakeSafe makes a value safe for sending to StatsD
	MakeSafe(in string) string
}

func (c *clientImpl) enrichTags(tags []string) []string {
	return append(tags, c.tags...)
}

// Duration is time tracking metrics
func (c *clientImpl) Duration(prefix, key string, start time.Time, tags ...string) {
	c.stats.Duration(prefix, key, start, c.enrichTags(tags)...)
}

// Gauge measures a value over time
func (c *clientImpl) Gauge(contextTag, key string, value float64, tags ...string) {
	c.stats.Gauge(contextTag, key, value, c.enrichTags(tags)...)
}

// Histogram tracks the statistical distribution of a set of values
func (c *clientImpl) Histogram(contextTag, key string, value float64, tags ...string) {
	c.stats.Histogram(contextTag, key, value, c.enrichTags(tags)...)
}

// Count1 tracks the occurrence of something (this is equivalent to Count(key, 1, tags...)
func (c *clientImpl) Count1(contextTag, key string, tags ...string) {
	c.stats.Count1(contextTag, key, c.enrichTags(tags)...)
}

// Count increases or Decrease the value of something over time
func (c *clientImpl) Count(contextTag, key string, amount int64, tags ...string) {
	c.stats.Count(contextTag, key, amount, c.enrichTags(tags)...)
}

// MakeSafe makes a value safe for sending to StatsD
func (c *clientImpl) MakeSafe(in string) string {
	return c.stats.MakeSafe(in)
}
