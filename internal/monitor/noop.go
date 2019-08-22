// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"context"
	"time"

	"gitlab.myteksi.net/grab-x/talaria/internal/async"
)

type noopClient struct{}

// NewNoop ...
func NewNoop() Client {
	return &noopClient{}
}

// TrackDiskSpace ...
func (c *noopClient) TrackDiskSpace() async.Task {
	return async.NewTask(func(ctx context.Context) (interface{}, error) { return nil, nil })
}

// DefaultTags ...
func (c *noopClient) DefaultTags() []string { return []string{} }

// WithHost ...
func (c *noopClient) WithHost(hostname string) Client { return c }

// Duration ...
func (c *noopClient) Duration(contextTag, key string, start time.Time, tags ...string) {}

// Gauge measures a value over time
func (c *noopClient) Gauge(contextTag, key string, value float64, tags ...string) {}

// Histogram tracks the statistical distribution of a set of values
func (c *noopClient) Histogram(contextTag, key string, value float64, tags ...string) {}

// Count1 tracks the occurrence of something (this is equivalent to Count(key, 1, tags...)
func (c *noopClient) Count1(contextTag, key string, tags ...string) {}

// Count increases or Decrease the value of something over time
func (c *noopClient) Count(contextTag, key string, amount int64, tags ...string) {}

// MakeSafe makes a value safe for sending to StatsD
func (c *noopClient) MakeSafe(in string) string {
	return ""
}

// Debug ...
func (c *noopClient) Debug(tag, message string, args ...interface{}) {}

// Info ...
func (c *noopClient) Info(tag, message string, args ...interface{}) {}

// Warn ...
func (c *noopClient) Warn(tag, message string, args ...interface{}) {}

// Error ...
func (c *noopClient) Error(tag, message string, args ...interface{}) {}

// Fatal ...
func (c *noopClient) Fatal(tag, message string, args ...interface{}) {}

// Security ...
func (c *noopClient) Security(tag, message string, args ...interface{}) {}

// WarnWithStats ...
func (c *noopClient) WarnWithStats(tag, errType, message string, args ...interface{}) {}

// ErrorWithStats ...
func (c *noopClient) ErrorWithStats(tag, errType, message string, args ...interface{}) {}

// FatalWithStats ...
func (c *noopClient) FatalWithStats(tag, errType, message string, args ...interface{}) {}

// SecurityWithStats ...
func (c *noopClient) SecurityWithStats(tag, errType, message string, args ...interface{}) {}
