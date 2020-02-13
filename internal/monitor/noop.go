// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"context"
	"time"

	"github.com/grab/async"
)

type noopClient struct{}

// NewNoop ...
func NewNoop() Monitor {
	return &noopClient{}
}

// TrackDiskSpace ...
func (c *noopClient) TrackDiskSpace() async.Task {
	return async.NewTask(func(ctx context.Context) (interface{}, error) { return nil, nil })
}

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

func (c *noopClient) Error(error) {}

func (c *noopClient) Warning(error) {}

func (c *noopClient) Info(f string, v ...interface{}) {}

func (c *noopClient) Debug(f string, v ...interface{}) {}
