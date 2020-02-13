// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"context"
	"time"

	"github.com/grab/async"
)

const (
	ctxTag    = "monitor"
	diskSpace = "diskspace"
)

// Tracker ...
type Tracker interface {
	TrackDiskSpace() async.Task
}

// TrackDiskSpace track disk available or total space
func (c *clientImpl) TrackDiskSpace() async.Task {
	return async.Repeat(context.Background(), 30*time.Second, func(ctx context.Context) (interface{}, error) {
		c.Debug(ctxTag, "monitor: disk space available: %d, total: %d", c.du.Available(), c.du.Size())
		c.Gauge(ctxTag, diskSpace, float64(c.du.Available()), "type:avail_space")
		c.Gauge(ctxTag, diskSpace, float64(c.du.Size()), "type:total_space")
		return nil, nil
	})
}
