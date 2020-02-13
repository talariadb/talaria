// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoop(t *testing.T) {
	c := NewNoop()
	testTag := "tag"
	testKey := "key"
	testStart := time.Now()
	testMsg := "message"

	assert.NotPanics(t, func() {
		c.TrackDiskSpace()
		c.Duration(testTag, testKey, testStart)
		c.Gauge(testTag, testKey, 1)
		c.Count1(testTag, testKey)
		c.Count(testTag, testKey, 1)
		c.Debug(testTag, testMsg)
		c.Info(testTag, testMsg)
		c.Error(nil)
		c.Warning(nil)
	})
}
