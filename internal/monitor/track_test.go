// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"os"
	"testing"

	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/monitor/statsd"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/stretchr/testify/assert"
)

func TestTrackDiskSpace(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	client := New(logging.NewStandard(), statsd.NewNoop(), "test", "stg")
	m := client.(*clientImpl)
	m.du = du.NewDiskUsage(wd)
	m.tags = []string{"host:1.1.1.2", "pid:30"}
	task := m.TrackDiskSpace()
	assert.NotNil(t, task)
}
