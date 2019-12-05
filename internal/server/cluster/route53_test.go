// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package cluster

import (
	"testing"

	"github.com/grab/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

func TestRoute53Upsert(t *testing.T) {
	dns := NewRoute53("ap-southeast-1", monitor.NewNoop())
	targets := []string{
		"127.0.0.1",
		"127.0.0.2",
	}

	err := dns.Upsert("xxx", "ABC", targets, 1000)
	assert.True(t, err != nil)
}
