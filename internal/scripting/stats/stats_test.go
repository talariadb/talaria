// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package stats

import (
	"context"
	"github.com/kelindar/lua"
	"testing"

	"github.com/grab/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

func Test_Module(t *testing.T) {
	m := New(monitor.NewNoop())
	assert.NotNil(t, m)

	// Create a new script
	s, err := lua.FromString("test", `
	local stats = require("stats")

	function main() 
		stats.count('key', 10, {'tag1:value1', 'tag2:value2'})
		stats.gauge('key', 10, {'tag1:value1', 'tag2:value2'})
		stats.histogram('key', 10, {'tag1:value1', 'tag2:value2'})
	end`, m)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	_, err = s.Run(context.Background())
	assert.NoError(t, err)
}
