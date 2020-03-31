// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

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
	local log = require("log")

	function main() 
		log.debug('message')
		log.info('message')
		log.warning('message')
		log.error('message')
	end`, m)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	_, err = s.Run(context.Background())
	assert.NoError(t, err)
}
