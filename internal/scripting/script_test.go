// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package script

import (
	"context"
	"testing"

	"github.com/kelindar/lua"
	"github.com/stretchr/testify/assert"
)

func Test_Download(t *testing.T) {
	l := NewLoader(nil)
	s, err := l.Load("data", "https://raw.githubusercontent.com/kelindar/lua/master/fixtures/json.lua")
	assert.NotNil(t, s)
	assert.NoError(t, err)

	out, err := s.Run(context.Background(), map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, lua.String(`{"a":1,"b":"hello"}`), out)
}
