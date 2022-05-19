// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package script

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestLoadLua(t *testing.T) {
	l := NewLuaLoader(nil, typeof.String)
	s, err := l.Load("https://raw.githubusercontent.com/kelindar/lua/master/fixtures/json.lua")
	assert.NotNil(t, s)
	assert.NoError(t, err)

	out, err := s.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":1,"b":"hello"}`, out)
}
