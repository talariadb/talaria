// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/scripting"
	"github.com/stretchr/testify/assert"
)

func Test_Computed(t *testing.T) {
	c := newDataColumn(t)
	out, err := c.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":1,"b":"hello"}`, out)
}

func Test_Identifier(t *testing.T) {
	c, err := NewComputed("id", typeof.String, "make://identifier", nil)
	assert.NoError(t, err)
	out, err := c.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.Equal(t, "id", c.Name())
	assert.Equal(t, typeof.String, c.Type())
	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, 32, len(out.(string)))
}

func Test_Timestamp(t *testing.T) {
	c, err := NewComputed("ts", typeof.String, "make://timestamp", nil)
	assert.NoError(t, err)
	out, err := c.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.Equal(t, "ts", c.Name())
	assert.Equal(t, typeof.Timestamp, c.Type())
	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.NotZero(t, out.(int64))
}

func Test_Download(t *testing.T) {
	l := script.NewLoader(nil)
	c, err := NewComputed("data", typeof.JSON, "https://raw.githubusercontent.com/kelindar/lua/master/fixtures/json.lua", l)
	out, err := c.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})

	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":1,"b":"hello"}`, out)
}

func newDataColumn(t *testing.T) Computed {
	l := script.NewLoader(nil)
	c, err := NewComputed("data", typeof.JSON, `
	local json = require("json")

	function main(row) 
		return json.encode(row)
	end`, l)
	assert.NoError(t, err)
	return c
}
