// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"testing"

	"github.com/grab/talaria/internal/encoding/typeof"
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

func Test_Download(t *testing.T) {
	c, err := NewComputed("data", typeof.JSON, "https://raw.githubusercontent.com/kelindar/lua/master/fixtures/json.lua")
	out, err := c.Value(map[string]interface{}{
		"a": 1,
		"b": "hello",
	})
	assert.NotNil(t, out)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":1,"b":"hello"}`, out)

}

func newDataColumn(t *testing.T) *Computed {
	c, err := NewComputed("data", typeof.JSON, `
	local json = require("json")

	function main(row) 
		return json.encode(row)
	end`)
	assert.NoError(t, err)
	return c
}
