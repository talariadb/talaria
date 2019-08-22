// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	c := newCache()
	c.Set([]byte("hi"), []byte("corey"))

	{
		ok := c.Contains([]byte("hi"))
		assert.True(t, ok)
	}

	{
		v, ok := c.Get([]byte("hi"))
		assert.True(t, ok)
		assert.Equal(t, []byte("corey"), v)
	}

	{
		v, ok := c.Get([]byte("not there"))
		assert.False(t, ok)
		assert.Nil(t, v)
	}

}
