// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"testing"

	"github.com/scritchley/orc"
	"github.com/stretchr/testify/assert"
)

func TestReflect(t *testing.T) {
	assert.Equal(t, reflectOfInt32, Int32.Reflect())
	assert.Equal(t, reflectOfInt64, Int64.Reflect())
	assert.Equal(t, reflectOfFloat64, Float64.Reflect())
	assert.Equal(t, reflectOfString, String.Reflect())
	assert.Equal(t, reflectOfBool, Bool.Reflect())
	assert.Equal(t, reflectOfTimestamp, Timestamp.Reflect())
	assert.Equal(t, reflectOfJSON, JSON.Reflect())
	assert.Nil(t, Type(123).Reflect())
}

func TestCategory(t *testing.T) {
	assert.Equal(t, orc.CategoryInt, Int32.Category())
	assert.Equal(t, orc.CategoryLong, Int64.Category())
	assert.Equal(t, orc.CategoryDouble, Float64.Category())
	assert.Equal(t, orc.CategoryString, String.Category())
	assert.Equal(t, orc.CategoryBoolean, Bool.Category())
	assert.Equal(t, orc.CategoryTimestamp, Timestamp.Category())
	assert.Equal(t, orc.CategoryString, JSON.Category())
	assert.Panics(t, func() {
		assert.Nil(t, Type(123).Category())
	})
}

func TestFromOrc(t *testing.T) {
	td, err := orc.NewTypeDescription(
		orc.SetCategory(orc.CategoryBoolean),
	)
	assert.NoError(t, err)

	typ, ok := FromOrc(td)
	assert.True(t, ok)
	assert.Equal(t, Bool, typ)
}
