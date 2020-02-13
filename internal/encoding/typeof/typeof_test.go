// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/crphang/orc"
	"github.com/stretchr/testify/assert"
)

func TestFromType(t *testing.T) {
	{
		typ, ok := FromType(reflectOfInt32)
		assert.Equal(t, Int32, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfInt64)
		assert.Equal(t, Int64, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfFloat64)
		assert.Equal(t, Float64, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfString)
		assert.Equal(t, String, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfBool)
		assert.Equal(t, Bool, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfTimestamp)
		assert.Equal(t, Timestamp, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflectOfJSON)
		assert.Equal(t, JSON, typ)
		assert.True(t, ok)
	}
	{
		typ, ok := FromType(reflect.TypeOf(complex128(1)))
		assert.Equal(t, Unsupported, typ)
		assert.False(t, ok)
	}

}

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

func TestSQL(t *testing.T) {
	assert.Equal(t, "INTEGER", Int32.SQL())
	assert.Equal(t, "BIGINT", Int64.SQL())
	assert.Equal(t, "DOUBLE", Float64.SQL())
	assert.Equal(t, "VARCHAR", String.SQL())
	assert.Equal(t, "BOOLEAN", Bool.SQL())
	assert.Equal(t, "TIMESTAMP", Timestamp.SQL())
	assert.Equal(t, "JSON", JSON.SQL())
	assert.Panics(t, func() {
		assert.Nil(t, Type(123).SQL())
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

func TestName(t *testing.T) {
	assert.Equal(t, "int32", Int32.String())
}

func TestMarshalJSON(t *testing.T) {
	types := []Type{Int32, Int64, Float64, Bool, String, Timestamp, JSON}
	for _, typ := range types {
		enc, err := json.Marshal(typ)
		assert.NoError(t, err)

		var out Type
		assert.NoError(t, json.Unmarshal(enc, &out))
		assert.Equal(t, typ, out)
	}
}
