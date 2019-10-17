// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../test/test2.orc"

const column = "string1"

func TestOrcRead(t *testing.T) {

	i, err := FromFile(testFile)
	defer func() { _ = i.Close() }()
	assert.NoError(t, err)

	schema := i.Schema()

	{
		kind, ok := schema[column]
		assert.True(t, ok)
		assert.Equal(t, "string", kind.Name())
	}

	{
		kind, ok := schema["int1"]
		assert.True(t, ok)
		assert.Equal(t, "int32", kind.Name())
	}

	count := 0
	i.Range(func(int, []interface{}) bool {
		count++
		return false
	}, column)

	assert.Equal(t, 2, count)
}

func TestOrcSplit(t *testing.T) {

	i, err := FromFile(testFile)
	defer func() { _ = i.Close() }()
	assert.NoError(t, err)

	count := 0
	err2 := i.SplitBySize(1000, func(v []byte) bool {
		count++
		return false
	})

	assert.NoError(t, err2)
	assert.Equal(t, 1, count) // 2 / 1000 = 1
}

func TestSplit(t *testing.T) {

	b, err := ioutil.ReadFile(testFile)
	assert.NoError(t, err)

	count := 0
	schema, err2 := SplitBySize(b, 1000, func(v []byte) bool {
		count++
		return false
	})

	assert.NoError(t, err2)
	assert.NotNil(t, schema)
	assert.Equal(t, 1, count) // 2 / 1000 = 1
}

func TestSplitByColumn(t *testing.T) {

	b, err := ioutil.ReadFile(testFile)
	assert.NoError(t, err)

	count := 0
	schema, err2 := SplitByColumn(b, column, func(event string, v []byte) bool {
		count++
		return false
	})

	assert.NoError(t, err2)
	assert.NotNil(t, schema)
	assert.Equal(t, 2, count)
	//chronos.bookingETA
	//chronos.chaosFailure
}

func TestRange(t *testing.T) {

	b, err := ioutil.ReadFile(testFile)
	assert.NoError(t, err)

	count := 0
	err = Range(b, func(int, []interface{}) bool {
		count++
		return false
	}, column)

	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}
