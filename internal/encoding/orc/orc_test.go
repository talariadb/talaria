// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test2.orc"

const column = "string1"

func TestOrcRead(t *testing.T) {

	// struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,middle:struct<list:array<struct<int1:int,string1:string>>>,list:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>>
	i, err := FromFile(testFile)
	defer func() { _ = i.Close() }()
	assert.NoError(t, err)

	schema := i.Schema()
	assert.Equal(t, 8, len(schema))

	{
		kind, ok := schema[column]
		assert.True(t, ok)
		assert.Equal(t, "string", kind.String())
	}

	{
		kind, ok := schema["int1"]
		assert.True(t, ok)
		assert.Equal(t, "int32", kind.String())
	}

	count := 0
	i.Range(func(int, []interface{}) bool {
		count++
		return false
	}, column)

	assert.Equal(t, 2, count)
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
