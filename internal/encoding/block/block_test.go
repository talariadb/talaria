// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test1-zlib.orc"

func TestBlock(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc(o)
	assert.NoError(t, err)

	const column = "_col5"
	result, err := b.Select(column)
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))

	encoded, err := b.Encode()
	assert.NotEmpty(t, encoded)
	assert.NoError(t, err)

	back, err := FromBuffer(encoded)
	assert.NotNil(t, back)
	assert.NoError(t, err)

	result, err = back.Select(column)
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))
}

func TestBlock_Types(t *testing.T) {
	o, err := ioutil.ReadFile("../../../test/test2.orc")
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc(o)
	assert.NoError(t, err)

	{
		result, err := b.Select("int1")
		assert.NoError(t, err)
		assert.Equal(t, 2, result["int1"].IntegerData.Count())
	}

	{
		result, err := b.Select("string1")
		assert.NoError(t, err)
		assert.Equal(t, 2, result["string1"].VarcharData.Count())
	}

	{
		result, err := b.Select("long1")
		assert.NoError(t, err)
		assert.Equal(t, 2, result["long1"].BigintData.Count())
	}
}

// BenchmarkBlockRead/from-buffer-8         	     276	   3989427 ns/op	120121419 B/op	      13 allocs/op
// BenchmarkBlockRead/read-8                	 1400730	       743 ns/op	    1344 B/op	       5 allocs/op
func BenchmarkBlockRead(b *testing.B) {
	o, err := ioutil.ReadFile(testFile)
	noerror(err)

	blk, err := FromOrc(o)
	noerror(err)

	b.Run("from-buffer", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, _ = FromBuffer(o)
		}
	})

	b.Run("read", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, _ = blk.Select("_col1")
		}
	})
}

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}
