// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test1-zlib.orc"
const smallFile = "../../../test/test2.orc"

func TestBlock(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc(o)
	assert.NoError(t, err)

	const column = "_col5"
	result, err := b.Select([]string{column})
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))

	encoded, err := b.Encode()
	assert.NotEmpty(t, encoded)
	assert.NoError(t, err)

	back, err := FromBuffer(encoded)
	assert.NotNil(t, back)
	assert.NoError(t, err)

	result, err = back.Select([]string{column})
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))
}

func TestBlock_Types(t *testing.T) {
	o, err := ioutil.ReadFile(smallFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc(o)
	assert.NoError(t, err)

	{
		result, err := b.Select([]string{"int1"})
		assert.NoError(t, err)
		assert.Equal(t, 2, result["int1"].IntegerData.Count())
	}

	{
		result, err := b.Select([]string{"string1"})
		assert.NoError(t, err)
		assert.Equal(t, 2, result["string1"].VarcharData.Count())
	}

	{
		result, err := b.Select([]string{"long1"})
		assert.NoError(t, err)
		assert.Equal(t, 2, result["long1"].BigintData.Count())
	}
}

// BenchmarkBlockRead/read-8         	     180	   6222619 ns/op	23055123 B/op	      11 allocs/op
func BenchmarkBlockRead(b *testing.B) {
	o, err := ioutil.ReadFile(testFile)
	noerror(err)

	blk, err := FromOrc(o)
	noerror(err)

	// 122MB uncompressed
	// 13MB snappy compressed
	buf, err := blk.Encode()
	noerror(err)

	columns := []string{"_col5"}
	b.Run("read", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, _ = Read(buf, columns)
		}
	})
}

// BenchmarkFromOrc/orc-8         	    8020	    124737 ns/op	  445735 B/op	    1098 allocs/op
func BenchmarkFromOrc(b *testing.B) {
	o, err := ioutil.ReadFile(smallFile)
	noerror(err)

	b.Run("orc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			FromOrc(o)
			noerror(err)
		}
	})

}

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}
