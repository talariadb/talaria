// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../test/test1-zlib.orc"

func TestBlock(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc(o)
	assert.NoError(t, err)

	const column = "_col1"
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

// BenchmarkBlockRead/read-8         	 2000000	       744 ns/op	    1344 B/op	       5 allocs/op
func BenchmarkBlockRead(b *testing.B) {
	o, err := ioutil.ReadFile(testFile)
	noerror(err)

	blk, err := FromOrc(o)
	noerror(err)

	// Run the actual benchmark
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
