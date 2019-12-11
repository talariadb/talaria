// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlock_Types(t *testing.T) {
	o, err := ioutil.ReadFile(smallFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrcBy(o, "string1")
	assert.Equal(t, 2, len(b))
	assert.NoError(t, err)

	schema := b[0].Schema()
	assert.Equal(t, 8, len(schema))
	assert.Contains(t, schema, "boolean1")
	assert.Contains(t, schema, "double1")
	assert.Contains(t, schema, "int1")
	assert.Contains(t, schema, "long1")
	assert.Contains(t, schema, "string1")
	assert.Contains(t, schema, "list")

	{
		result, err := b[0].Select([]string{"int1"})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["int1"].IntegerData.Count())
	}

	{
		result, err := b[0].Select([]string{"string1"})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["string1"].VarcharData.Count())
	}

	{
		result, err := b[0].Select([]string{"long1"})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["long1"].BigintData.Count())
	}

	{
		min, ok := b[0].Min("int1")
		assert.True(t, ok)
		assert.Equal(t, int64(65536), min)
	}

	{
		min, ok := b[0].Min("long1")
		assert.False(t, ok) // Actually might be wrong...
		assert.Equal(t, int64(9223372036854775807), min)
	}
}

// BenchmarkBlockRead/read-8         	   50000	     28001 ns/op	   43285 B/op	      10 allocs/op
func BenchmarkBlockRead(b *testing.B) {
	o, err := ioutil.ReadFile(testFile)
	noerror(err)

	blk, err := FromOrcBy(o, "_col5")
	noerror(err)

	// 122MB uncompressed
	// 13MB snappy compressed
	buf, err := blk[0].Encode()
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

// BenchmarkFrom/orcby-8       	   10000	    215843 ns/op	  469689 B/op	    1203 allocs/op
// BenchmarkFrom/batch-8       	  138270	      8714 ns/op	    7651 B/op	     100 allocs/op
func BenchmarkFrom(b *testing.B) {
	orc, err := ioutil.ReadFile(smallFile)
	noerror(err)

	b.Run("orc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, err = FromOrcBy(orc, "string1")
			noerror(err)
		}
	})

	b.Run("batch", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, err = FromBatchBy(testBatch, "d")
			noerror(err)
		}
	})

}

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}
