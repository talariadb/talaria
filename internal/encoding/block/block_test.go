// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func setup() (Block, typeof.Schema) {

	const testFile = "../../../test/test4.csv"
	o, _ := ioutil.ReadFile(testFile)
	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := Transform(schema)
	b, _ := FromCSVBy(o, "raisedCurrency", &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}, apply)
	if len(b) > 0 {
		return b[0], *schema
	}
	return Block{}, *schema
}

func TestBlock_Types(t *testing.T) {
	o, err := ioutil.ReadFile(smallFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(nil)
	b, err := FromOrcBy(o, "string1", nil, apply)
	assert.Equal(t, 2, len(b))
	assert.NoError(t, err)

	schema := b[0].Schema()
	assert.Equal(t, 9, len(schema))
	assert.Contains(t, schema, "boolean1")
	assert.Contains(t, schema, "double1")
	assert.Contains(t, schema, "int1")
	assert.Contains(t, schema, "long1")
	assert.Contains(t, schema, "string1")
	assert.Contains(t, schema, "list")

	{
		result, err := b[0].Select(typeof.Schema{"int1": typeof.Int32})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["int1"].Count())
	}

	{
		result, err := b[0].Select(typeof.Schema{"string1": typeof.String})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["string1"].Count())
	}

	{
		result, err := b[0].Select(typeof.Schema{"long1": typeof.Int64})
		assert.NoError(t, err)
		assert.Equal(t, 1, result["long1"].Count())
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

// BenchmarkBlockRead/read-8         	   13884	     86016 ns/op	  321704 B/op	      12 allocs/op
func BenchmarkBlockRead(b *testing.B) {
	o, err := ioutil.ReadFile(testFile)
	noerror(err)

	apply := Transform(nil)
	blk, err := FromOrcBy(o, "_col5", nil, apply)
	noerror(err)

	// 122MB uncompressed
	// 13MB snappy compressed
	buf, err := blk[0].Encode()
	noerror(err)

	columns := typeof.Schema{
		"_col5": typeof.String,
	}

	b.Run("read", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, _ = Read(buf, columns)
		}
	})
}

// BenchmarkBlockReadForParquet/read-12         	  469470	      2482 ns/op	    1665 B/op	      16 allocs/op
func BenchmarkBlockReadForParquet(b *testing.B) {
	o, err := ioutil.ReadFile(testFileForParquet)
	noerror(err)

	apply := Transform(nil)
	blk, err := FromParquetBy(o, "foo", nil, apply)
	noerror(err)

	// 122MB uncompressed
	// 13MB snappy compressed
	buf, err := blk[0].Encode()
	noerror(err)

	columns := typeof.Schema{
		"foo": typeof.String,
	}

	b.Run("read", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, _ = Read(buf, columns)
		}
	})
}

// BenchmarkFrom/orc-8         	    3434	    309327 ns/op	  789779 B/op	    1906 allocs/op
// BenchmarkFrom/batch-8       	   50971	     22366 ns/op	   19824 B/op	     206 allocs/op
func BenchmarkFrom(b *testing.B) {
	orc, err := ioutil.ReadFile(smallFile)
	noerror(err)

	apply := Transform(nil)
	b.Run("orc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, err = FromOrcBy(orc, "string1", nil, apply)
			noerror(err)
		}
	})

	b.Run("batch", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, err = FromBatchBy(testBatch, "d", nil, apply)
			noerror(err)
		}
	})

	b.Run("parquet", func(b *testing.B) {
		o, err := ioutil.ReadFile(testFileForParquet)
		noerror(err)

		apply := Transform(nil)

		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_, err = FromParquetBy(o, "bar", nil, apply)
			noerror(err)
		}
	})
}

func noerror(err error) {
	if err != nil {
		panic(err)
	}
}

func TestFromBlock(t *testing.T) {
	blk, schema := setup()
	rows, err := FromBlockBy(blk, schema)
	assert.NoError(t, err)
	cols, err := blk.Select(schema)
	assert.NoError(t, err)
	rowCount := cols.Any().Count()
	// verify row count.
	assert.Equal(t, rowCount, len(rows))
	for _, row := range rows {
		// verify values
		assert.Contains(t, []string{"EUR", "CAD", "USD"}, row.Values["raisedCurrency"])
		// verify type
		assert.Equal(t, typeof.String, row.Schema["raisedCurrency"])
	}
}
