package merge

import (
	"io/ioutil"
	"testing"

	"github.com/kelindar/binary"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/stretchr/testify/assert"
)

// BenchmarkFlush runs a benchmark for a Merge function for flushing
// To run it, go in the directory and do 'go test -benchmem -bench=. -benchtime=1s'
// BenchmarkMerge/orc-8         	       1	7195029600 ns/op	2101578032 B/op	36859501 allocs/op
// BenchmarkMerge/parquet-12     	       1	18666411036 ns/op	5142058320 B/op	115850463 allocs/op
func BenchmarkMerge(b *testing.B) {

	// Append some files
	blksData, _ := ioutil.ReadFile(testBlockFile)
	blocks := make([]block.Block, 0)
	_ = binary.Unmarshal(blksData, &blocks)

	// Run the actual benchmark
	b.Run("orc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			ToOrc(blocks, blocks[0].Schema())
		}
	})

	// Run the actual benchmark
	b.Run("parquet", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			ToParquet(blocks, blocks[0].Schema())
		}
	})
}

func TestMergeNew(t *testing.T) {

	{
		o, err := New("orc")
		assert.NotNil(t, o)
		assert.NoError(t, err)
	}

	{
		o, err := New("")
		assert.NotNil(t, o)
		assert.NoError(t, err)
	}

	{
		o, err := New("xxx")
		assert.Nil(t, o)
		assert.Error(t, err)
	}
}
