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
// cpu: Intel(R) Core(TM) i7-8850H CPU @ 2.60GHz
// BenchmarkMerge/orc-12                  1        7868658835 ns/op        2021461008 B/op26978868 allocs/op
// BenchmarkMerge/parquet-12          13016             91517 ns/op          225734 B/op       294 allocs/op
// BenchmarkMerge/block-12                1        8572385675 ns/op        64364809048 B/op   58445 allocs/op
// PASS
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
			ToOrc(blocks)
		}
	})

	// Run the actual benchmark
	b.Run("parquet", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			ToParquet(blocks)
		}
	})

	// Run the actual benchmark
	b.Run("block", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			ToBlock(blocks)
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
		o, err := New("parquet")
		assert.NotNil(t, o)
		assert.NoError(t, err)
	}
	{
		o, err := New("block")
		assert.NotNil(t, o)
		assert.NoError(t, err)
	}
	{
		o, err := New("xxx")
		assert.Nil(t, o["block"])
		assert.NotNil(t, o["row"])
		assert.NoError(t, err)
	}
}
