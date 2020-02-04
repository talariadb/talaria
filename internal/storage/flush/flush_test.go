// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"bytes"
	"compress/flate"

	eorc "github.com/crphang/orc"

	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/orc"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage/flush/writers"
	"github.com/kelindar/binary"
	"io/ioutil"
	"testing"
)

const testBlockFile = "../../../test/testBlocks"

func TestMerge(t *testing.T) {
	flusher := New(monitor.NewNoop(), nil)

	schema := typeof.Schema{
		"col1": typeof.Int64,
		"col2": typeof.Float64,
	}
	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		t.Fatal(err)
	}

	orcBuffer1 := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer1,
		eorc.SetSchema(orcSchema))
	_ = writer.Write(1, 1.0)
	_ = writer.Close()

	orcBuffer2 := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer2,
		eorc.SetSchema(orcSchema))
	_ = writer.Write(2, 2.0)
	_ = writer.Close()

	block1, err := block.FromOrcBy(orcBuffer1.Bytes(), "col1")
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col1")

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	_, mergedValue := flusher.Merge(mergedBlocks, schema)

	orcBuffer := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer,
		eorc.SetSchema(orcSchema),
		eorc.SetCompression(eorc.CompressionZlib{Level: flate.DefaultCompression}))
	_ = writer.Write(1, 1.0)
	_ = writer.Write(2, 2.0)
	_ = writer.Close()

	if !bytes.Equal(orcBuffer.Bytes(), mergedValue) {
		t.Fatal("Merged orc value differ")
	}
}

// BenchmarkFlush runs a benchmark for a Merge function for flushing
// To run it, go in the directory and do 'go test -benchmem -bench=. -benchtime=1s'
// BenchmarkFlush/flush-12         	       5	2427187755 ns/op	1872955505 B/op	25340502 allocs/op
func BenchmarkFlush(b *testing.B) {
	// create monitor
	monitor := monitor.NewNoop()

	// Create flusher
	noopWriter := writers.NewNoop()
	flusher := New(monitor, noopWriter)

	// Append some files

	blksData, _ := ioutil.ReadFile(testBlockFile)
	blocks := make([]block.Block, 0)
	_ = binary.Unmarshal(blksData, &blocks)

	// Run the actual benchmark
	b.Run("flush", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			flusher.Merge(blocks, blocks[0].Schema())
		}
	})

}
