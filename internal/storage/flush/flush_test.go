// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"bytes"
	"compress/flate"
	"io/ioutil"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/orc"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage/flush/writers"
	"github.com/kelindar/binary"
)

const testBlockFile = "../../../test/testBlocks"

func TestMerge(t *testing.T) {
	fileNameFunc := func(row map[string]interface{}) (string, error) {
		lua, _ := column.NewComputed("fileName", typeof.String, `

		function main(row)
			current_time = 0
			fileName = string.format("%s-%d",row["col0"],0)
			return fileName
		end`)

		output, err := lua.Value(row)
		return output.(string), err
	}
	flusher := New(monitor.NewNoop(), writers.NewNoop(), fileNameFunc)

	schema := typeof.Schema{
		"col0": typeof.String,
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
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	orcBuffer2 := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer2,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 2, 2.0)
	_ = writer.Close()

	block1, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0")
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0")

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	fileName, mergedValue := flusher.Merge(mergedBlocks, schema)

	orcBuffer := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer,
		eorc.SetSchema(orcSchema),
		eorc.SetCompression(eorc.CompressionZlib{Level: flate.DefaultCompression}))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Write("eventName", 2, 2.0)
	_ = writer.Close()

	if !bytes.Equal(orcBuffer.Bytes(), mergedValue) {
		t.Fatal("Merged orc value differ")
	}

	if !bytes.Equal([]byte("eventName-0"), fileName) {
		t.Fatal("File name differ")
	}
}

func TestMerge_DifferentSchema(t *testing.T) {
	fileNameFunc := func(row map[string]interface{}) (string, error) {
		lua, _ := column.NewComputed("fileName", typeof.String, `

		function main(row)
			current_time = 0
			fileName = string.format("%s-%d",row["col0"],0)
			return fileName
		end`)

		output, err := lua.Value(row)
		return output.(string), err
	}
	flusher := New(monitor.NewNoop(), writers.NewNoop(), fileNameFunc)

	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
	}

	schema2 := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
		"col3": typeof.String,
	}
	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		t.Fatal(err)
	}

	orcSchema2, err := orc.SchemaFor(schema2)
	if err != nil {
		t.Fatal(err)
	}

	orcBuffer1 := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer1,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	orcBuffer2 := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer2,
		eorc.SetSchema(orcSchema2))
	_ = writer.Write("eventName", 2, 2.0, "s")
	_ = writer.Close()

	block1, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0")
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0")

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	fileName, mergedValue := flusher.Merge(mergedBlocks, schema2)

	orcBuffer := &bytes.Buffer{}
	writer, _ = eorc.NewWriter(orcBuffer,
		eorc.SetSchema(orcSchema2),
		eorc.SetCompression(eorc.CompressionZlib{Level: flate.DefaultCompression}))
	_ = writer.Write("eventName", 1, 1.0, nil)
	_ = writer.Write("eventName", 2, 2.0, "s")
	_ = writer.Close()

	if !bytes.Equal(orcBuffer.Bytes(), mergedValue) {
		t.Fatal("Merged orc value differ")
	}

	if !bytes.Equal([]byte("eventName-0"), fileName) {
		t.Fatal("File name differ")
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

	fileNameFunc := func(row map[string]interface{}) (string, error) {
		return "noop", nil
	}

	flusher := New(monitor, noopWriter, fileNameFunc)

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
