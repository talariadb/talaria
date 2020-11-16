// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"bytes"
	"compress/flate"
	"io/ioutil"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/binary"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/noop"
	"github.com/stretchr/testify/assert"
)

const testBlockFile = "../../../test/testBlocks"

func TestMerge(t *testing.T) {
	fileNameFunc := func(row map[string]interface{}) (string, error) {
		lua, _ := column.NewComputed("fileName", typeof.String, `

		function main(row)
			current_time = 0
			fileName = string.format("%s-%d",row["col0"],0)
			return fileName
		end`, script.NewLoader(nil))

		output, err := lua.Value(row)
		return output.(string), err
	}
	flusher := New(monitor.NewNoop(), noop.New(), fileNameFunc)

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

	apply := block.Transform(nil)

	block1, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0", nil, apply)
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0", nil, apply)

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
		end`, script.NewLoader(nil))

		output, err := lua.Value(row)
		return output.(string), err
	}
	flusher := New(monitor.NewNoop(), noop.New(), fileNameFunc)

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

	apply := block.Transform(nil)

	block1, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0", nil, apply)
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0", nil, apply)

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
	noopWriter := noop.New()

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

func TestNameFunc(t *testing.T) {
	fileNameFunc := func(row map[string]interface{}) (string, error) {
		lua, _ := column.NewComputed("fileName", typeof.String, `
	function main(row)

		-- Convert the time to a lua date
		local ts = row["col1"]
		local tz = timezone()
		local dt = os.date('*t', ts - tz)
	
		-- Format the filename
		return string.format("year=%d/month=%d/day=%d/ns=%s/%d-%d-%d-%s.orc", 
			dt.year,
			dt.month,
			dt.day,
			row["col0"],
			dt.hour,
			dt.min,
			dt.sec,
			"127.0.0.1")
	end

	function timezone()
		local utcdate   = os.date("!*t")
		local localdate = os.date("*t")
		--localdate.isdst = false
		return os.difftime(os.time(localdate), os.time(utcdate))
	end
	`, script.NewLoader(nil))

		output, err := lua.Value(row)
		return output.(string), err
	}
	flusher := New(monitor.NewNoop(), noop.New(), fileNameFunc)

	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Timestamp,
		"col2": typeof.Float64,
	}

	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		t.Fatal(err)
	}

	orcBuffer := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	apply := block.Transform(nil)

	blocks, err := block.FromOrcBy(orcBuffer.Bytes(), "col0", nil, apply)
	fileName, _ := flusher.Merge(blocks, schema)

	assert.Equal(t, "year=46970/month=3/day=29/ns=eventName/0-0-0-127.0.0.1.orc", string(fileName))

}
