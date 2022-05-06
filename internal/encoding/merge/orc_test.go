// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package merge

import (
	"bytes"
	"compress/flate"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

const testBlockFile = "../../../test/testBlocks"

func TestToOrc(t *testing.T) {

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
	assert.NoError(t, err)
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0", nil, apply)
	assert.NoError(t, err)

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	mergedValue, err := ToOrc(mergedBlocks, schema)
	assert.NoError(t, err)

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
}

func TestMerge_DifferentSchema(t *testing.T) {
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
	assert.NoError(t, err)
	block2, err := block.FromOrcBy(orcBuffer2.Bytes(), "col0", nil, apply)
	assert.NoError(t, err)

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	mergedValue, err := ToOrc(mergedBlocks, schema2)
	assert.NoError(t, err)

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

}
