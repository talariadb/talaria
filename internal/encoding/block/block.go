// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"bytes"
	"errors"

	"github.com/golang/snappy"
	"github.com/grab/talaria/internal/presto"
	"github.com/kelindar/binary"
	"github.com/kelindar/binary/nocopy"
)

var (
	errSchemaMismatch    = errors.New("mismatch between internal schema and requested columns")
	errEmptyBatch        = errors.New("batch is empty")
	errPartitionNotFound = errors.New("one or more events in the batch does not contain the partition key")
	errPartitionInvalid  = errors.New("partition is not a string")
)

// Block represents a serialized block
type Block struct {
	Size    int64
	Columns nocopy.ByteMap
	Data    nocopy.Bytes
}

// FromBuffer unmarshals a block from a in-memory buffer.
func FromBuffer(b []byte) (block Block, err error) {
	err = binary.Unmarshal(b, &block)
	return
}

// Read decodes the block and selects the columns
func Read(buffer []byte, columns []string) (map[string]presto.PrestoThriftBlock, error) {
	block, err := FromBuffer(buffer)
	if err != nil {
		return nil, err
	}

	return block.Select(columns)
}

// Encode encodes the block as bytes
func (b Block) Encode() ([]byte, error) {
	return binary.Marshal(b)
}

// Select selects a set of thrift columns
func (b *Block) Select(columns []string) (map[string]presto.PrestoThriftBlock, error) {
	response := make(map[string]presto.PrestoThriftBlock, len(columns))

	for _, column := range columns {
		if meta, ok := b.Columns[column]; ok {
			offset := binary.BigEndian.Uint32(meta[0:4])
			size := binary.BigEndian.Uint32(meta[4:8])

			buffer, err := snappy.Decode(nil, b.Data[offset:offset+size])
			if err != nil {
				return nil, err
			}

			// Read the buffer at the offset
			var value value
			if err := binary.Unmarshal(buffer, &value); err != nil {
				return nil, err
			}
			response[column] = value.asBlock()
		}
	}

	return response, nil
}

// Writes a set of columns into the block
func (b *Block) writeColumns(columns presto.NamedColumns) error {
	var offset uint32
	var buffer bytes.Buffer

	b.Columns = make(nocopy.ByteMap, len(columns))
	for name, column := range columns {
		p, err := binary.Marshal(newValue(column.AsBlock()))
		if err != nil {
			return err
		}

		// Encoode and write
		buffer.Write(snappy.Encode(nil, p))
		size := uint32(buffer.Len() - int(offset))

		// Write the metadata, increment the offset and total size
		b.writeMeta(name, offset, size)
		offset += size
		b.Size += int64(column.Size())
	}

	b.Data = nocopy.Bytes(buffer.Bytes())
	return nil
}

// Writes a metadata into the column
func (b *Block) writeMeta(column string, offset, size uint32) {
	meta := make([]byte, 8)
	binary.BigEndian.PutUint32(meta[0:4], offset)
	binary.BigEndian.PutUint32(meta[4:8], size)
	b.Columns[column] = meta
}

// value ...
type value struct {
	VarcharData blockOfStrings
	IntegerData blockOfInt32
	BigintData  blockOfInt64
	DoubleData  blockOfFloat64
	BooleanData blockOfBool
}

// blockOfBool ...
type blockOfBool struct {
	Nulls    nocopy.Bools
	Booleans nocopy.Bools
}

// AsColumn converts this block to a presto thrift column.
func (v *blockOfBool) asColumn() *presto.PrestoThriftBoolean {
	if len(v.Nulls) == 0 {
		return nil
	}

	return &presto.PrestoThriftBoolean{
		Nulls:    v.Nulls,
		Booleans: v.Booleans,
	}
}

// blockOfInt32 ...
type blockOfInt32 struct {
	Nulls nocopy.Bools
	Ints  nocopy.Int32s
}

// AsColumn converts this block to a presto thrift column.
func (v *blockOfInt32) asColumn() *presto.PrestoThriftInteger {
	if len(v.Nulls) == 0 {
		return nil
	}

	return &presto.PrestoThriftInteger{
		Nulls: v.Nulls,
		Ints:  v.Ints,
	}
}

// blockOfInt64 ...
type blockOfInt64 struct {
	Nulls nocopy.Bools
	Longs nocopy.Int64s
}

// AsColumn converts this block to a presto thrift column.
func (v *blockOfInt64) asColumn() *presto.PrestoThriftBigint {
	if len(v.Nulls) == 0 {
		return nil
	}

	return &presto.PrestoThriftBigint{
		Nulls: v.Nulls,
		Longs: v.Longs,
	}
}

// blockOfFloat64 ...
type blockOfFloat64 struct {
	Nulls   nocopy.Bools
	Doubles nocopy.Float64s
}

// AsColumn converts this block to a presto thrift column.
func (v *blockOfFloat64) asColumn() *presto.PrestoThriftDouble {
	if len(v.Nulls) == 0 {
		return nil
	}

	return &presto.PrestoThriftDouble{
		Nulls:   v.Nulls,
		Doubles: v.Doubles,
	}
}

// blockOfStrings ...
type blockOfStrings struct {
	Nulls nocopy.Bools
	Sizes nocopy.Int32s
	Bytes nocopy.Bytes
}

// AsColumn converts this block to a presto thrift column.
func (v *blockOfStrings) asColumn() *presto.PrestoThriftVarchar {
	if len(v.Nulls) == 0 {
		return nil
	}

	return &presto.PrestoThriftVarchar{
		Nulls: v.Nulls,
		Sizes: v.Sizes,
		Bytes: v.Bytes,
	}
}

func newValue(b *presto.PrestoThriftBlock) (v value) {
	if b.IntegerData != nil {
		v.IntegerData.Ints = b.IntegerData.Ints
		v.IntegerData.Nulls = b.IntegerData.Nulls
	}
	if b.BigintData != nil {
		v.BigintData.Longs = b.BigintData.Longs
		v.BigintData.Nulls = b.BigintData.Nulls
	}
	if b.DoubleData != nil {
		v.DoubleData.Doubles = b.DoubleData.Doubles
		v.DoubleData.Nulls = b.DoubleData.Nulls
	}
	if b.VarcharData != nil {
		v.VarcharData.Nulls = b.VarcharData.Nulls
		v.VarcharData.Sizes = b.VarcharData.Sizes
		v.VarcharData.Bytes = b.VarcharData.Bytes
	}
	if b.BooleanData != nil {
		v.BooleanData.Booleans = b.BooleanData.Booleans
		v.BooleanData.Nulls = b.BooleanData.Nulls
	}
	return
}

func (v *value) asBlock() presto.PrestoThriftBlock {
	return presto.PrestoThriftBlock{
		IntegerData: v.IntegerData.asColumn(),
		BigintData:  v.BigintData.asColumn(),
		DoubleData:  v.DoubleData.asColumn(),
		VarcharData: v.VarcharData.asColumn(),
		BooleanData: v.BooleanData.asColumn(),
	}
}
