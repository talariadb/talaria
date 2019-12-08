// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/golang/snappy"
	"github.com/grab/talaria/internal/encoding/typeof"
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
	Key     nocopy.String
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

			v, err := decodeValue(typeof.Type(meta[8]), b.Data[offset:offset+size])
			if err != nil {
				return nil, err
			}

			response[column] = v
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
		size, err := writeValue(column.AsBlock(), &buffer)
		if err != nil {
			return err
		}

		// Write the metadata, increment the offset and total size
		b.writeMeta(name, column.Kind(), offset, uint32(size))
		offset += uint32(size)
		b.Size += int64(column.Size())
	}

	b.Data = nocopy.Bytes(buffer.Bytes())

	return nil
}

// Writes a metadata into the column
func (b *Block) writeMeta(column string, kind typeof.Type, offset, size uint32) {
	meta := make([]byte, 9)
	binary.BigEndian.PutUint32(meta[0:4], offset)
	binary.BigEndian.PutUint32(meta[4:8], size)
	meta[8] = byte(kind)
	b.Columns[column] = meta
}

// ------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------

func writeValue(b *presto.PrestoThriftBlock, buffer *bytes.Buffer) (int, error) {
	var v interface{}
	switch {
	case b.IntegerData != nil:
		v = &blockOfInt32{Nulls: b.IntegerData.Nulls, Ints: b.IntegerData.Ints}
	case b.BigintData != nil:
		v = &blockOfInt64{Nulls: b.BigintData.Nulls, Longs: b.BigintData.Longs}
	case b.DoubleData != nil:
		v = &blockOfFloat64{Nulls: b.DoubleData.Nulls, Doubles: b.DoubleData.Doubles}
	case b.VarcharData != nil:
		v = &blockOfStrings{Nulls: b.VarcharData.Nulls, Sizes: b.VarcharData.Sizes, Bytes: b.VarcharData.Bytes}
	case b.BooleanData != nil:
		v = &blockOfBool{Nulls: b.BooleanData.Nulls, Booleans: b.BooleanData.Booleans}
	}

	// Marshal the block
	p, err := binary.Marshal(v)
	if err != nil {
		return 0, err
	}

	// Encoode and write
	return buffer.Write(snappy.Encode(nil, p))
}

// decodeValue decodes a value from the underlying buffer
func decodeValue(kind typeof.Type, b []byte) (presto.PrestoThriftBlock, error) {
	buffer, err := snappy.Decode(nil, b)
	if err != nil {
		return emptyBlock, err
	}

	switch kind {
	case typeof.Int32:
		var v blockOfInt32
		if err := binary.Unmarshal(buffer, &v); err != nil {
			return emptyBlock, err
		}
		return presto.PrestoThriftBlock{IntegerData: v.asColumn()}, nil

	case typeof.Int64:
		var v blockOfInt64
		if err := binary.Unmarshal(buffer, &v); err != nil {
			return emptyBlock, err
		}
		return presto.PrestoThriftBlock{BigintData: v.asColumn()}, nil

	case typeof.Float64:
		var v blockOfFloat64
		if err := binary.Unmarshal(buffer, &v); err != nil {
			return emptyBlock, err
		}
		return presto.PrestoThriftBlock{DoubleData: v.asColumn()}, nil

	case typeof.Bool:
		var v blockOfBool
		if err := binary.Unmarshal(buffer, &v); err != nil {
			return emptyBlock, err
		}
		return presto.PrestoThriftBlock{BooleanData: v.asColumn()}, nil

	case typeof.String:
		var v blockOfStrings
		if err := binary.Unmarshal(buffer, &v); err != nil {
			return emptyBlock, err
		}
		return presto.PrestoThriftBlock{VarcharData: v.asColumn()}, nil
	}

	return emptyBlock, fmt.Errorf("column type %v is not supported", kind)

}

var emptyBlock = presto.PrestoThriftBlock{}
