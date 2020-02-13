// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"github.com/grab/talaria/internal/column"
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
	Size    int64          // The unencoded size of the block
	Key     nocopy.String  // The key of the block
	Columns nocopy.ByteMap // The encoded column metadata
	Data    nocopy.Bytes   // The set of columnar data
	Expires int64          // The expiration time for the block, in unix seconds
	schema  typeof.Schema  `binary:"-"` // The cached schema of the block
}

// Read decodes the block and selects the columns
func Read(buffer []byte, desiredSchema typeof.Schema) (column.Columns, error) {
	block, err := FromBuffer(buffer)
	if err != nil {
		return nil, err
	}

	// Compare the block schema with the desired schema to see if there's missing or mismatched columns
	schema := block.Schema()
	misses, ok := schema.Compare(desiredSchema)
	if ok { // Happy path, simply select the columns
		return block.Select(desiredSchema)
	}

	// Select the valid columns
	common := schema.Except(misses)
	if len(common) == 0 {
		return column.Columns{}, nil
	}

	// Select the common columns
	result, err := block.Select(common)
	if err != nil {
		return nil, err
	}

	// Get the number of rows in the block so we can backfill
	count := result.Any().Count()

	// For every miss, create an empty column
	for col, typ := range misses {
		result[col] = column.NullColumn(typ, count)
	}

	return result, nil
}

// Schema returns a schema of the block.
func (b *Block) Schema() typeof.Schema {
	if b.schema == nil {
		b.schema = make(typeof.Schema, len(b.Columns))
		for column, meta := range b.Columns {
			b.schema[column] = typeof.Type(meta[8])
		}
	}

	return b.schema
}

// Encode encodes the block as bytes
func (b *Block) Encode() ([]byte, error) {
	return binary.Marshal(b)
}

// Select selects a set of thrift columns
func (b *Block) Select(columns typeof.Schema) (column.Columns, error) {
	response := make(column.Columns, len(columns))
	for column := range columns {
		meta, ok := b.Columns[column]
		if !ok {
			return nil, fmt.Errorf("block: column %s was not found in the block", column)
		}

		offset := binary.BigEndian.Uint32(meta[0:4])
		size := binary.BigEndian.Uint32(meta[4:8])
		v, err := decodeValue(typeof.Type(meta[8]), b.Data[offset:offset+size])
		if err != nil {
			return nil, err
		}

		response[column] = v
	}

	return response, nil
}

// LastRow returns the last row of the block
func (b *Block) LastRow() (map[string]interface{}, error) {
	cols, err := b.Select(b.Schema())
	if err != nil {
		return nil, err
	}

	return cols.LastRow(), nil
}

// Min selects the smallest value for a column (must be an integer or a bigint)
func (b *Block) Min(column string) (int64, bool) {
	columns, err := b.Select(typeof.Schema{
		column: typeof.Int64,
	})
	if err != nil {
		return 0, false
	}

	col := columns[column]
	return col.Min()
}

// Writes a set of columns into the block
func (b *Block) writeColumns(columns column.Columns) error {
	var offset uint32
	var buffer bytes.Buffer

	b.Columns = make(nocopy.ByteMap, len(columns))
	for name, column := range columns {
		size, err := writeValue(column.AsThrift(), &buffer)
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

// readBlockOfBool reads a thrift block
func readBlockOfBool(buffer []byte) (presto.Column, error) {
	var v blockOfBool
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftBoolean{
		Nulls:    v.Nulls,
		Booleans: v.Booleans,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfInt32 ...
type blockOfInt32 struct {
	Nulls nocopy.Bools
	Ints  nocopy.Int32s
}

// readBlockOfInt32 reads a thrift block
func readBlockOfInt32(buffer []byte) (presto.Column, error) {
	var v blockOfInt32
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftInteger{
		Nulls: v.Nulls,
		Ints:  v.Ints,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfInt64 ...
type blockOfInt64 struct {
	Nulls nocopy.Bools
	Longs nocopy.Int64s
}

// readBlockOfInt64 reads a thrift block
func readBlockOfInt64(buffer []byte) (presto.Column, error) {
	var v blockOfInt64
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftBigint{
		Nulls: v.Nulls,
		Longs: v.Longs,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfFloat64 ...
type blockOfFloat64 struct {
	Nulls   nocopy.Bools
	Doubles nocopy.Float64s
}

// readBlockOfFloat64 reads a thrift block
func readBlockOfFloat64(buffer []byte) (presto.Column, error) {
	var v blockOfFloat64
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftDouble{
		Nulls:   v.Nulls,
		Doubles: v.Doubles,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfStrings ...
type blockOfStrings struct {
	Nulls nocopy.Bools
	Sizes nocopy.Int32s
	Bytes nocopy.Bytes
}

// readBlockOfFloat64 reads a thrift block
func readBlockOfStrings(buffer []byte) (presto.Column, error) {
	var v blockOfStrings
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftVarchar{
		Nulls: v.Nulls,
		Sizes: v.Sizes,
		Bytes: v.Bytes,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfTimestamp ...
type blockOfTimestamp struct {
	Nulls      nocopy.Bools
	Timestamps nocopy.Int64s
}

// readBlockOfTimestamp reads a thrift block
func readBlockOfTimestamp(buffer []byte) (presto.Column, error) {
	var v blockOfTimestamp
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftTimestamp{
		Nulls:      v.Nulls,
		Timestamps: v.Timestamps,
	}, nil
}

// ------------------------------------------------------------------------------------------

// blockOfJSON ...
type blockOfJSON struct {
	Nulls nocopy.Bools
	Sizes nocopy.Int32s
	Bytes nocopy.Bytes
}

// readBlockOfJSON reads a thrift block
func readBlockOfJSON(buffer []byte) (presto.Column, error) {
	var v blockOfJSON
	if err := binary.Unmarshal(buffer, &v); err != nil {
		return nil, err
	}

	return &presto.PrestoThriftJson{
		Nulls: v.Nulls,
		Sizes: v.Sizes,
		Bytes: v.Bytes,
	}, nil
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
	case b.TimestampData != nil:
		v = &blockOfTimestamp{Nulls: b.TimestampData.Nulls, Timestamps: b.TimestampData.Timestamps}
	case b.JsonData != nil:
		v = &blockOfJSON{Nulls: b.JsonData.Nulls, Sizes: b.JsonData.Sizes, Bytes: b.JsonData.Bytes}
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
func decodeValue(kind typeof.Type, b []byte) (presto.Column, error) {
	buffer, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	switch kind {
	case typeof.Int32:
		return readBlockOfInt32(buffer)
	case typeof.Int64:
		return readBlockOfInt64(buffer)
	case typeof.Float64:
		return readBlockOfFloat64(buffer)
	case typeof.Bool:
		return readBlockOfBool(buffer)
	case typeof.String:
		return readBlockOfStrings(buffer)
	case typeof.Timestamp:
		return readBlockOfTimestamp(buffer)
	case typeof.JSON:
		return readBlockOfJSON(buffer)
	}

	return nil, fmt.Errorf("column type %v is not supported", kind)
}

var emptyBlock = presto.PrestoThriftBlock{}
