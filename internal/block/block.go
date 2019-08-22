// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"errors"
	"sort"

	"github.com/kelindar/binary"
	"github.com/kelindar/binary/nocopy"
	"gitlab.myteksi.net/grab-x/talaria/internal/orc"
	"gitlab.myteksi.net/grab-x/talaria/internal/presto"
)

var (
	errSchemaMismatch = errors.New("mismatch between internal schema and requested columns")
)

// Block represents a serializ
type Block struct {
	Size int64
	Data map[string][]byte
}

// FromBuffer unmarshals a block from a in-memory buffer.
func FromBuffer(b []byte) (block Block, err error) {
	err = binary.Unmarshal(b, &block)
	return
}

// FromOrc ...
func FromOrc(b []byte) (block *Block, err error) {
	i, err := orc.FromBuffer(b)
	if err != nil {
		return nil, err
	}

	// Get the list of columns in the ORC file
	var columns []string
	schema := i.Schema()
	for k := range schema {
		columns = append(columns, k)
	}

	// Sort the columns for consistency
	sort.Strings(columns)

	// Create presto columns
	var blocks []presto.Column
	for _, c := range columns {
		if kind, hasType := schema[c]; hasType {
			appender, ok := presto.NewColumn(kind)
			if !ok {
				return nil, errSchemaMismatch
			}

			blocks = append(blocks, appender)
		}
	}

	// Create a block
	block = new(Block)
	block.Data = make(map[string][]byte)
	i.Range(func(i int, row []interface{}) bool {
		for i, v := range row {
			block.Size += int64(blocks[i].Append(v))
		}
		return false
	}, columns...)

	for i, name := range columns {

		// Encode a column
		buffer, err := binary.Marshal(newValue(blocks[i].AsBlock()))
		if err != nil {
			return nil, err
		}

		// Assign encoded column to the block
		block.Data[name] = buffer
	}
	return
}

// Encode encodes the block as bytes
func (b Block) Encode() ([]byte, error) {
	return binary.Marshal(b)
}

// Select selects a set of thrift columns
func (b *Block) Select(columns ...string) (map[string]presto.PrestoThriftBlock, error) {
	response := make(map[string]presto.PrestoThriftBlock)
	for _, column := range columns {
		if buffer, ok := b.Data[column]; ok {
			var value value
			if err := binary.Unmarshal(buffer, &value); err != nil {
				return nil, err
			}

			response[column] = value.asBlock()
		}
	}

	return response, nil
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
