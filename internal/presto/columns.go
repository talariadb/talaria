// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// The types of the columns supported
const (
	TypeUnknown = byte(iota)
	TypeInt32
	TypeInt64
	TypeFloat64
	TypeString
	TypeBool
	TypeTimestamp
	TypeJSON
)

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftInteger) Append(v interface{}) int {
	const size = 2 + 4
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Ints = append(b.Ints, 0)
		return size
	}

	switch v := v.(type) {
	case int64:
		b.Nulls = append(b.Nulls, false)
		b.Ints = append(b.Ints, int32(v))
	case int32:
		b.Nulls = append(b.Nulls, false)
		b.Ints = append(b.Ints, v)
	default:
		b.Nulls = append(b.Nulls, true)
		b.Ints = append(b.Ints, 0)
	}

	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftInteger) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.IntegerData.Count()
	}

	nulls := make([]bool, 0, count)
	ints := make([]int32, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Ints = append(ints, b.Ints...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.IntegerData.Nulls...)
		b.Ints = append(b.Ints, a.IntegerData.Ints...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftInteger) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		IntegerData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftInteger) Size() int {
	const size = 2 + 4
	return size * b.Count()
}

// Count returns the number of elements in the block
func (b *PrestoThriftInteger) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftInteger) Kind() byte {
	return TypeInt32
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftBigint) Append(v interface{}) int {
	const size = 2 + 8
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Longs = append(b.Longs, 0)
		return size
	}

	b.Nulls = append(b.Nulls, false)
	b.Longs = append(b.Longs, v.(int64))
	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftBigint) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.BigintData.Count()
	}

	nulls := make([]bool, 0, count)
	longs := make([]int64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Longs = append(longs, b.Longs...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.BigintData.Nulls...)
		b.Longs = append(b.Longs, a.BigintData.Longs...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftBigint) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		BigintData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftBigint) Size() int {
	const size = 2 + 8
	return size * b.Count()
}

// Count returns the number of elements in the block
func (b *PrestoThriftBigint) Count() int {
	return len(b.Nulls)
}

// First returns the first element
func (b *PrestoThriftBigint) First() int64 {
	if b.Longs == nil || len(b.Longs) == 0 {
		return 0
	}

	return b.Longs[0]
}

// Kind returns a type of the block
func (b *PrestoThriftBigint) Kind() byte {
	return TypeInt64
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftDouble) Append(v interface{}) int {
	const size = 2 + 8
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Doubles = append(b.Doubles, 0)
		return size
	}

	b.Nulls = append(b.Nulls, false)
	b.Doubles = append(b.Doubles, reflect.ValueOf(v).Float())
	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftDouble) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.DoubleData.Count()
	}

	nulls := make([]bool, 0, count)
	doubles := make([]float64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Doubles = append(doubles, b.Doubles...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.DoubleData.Nulls...)
		b.Doubles = append(b.Doubles, a.DoubleData.Doubles...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftDouble) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		DoubleData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftDouble) Size() int {
	const size = 2 + 8
	return size * b.Count()
}

// Count returns the number of elements in the block
func (b *PrestoThriftDouble) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftDouble) Kind() byte {
	return TypeFloat64
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftVarchar) Append(v interface{}) int {
	const size = 2 + 4
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Sizes = append(b.Sizes, 0)
		return size
	}

	length := len(v.(string))
	b.Nulls = append(b.Nulls, false)
	b.Sizes = append(b.Sizes, int32(length))
	b.Bytes = append(b.Bytes, []byte(v.(string))...)
	return size + length
}

// AppendBlock appends an entire block
func (b *PrestoThriftVarchar) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.VarcharData.Count()
	}

	nulls := make([]bool, 0, count)
	sizes := make([]int32, 0, count)
	bytes := make([]byte, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Sizes = append(sizes, b.Sizes...)
	b.Bytes = append(bytes, b.Bytes...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.VarcharData.Nulls...)
		b.Sizes = append(b.Sizes, a.VarcharData.Sizes...)
		b.Bytes = append(b.Bytes, a.VarcharData.Bytes...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftVarchar) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		VarcharData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftVarchar) Size() int {
	const size = 2 + 4
	return (size * b.Count()) + len(b.Bytes)
}

// Count returns the number of elements in the block
func (b *PrestoThriftVarchar) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftVarchar) Kind() byte {
	return TypeString
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftBoolean) Append(v interface{}) int {
	const size = 2 + 2
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Booleans = append(b.Booleans, false)
		return size
	}

	b.Nulls = append(b.Nulls, false)
	b.Booleans = append(b.Booleans, reflect.ValueOf(v).Bool())
	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftBoolean) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.BooleanData.Count()
	}

	nulls := make([]bool, 0, count)
	bools := make([]bool, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Booleans = append(bools, b.Booleans...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.BooleanData.Nulls...)
		b.Booleans = append(b.Booleans, a.BooleanData.Booleans...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftBoolean) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		BooleanData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftBoolean) Size() int {
	const size = 2 + 2
	return size * b.Count()
}

// Count returns the number of elements in the block
func (b *PrestoThriftBoolean) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftBoolean) Kind() byte {
	return TypeBool
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftTimestamp) Append(v interface{}) int {
	const size = 2 + 8
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Timestamps = append(b.Timestamps, 0)
		return size
	}

	switch v := v.(type) {
	case int64:
		b.Nulls = append(b.Nulls, false)
		b.Timestamps = append(b.Timestamps, v)
	case time.Time:
		b.Nulls = append(b.Nulls, false)
		b.Timestamps = append(b.Timestamps, v.Unix())
	default:
		b.Nulls = append(b.Nulls, false)
		b.Timestamps = append(b.Timestamps, reflect.ValueOf(v).Int())
	}

	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftTimestamp) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.TimestampData.Count()
	}

	nulls := make([]bool, 0, count)
	times := make([]int64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Timestamps = append(times, b.Timestamps...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.TimestampData.Nulls...)
		b.Timestamps = append(b.Timestamps, a.TimestampData.Timestamps...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftTimestamp) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		TimestampData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftTimestamp) Size() int {
	const size = 2 + 8
	return size * b.Count()
}

// Count returns the number of elements in the block
func (b *PrestoThriftTimestamp) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftTimestamp) Kind() byte {
	return TypeTimestamp
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftJson) Append(v interface{}) int {
	const size = 2 + 4
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Sizes = append(b.Sizes, 0)
		return size
	}

	var data []byte
	switch v := v.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	case json.RawMessage:
		data = []byte(v)
	default:
		panic(fmt.Errorf("thrift json: unsupported type %T", v))
	}

	length := len(data)
	b.Nulls = append(b.Nulls, false)
	b.Sizes = append(b.Sizes, int32(length))
	b.Bytes = append(b.Bytes, data...)
	return size + length
}

// AppendBlock appends an entire block
func (b *PrestoThriftJson) AppendBlock(blocks ...PrestoThriftBlock) {
	count := b.Count()
	for _, a := range blocks {
		count += a.JsonData.Count()
	}

	nulls := make([]bool, 0, count)
	sizes := make([]int32, 0, count)
	bytes := make([]byte, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Sizes = append(sizes, b.Sizes...)
	b.Bytes = append(bytes, b.Bytes...)

	for _, a := range blocks {
		b.Nulls = append(b.Nulls, a.JsonData.Nulls...)
		b.Sizes = append(b.Sizes, a.JsonData.Sizes...)
		b.Bytes = append(b.Bytes, a.JsonData.Bytes...)
	}
}

// AsBlock returns a block for the response.
func (b *PrestoThriftJson) AsBlock() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		JsonData: b,
	}
}

// Size returns the size of the column, in bytes.
func (b *PrestoThriftJson) Size() int {
	const size = 2 + 4
	return (size * b.Count()) + len(b.Bytes)
}

// Count returns the number of elements in the block
func (b *PrestoThriftJson) Count() int {
	return len(b.Nulls)
}

// Kind returns a type of the block
func (b *PrestoThriftJson) Kind() byte {
	return TypeString
}
