// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/grab/talaria/internal/encoding/typeof"
	talaria "github.com/grab/talaria/proto"
)

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
func (b *PrestoThriftInteger) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftInteger).Count()
	}

	nulls := make([]bool, 0, count)
	ints := make([]int32, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Ints = append(ints, b.Ints...)

	for _, a := range blocks {
		block := a.(*PrestoThriftInteger)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Ints = append(b.Ints, block.Ints...)
	}
}

// Last returns the last value
func (b *PrestoThriftInteger) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}
	return b.Ints[offset]
}

// AsThrift returns a block for the response.
func (b *PrestoThriftInteger) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		IntegerData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftInteger) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Int32{
			Int32: &talaria.ColumnOfInt32{
				Nulls: b.Nulls,
				Ints:  b.Ints,
			},
		},
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
func (b *PrestoThriftInteger) Kind() typeof.Type {
	return typeof.Int32
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftInteger) Min() (int64, bool) {
	if len(b.Ints) == 0 {
		return 0, false
	}

	// Go through the array and find the min value
	min := int32(math.MaxInt32)
	for i, v := range b.Ints {
		if v < min && !b.Nulls[i] {
			min = b.Ints[i]
		}
	}

	return int64(min), min != math.MaxInt32
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftInteger) Range(from int, until int, f func(int, interface{})) {
	for i := from; i < until; i++ {
		if i >= len(b.Ints) {
			break
		}

		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		f(i, b.Ints[i])
	}
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
func (b *PrestoThriftBigint) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftBigint).Count()
	}

	nulls := make([]bool, 0, count)
	longs := make([]int64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Longs = append(longs, b.Longs...)

	for _, a := range blocks {
		block := a.(*PrestoThriftBigint)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Longs = append(b.Longs, block.Longs...)
	}
}

// Last returns the last value
func (b *PrestoThriftBigint) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}
	return b.Longs[offset]
}

// AsThrift returns a block for the response.
func (b *PrestoThriftBigint) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		BigintData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftBigint) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Int64{
			Int64: &talaria.ColumnOfInt64{
				Nulls: b.Nulls,
				Longs: b.Longs,
			},
		},
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

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftBigint) Min() (int64, bool) {
	if len(b.Longs) == 0 {
		return 0, false
	}

	// Go through the array and find the min value
	min := int64(math.MaxInt64)
	for i, v := range b.Longs {
		if v < min && !b.Nulls[i] {
			min = b.Longs[i]
		}
	}

	return min, min != math.MaxInt64
}

// Kind returns a type of the block
func (b *PrestoThriftBigint) Kind() typeof.Type {
	return typeof.Int64
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftBigint) Range(from int, until int, f func(int, interface{})) {
	for i := from; i < until; i++ {
		if i >= len(b.Longs) {
			break
		}

		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		f(i, b.Longs[i])
	}
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
func (b *PrestoThriftDouble) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftDouble).Count()
	}

	nulls := make([]bool, 0, count)
	doubles := make([]float64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Doubles = append(doubles, b.Doubles...)

	for _, a := range blocks {
		block := a.(*PrestoThriftDouble)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Doubles = append(b.Doubles, block.Doubles...)
	}
}

// Last returns the last value
func (b *PrestoThriftDouble) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}
	return b.Doubles[offset]
}

// AsThrift returns a block for the response.
func (b *PrestoThriftDouble) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		DoubleData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftDouble) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Float64{
			Float64: &talaria.ColumnOfFloat64{
				Nulls:   b.Nulls,
				Doubles: b.Doubles,
			},
		},
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
func (b *PrestoThriftDouble) Kind() typeof.Type {
	return typeof.Float64
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftDouble) Min() (int64, bool) {
	return 0, false
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftDouble) Range(from int, until int, f func(int, interface{})) {
	for i := from; i < until; i++ {
		if i >= len(b.Doubles) {
			break
		}

		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		f(i, b.Doubles[i])
	}
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
func (b *PrestoThriftVarchar) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftVarchar).Count()
	}

	nulls := make([]bool, 0, count)
	sizes := make([]int32, 0, count)
	bytes := make([]byte, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Sizes = append(sizes, b.Sizes...)
	b.Bytes = append(bytes, b.Bytes...)

	for _, a := range blocks {
		block := a.(*PrestoThriftVarchar)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Sizes = append(b.Sizes, block.Sizes...)
		b.Bytes = append(b.Bytes, block.Bytes...)
	}
}

// Last returns the last value
func (b *PrestoThriftVarchar) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}

	size := int(b.Sizes[offset])
	out := b.Bytes[len(b.Bytes)-size:]
	return binaryToString(&out)
}

// AsThrift returns a block for the response.
func (b *PrestoThriftVarchar) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		VarcharData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftVarchar) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_String_{
			String_: &talaria.ColumnOfString{
				Nulls: b.Nulls,
				Sizes: b.Sizes,
				Bytes: b.Bytes,
			},
		},
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
func (b *PrestoThriftVarchar) Kind() typeof.Type {
	return typeof.String
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftVarchar) Min() (int64, bool) {
	return 0, false
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftVarchar) Range(from int, until int, f func(int, interface{})) {
	var offset int32
	// Seek to current offset
	for k := 0; k < from; k++ {
		offset += b.Sizes[k]
	}

	for i := from; i < until; i++ {
		if i >= len(b.Sizes) {
			break
		}

		size := b.Sizes[i]
		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		v := b.Bytes[offset:offset+size]
		f(i, binaryToString(&v))
		offset += size
	}
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
func (b *PrestoThriftBoolean) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftBoolean).Count()
	}

	nulls := make([]bool, 0, count)
	bools := make([]bool, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Booleans = append(bools, b.Booleans...)

	for _, a := range blocks {
		block := a.(*PrestoThriftBoolean)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Booleans = append(b.Booleans, block.Booleans...)
	}
}

// Last returns the last value
func (b *PrestoThriftBoolean) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}
	return b.Booleans[offset]
}

// AsThrift returns a block for the response.
func (b *PrestoThriftBoolean) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		BooleanData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftBoolean) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Bool{
			Bool: &talaria.ColumnOfBools{
				Nulls: b.Nulls,
				Bools: b.Booleans,
			},
		},
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
func (b *PrestoThriftBoolean) Kind() typeof.Type {
	return typeof.Bool
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftBoolean) Min() (int64, bool) {
	return 0, false
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftBoolean) Range(from int, until int, f func(int, interface{})) {
	for i := from; i < until; i++ {
		if i >= len(b.Booleans) {
			break
		}

		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		f(i, b.Booleans[i])
	}
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
		b.Timestamps = append(b.Timestamps, v.UnixNano()/1000000) // UNIX time in millisecond
	default:
		b.Nulls = append(b.Nulls, false)
		b.Timestamps = append(b.Timestamps, reflect.ValueOf(v).Int())
	}

	return size
}

// AppendBlock appends an entire block
func (b *PrestoThriftTimestamp) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftTimestamp).Count()
	}

	nulls := make([]bool, 0, count)
	times := make([]int64, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Timestamps = append(times, b.Timestamps...)

	for _, a := range blocks {
		block := a.(*PrestoThriftTimestamp)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Timestamps = append(b.Timestamps, block.Timestamps...)
	}
}

// Last returns the last value
func (b *PrestoThriftTimestamp) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}
	return b.Timestamps[offset]
}

// AsThrift returns a block for the response.
func (b *PrestoThriftTimestamp) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		TimestampData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftTimestamp) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Time{
			Time: &talaria.ColumnOfInt64{
				Nulls: b.Nulls,
				Longs: b.Timestamps,
			},
		},
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
func (b *PrestoThriftTimestamp) Kind() typeof.Type {
	return typeof.Timestamp
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftTimestamp) Min() (int64, bool) {
	return 0, false
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftTimestamp) Range(from int, until int, f func(int, interface{})) {
	for i := from; i < until; i++ {
		if i >= len(b.Timestamps) {
			break
		}

		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		f(i, b.Timestamps[i])
	}
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
func (b *PrestoThriftJson) AppendBlock(blocks []Column) {
	count := b.Count()
	for _, a := range blocks {
		count += a.(*PrestoThriftJson).Count()
	}

	nulls := make([]bool, 0, count)
	sizes := make([]int32, 0, count)
	bytes := make([]byte, 0, count)

	b.Nulls = append(nulls, b.Nulls...)
	b.Sizes = append(sizes, b.Sizes...)
	b.Bytes = append(bytes, b.Bytes...)

	for _, a := range blocks {
		block := a.(*PrestoThriftJson)
		b.Nulls = append(b.Nulls, block.Nulls...)
		b.Sizes = append(b.Sizes, block.Sizes...)
		b.Bytes = append(b.Bytes, block.Bytes...)
	}
}

// Last returns the last value
func (b *PrestoThriftJson) Last() interface{} {
	offset := len(b.Nulls) - 1
	if offset < 0 || b.Nulls[offset] {
		return nil
	}

	size := int(b.Sizes[offset])
	return json.RawMessage(b.Bytes[len(b.Bytes)-size:])
}

// AsThrift returns a block for the response.
func (b *PrestoThriftJson) AsThrift() *PrestoThriftBlock {
	return &PrestoThriftBlock{
		JsonData: b,
	}
}

// AsProto returns a block for the response.
func (b *PrestoThriftJson) AsProto() *talaria.Column {
	return &talaria.Column{
		Value: &talaria.Column_Json{
			Json: &talaria.ColumnOfString{
				Nulls: b.Nulls,
				Sizes: b.Sizes,
				Bytes: b.Bytes,
			},
		},
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
func (b *PrestoThriftJson) Kind() typeof.Type {
	return typeof.JSON
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftJson) Min() (int64, bool) {
	return 0, false
}

// Range iterates over the column executing f on its elements
func (b *PrestoThriftJson) Range(from int, until int, f func(int, interface{})) {
	var offset int32
	// Seek to current offset
	for k := 0; k < from; k++ {
		offset += b.Sizes[k]
	}

	for i := from; i < until; i++ {
		if i >= len(b.Sizes) {
			break
		}

		size := b.Sizes[i]
		if b.Nulls[i] {
			f(i, nil)
			continue
		}

		v := b.Bytes[offset:offset+size]
		f(i, binaryToString(&v))
		offset += size
	}
}

// Converts binary to string in a zero-alloc manner
func binaryToString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}

