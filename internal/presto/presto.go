// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"reflect"
	"time"

	"github.com/samuel/go-thrift/thrift"
)

const frameSize = 1 << 24 // 16MB

// Column contract represent a column that can be appended.
type Column interface {
	Append(v interface{}) int
	AppendBlock(blocks ...PrestoThriftBlock)
	AsBlock() *PrestoThriftBlock
	Count() int
	Size() int
}

// Serve creates and serves thrift RPC for presto. Context is used for cancellation purposes.
func Serve(ctx context.Context, port int32, service PrestoThriftService) error {
	if err := rpc.RegisterName("Thrift", &PrestoThriftServiceServer{
		Implementation: service,
	}); err != nil {
		return err
	}

	// Create a TCP listener for our thrift
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return err
	}

	// Connection loop, old school
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if conn, err := ln.Accept(); err == nil {
				t := thrift.NewTransport(thrift.NewFramedReadWriteCloser(conn, frameSize), thrift.BinaryProtocol)
				go rpc.ServeCodec(thrift.NewServerCodec(t))
			}
		}
	}
}

// ------------------------------------------------------------------------------------------------------------

// Columns ...
type Columns []PrestoThriftBlock

// Size returns the space (in bytes) required for the set of blocks.
func (b Columns) Size() (size int) {
	for _, block := range b {
		size += block.Size()
	}
	return
}

// ------------------------------------------------------------------------------------------------------------

// NewColumn creates a new appendable column
func NewColumn(t reflect.Type) (Column, bool) {
	switch t.Name() {
	case "string":
		return new(PrestoThriftVarchar), true
	case "int32":
		return new(PrestoThriftInteger), true
	case "int64":
		return new(PrestoThriftBigint), true
	case "float64":
		return new(PrestoThriftDouble), true
	}

	return nil, false
}

// Size returns the size of the block.
func (b *PrestoThriftBlock) Size() int {
	if b.BigintData != nil {
		return b.BigintData.Size()
	}

	if b.VarcharData != nil {
		return b.VarcharData.Size()
	}

	if b.DoubleData != nil {
		return b.DoubleData.Size()
	}

	return 0
}

// ------------------------------------------------------------------------------------------------------------

// Append adds a value to the block.
func (b *PrestoThriftInteger) Append(v interface{}) int {
	const size = 2 + 4
	if v == nil {
		b.Nulls = append(b.Nulls, true)
		b.Ints = append(b.Ints, 0)
		return size
	}

	b.Nulls = append(b.Nulls, false)
	b.Ints = append(b.Ints, int32(v.(int64)))
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

// First returns the first element
func (b *PrestoThriftBigint) First() int64 {
	if b.Longs == nil || len(b.Longs) == 0 {
		return 0
	}

	return b.Longs[0]
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

// ------------------------------------------------------------------------------------------------------------

// AsTimeRange converts thrift range as a time range
func (r *PrestoThriftRange) AsTimeRange() (time.Time, time.Time, bool) {

	// We must always have a low bound
	zero := time.Unix(0, 0)
	if r.Low == nil || r.Low.Value == nil || r.Low.Value.BigintData == nil {
		return zero, zero, false
	}

	switch {

	// Concrete interval [t0, t1]
	case r.Low.Bound == PrestoThriftBoundExactly &&
		r.High != nil && r.High.Bound == PrestoThriftBoundExactly && r.High.Value.BigintData != nil:
		return toTime(r.Low.Value.BigintData.First()), toTime(r.High.Value.BigintData.First()), true

	// Lower bound [t0, max]
	case r.Low.Bound == PrestoThriftBoundAbove:
		return toTime(r.Low.Value.BigintData.First()), time.Unix(math.MaxInt64, 0), true

	// Upper bound [min, t0]
	case r.Low.Bound == PrestoThriftBoundBelow:
		return time.Unix(0, 0), toTime(r.Low.Value.BigintData.First()), true

	}

	return zero, zero, false
}

// Converts time provided to a golang time
func toTime(t int64) time.Time {
	watermark := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)

	switch {
	case t > watermark.UnixNano():
		return time.Unix(0, t)
	case t > watermark.UnixNano()/1000:
		return time.Unix(0, t*1000)
	case t > watermark.UnixNano()/1000000:
		return time.Unix(0, t*1000000)
	default:
		return time.Unix(t, 0)
	}
}
