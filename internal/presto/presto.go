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

	"github.com/grab/talaria/internal/encoding/typeof"
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
	Kind() typeof.Type
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

	// Close the listener if context is cancelled
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

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

// NamedColumns represents a set of named columns
type NamedColumns map[string]Column

// Append adds a value at a particular index to the block.
func (c NamedColumns) Append(name string, value interface{}) bool {
	if col, exists := c[name]; exists {
		return col.Append(value) > 0
	}

	// Get the type of the value
	rt := reflect.TypeOf(value)
	typ, supported := typeof.FromType(rt)
	if !supported {
		return false
	}

	// If column does not exist, create it and fill it with nulls up until the max - 1
	newColumn := NewColumn(typ)
	until := c.Max() - 1
	for i := 0; i < until; i++ {
		newColumn.Append(nil)
	}

	c[name] = newColumn
	return newColumn.Append(value) > 0
}

// Max finds the maximum count of a column in the set
func (c NamedColumns) Max() (max int) {
	for _, column := range c {
		if count := column.Count(); count > max {
			max = count
		}
	}
	return
}

// FillNulls adds nulls onto all uneven columns left.
func (c NamedColumns) FillNulls() {
	max := c.Max()
	for _, column := range c {
		delta := max - column.Count()
		for i := 0; i < delta; i++ {
			column.Append(nil)
		}
	}
}

// ------------------------------------------------------------------------------------------------------------

// NewColumn creates a new appendable column
func NewColumn(t typeof.Type) Column {
	switch t {
	case typeof.String:
		return new(PrestoThriftVarchar)
	case typeof.Int32:
		return new(PrestoThriftInteger)
	case typeof.Int64:
		return new(PrestoThriftBigint)
	case typeof.Float64:
		return new(PrestoThriftDouble)
	case typeof.Bool:
		return new(PrestoThriftBoolean)
	case typeof.Timestamp:
		return new(PrestoThriftTimestamp)
	case typeof.JSON:
		return new(PrestoThriftJson)
	}

	panic(fmt.Errorf("presto: unknown type %v", t))
}

// Size returns the size of the block.
func (b *PrestoThriftBlock) Size() int {
	switch {
	case b.IntegerData != nil:
		return b.IntegerData.Size()
	case b.BigintData != nil:
		return b.BigintData.Size()
	case b.VarcharData != nil:
		return b.VarcharData.Size()
	case b.DoubleData != nil:
		return b.DoubleData.Size()
	case b.BooleanData != nil:
		return b.BooleanData.Size()
	case b.TimestampData != nil:
		return b.TimestampData.Size()
	case b.JsonData != nil:
		return b.JsonData.Size()
	}
	return 0
}

// Min returns the minimum value of the column (only works for numbers).
func (b *PrestoThriftBlock) Min() (int64, bool) {
	switch {
	case b.IntegerData != nil:
		return b.IntegerData.Min()
	case b.BigintData != nil:
		return b.BigintData.Min()
	}
	return 0, false
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
		return toTime(r.Low.Value.BigintData.Min()), toTime(r.High.Value.BigintData.Min()), true

	// Lower bound [t0, max]
	case r.Low.Bound == PrestoThriftBoundAbove:
		return toTime(r.Low.Value.BigintData.Min()), time.Unix(math.MaxInt64, 0), true

	// Upper bound [min, t0]
	case r.Low.Bound == PrestoThriftBoundBelow:
		return time.Unix(0, 0), toTime(r.Low.Value.BigintData.Min()), true

	}

	return zero, zero, false
}

// Converts time provided to a golang time
func toTime(t int64, ok bool) time.Time {
	if !ok {
		return time.Unix(t, 0)
	}

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
