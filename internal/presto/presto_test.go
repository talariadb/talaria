// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkAppendBlocks(b *testing.B) {
	var block PrestoThriftBigint
	for i := 0; i < 25000; i++ {
		block.Append(int64(i))
	}

	var blocks []PrestoThriftBlock
	for i := 0; i < 10; i++ {
		blocks = append(blocks, PrestoThriftBlock{BigintData: &block})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		var output PrestoThriftBigint
		output.AppendBlock(blocks...)
	}
}

func TestServeCancellation(t *testing.T) {
	assert.NotPanics(t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = Serve(ctx, 9999, nil)
	})
}

func TestAppend_Bigint(t *testing.T) {
	tests := []struct {
		desc      string
		input     interface{}
		output    *PrestoThriftBigint
		outputRes int
		size      int
		count     int
	}{
		{
			desc:  "int64 appended",
			input: int64(321),
			output: &PrestoThriftBigint{
				Nulls: []bool{false, false},
				Longs: []int64{123, 321},
			},
			outputRes: 10,
			size:      20,
			count:     2,
		},
		{
			desc:  "null value appended",
			input: nil,
			output: &PrestoThriftBigint{
				Nulls: []bool{false, true},
				Longs: []int64{123, 0},
			},
			outputRes: 10,
			size:      20,
			count:     2,
		},
		{
			desc: "block appended",
			input: PrestoThriftBigint{
				Nulls: []bool{false},
				Longs: []int64{456},
			},
			output: &PrestoThriftBigint{
				Nulls: []bool{false, false},
				Longs: []int64{123, 456},
			},
			size:  20,
			count: 2,
		},
	}

	for _, td := range tests {
		output := &PrestoThriftBigint{
			Nulls: []bool{false},
			Longs: []int64{123},
		}

		if array, ok := td.input.(PrestoThriftBigint); ok {
			t.Run(td.desc, func(*testing.T) {
				output.AppendBlock(*array.AsBlock())
				assert.Equal(t, td.count, output.Count(), td.desc)
			})
			continue
		}

		// Append a single value
		t.Run(td.desc, func(*testing.T) {
			res := output.Append(td.input)
			assert.Equal(t, td.outputRes, res, td.desc)
			assert.Equal(t, td.output, output, td.desc)
			assert.Equal(t, td.size, output.Size(), td.desc)
			assert.Equal(t, td.count, output.Count(), td.desc)
			assert.NotNil(t, output.AsBlock().BigintData)
		})
	}
}

func TestAppend_Varchar(t *testing.T) {
	tests := []struct {
		desc      string
		input     interface{}
		output    *PrestoThriftVarchar
		outputRes int
		size      int
		count     int
	}{
		{
			desc:  "varchar appended",
			input: "hello",
			output: &PrestoThriftVarchar{
				Nulls: []bool{false, false},
				Sizes: []int32{2, 5},
				Bytes: []byte("hihello"),
			},
			outputRes: 11,
			size:      19,
			count:     2,
		},
		{
			desc:  "null value appended",
			input: nil,
			output: &PrestoThriftVarchar{
				Nulls: []bool{false, true},
				Sizes: []int32{2, 0},
				Bytes: []byte("hi"),
			},
			outputRes: 6,
			size:      14,
			count:     2,
		},
		{
			desc: "block appended",
			input: PrestoThriftVarchar{
				Nulls: []bool{false},
				Sizes: []int32{5},
				Bytes: []byte("world"),
			},
			output: &PrestoThriftVarchar{
				Nulls: []bool{false, false},
				Sizes: []int32{2, 5},
				Bytes: []byte("hiworld"),
			},
			count: 2,
		},
	}

	for _, td := range tests {
		output := &PrestoThriftVarchar{
			Nulls: []bool{false},
			Sizes: []int32{2},
			Bytes: []byte("hi"),
		}

		if array, ok := td.input.(PrestoThriftVarchar); ok {
			t.Run(td.desc, func(*testing.T) {
				output.AppendBlock(*array.AsBlock())
				assert.Equal(t, td.count, output.Count(), td.desc)
			})
			continue
		}

		// Append a single value
		t.Run(td.desc, func(*testing.T) {
			res := output.Append(td.input)
			assert.Equal(t, td.outputRes, res, td.desc)
			assert.Equal(t, td.output, output, td.desc)
			assert.Equal(t, td.size, output.Size(), td.desc)
			assert.Equal(t, td.count, output.Count(), td.desc)
			assert.NotNil(t, output.AsBlock().VarcharData)
		})
	}
}

func TestAppend_Double(t *testing.T) {
	tests := []struct {
		desc      string
		input     interface{}
		output    *PrestoThriftDouble
		outputRes int
		size      int
		count     int
	}{
		{
			desc:  "int64 appended",
			input: float64(321),
			output: &PrestoThriftDouble{
				Nulls:   []bool{false, false},
				Doubles: []float64{123, 321},
			},
			outputRes: 10,
			size:      20,
			count:     2,
		},
		{
			desc:  "null value appended",
			input: nil,
			output: &PrestoThriftDouble{
				Nulls:   []bool{false, true},
				Doubles: []float64{123, 0},
			},
			outputRes: 10,
			size:      20,
			count:     2,
		},
		{
			desc: "block appended",
			input: PrestoThriftDouble{
				Nulls:   []bool{false},
				Doubles: []float64{456},
			},
			output: &PrestoThriftDouble{
				Nulls:   []bool{false, false},
				Doubles: []float64{123, 456},
			},
			size:  20,
			count: 2,
		},
	}

	for _, td := range tests {
		output := &PrestoThriftDouble{
			Nulls:   []bool{false},
			Doubles: []float64{123},
		}

		if array, ok := td.input.(PrestoThriftDouble); ok {
			t.Run(td.desc, func(*testing.T) {
				output.AppendBlock(*array.AsBlock())
				assert.Equal(t, td.count, output.Count(), td.desc)
			})
			continue
		}

		// Append a single value
		t.Run(td.desc, func(*testing.T) {
			res := output.Append(td.input)
			assert.Equal(t, td.outputRes, res, td.desc)
			assert.Equal(t, td.output, output, td.desc)
			assert.Equal(t, td.size, output.Size(), td.desc)
			assert.Equal(t, td.count, output.Count(), td.desc)
			assert.NotNil(t, output.AsBlock().DoubleData)
		})
	}
}

func TestAppend_Int32(t *testing.T) {
	tests := []struct {
		desc      string
		input     interface{}
		output    *PrestoThriftInteger
		outputRes int
		size      int
		count     int
	}{
		{
			desc:  "int32 appended",
			input: int32(321),
			output: &PrestoThriftInteger{
				Nulls: []bool{false, false},
				Ints:  []int32{123, 321},
			},
			outputRes: 6,
			size:      12,
			count:     2,
		},
		{
			desc:  "null value appended",
			input: nil,
			output: &PrestoThriftInteger{
				Nulls: []bool{false, true},
				Ints:  []int32{123, 0},
			},
			outputRes: 6,
			size:      12,
			count:     2,
		},
		{
			desc: "block appended",
			input: PrestoThriftInteger{
				Nulls: []bool{false},
				Ints:  []int32{456},
			},
			output: &PrestoThriftInteger{
				Nulls: []bool{false, false},
				Ints:  []int32{123, 456},
			},
			size:  12,
			count: 2,
		},
	}

	for _, td := range tests {
		output := &PrestoThriftInteger{
			Nulls: []bool{false},
			Ints:  []int32{123},
		}

		if array, ok := td.input.(PrestoThriftInteger); ok {
			t.Run(td.desc, func(*testing.T) {
				output.AppendBlock(*array.AsBlock())
				assert.Equal(t, td.count, output.Count(), td.desc)
			})
			continue
		}

		// Append a single value
		t.Run(td.desc, func(*testing.T) {
			res := output.Append(td.input)
			assert.Equal(t, td.outputRes, res, td.desc)
			assert.Equal(t, td.output, output, td.desc)
			assert.Equal(t, td.size, output.Size(), td.desc)
			assert.Equal(t, td.count, output.Count(), td.desc)
			assert.NotNil(t, output.AsBlock().IntegerData)
		})
	}
}

func Test_toTime(t *testing.T) {
	tests := []struct {
		input  int64
		output time.Time
	}{
		{
			input:  1514764800,
			output: time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:  1514764800000,
			output: time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:  1514764800000000,
			output: time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:  1514764800000000000,
			output: time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		o := toTime(tc.input)
		assert.Equal(t, tc.output, o.UTC())
	}
}

func TestNewColumn(t *testing.T) {
	tests := []struct {
		input  interface{}
		output interface{}
		failed bool
	}{
		{
			input:  "hi",
			output: new(PrestoThriftVarchar),
		},
		{
			input:  int64(1),
			output: new(PrestoThriftBigint),
		},
		{
			input:  float64(1),
			output: new(PrestoThriftDouble),
		},
		{
			input:  complex64(1),
			output: nil,
			failed: true,
		},
	}

	for _, tc := range tests {
		c, ok := NewColumn(reflect.TypeOf(tc.input))
		assert.Equal(t, tc.failed, !ok)
		assert.Equal(t, tc.output, c)

		if c != nil {
			assert.Equal(t, 0, c.AsBlock().Size())
			assert.Equal(t, 0, c.Size())
		}
	}
}

func TestTimeRange_zero(t *testing.T) {
	v := PrestoThriftRange{}

	t0, t1, ok := v.AsTimeRange()
	assert.False(t, ok)
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), t0.UTC())
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), t1.UTC())
}

func TestTimeRange_bounds(t *testing.T) {
	v := PrestoThriftRange{
		Low: &PrestoThriftMarker{
			Value: &PrestoThriftBlock{
				BigintData: &PrestoThriftBigint{
					Nulls: []bool{false},
					Longs: []int64{1514764800},
				},
			},
			Bound: PrestoThriftBoundExactly,
		},
		High: &PrestoThriftMarker{
			Value: &PrestoThriftBlock{
				BigintData: &PrestoThriftBigint{
					Nulls: []bool{false},
					Longs: []int64{1514764800},
				},
			},
			Bound: PrestoThriftBoundExactly,
		},
	}

	t0, t1, ok := v.AsTimeRange()
	assert.True(t, ok)
	assert.Equal(t, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), t0.UTC())
	assert.Equal(t, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), t1.UTC())
}

func TestTimeRange_above(t *testing.T) {
	v := PrestoThriftRange{
		Low: &PrestoThriftMarker{
			Value: &PrestoThriftBlock{
				BigintData: &PrestoThriftBigint{
					Nulls: []bool{false},
					Longs: []int64{1514764800},
				},
			},
			Bound: PrestoThriftBoundAbove,
		},
	}

	t0, t1, ok := v.AsTimeRange()
	assert.True(t, ok)
	assert.Equal(t, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), t0.UTC())
	assert.Equal(t, time.Unix(math.MaxInt64, 0).UTC(), t1.UTC())
}

func TestTimeRange_below(t *testing.T) {
	v := PrestoThriftRange{
		Low: &PrestoThriftMarker{
			Value: &PrestoThriftBlock{
				BigintData: &PrestoThriftBigint{
					Nulls: []bool{false},
					Longs: []int64{1514764800},
				},
			},
			Bound: PrestoThriftBoundBelow,
		},
	}

	t0, t1, ok := v.AsTimeRange()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(0, 0).UTC(), t0.UTC())
	assert.Equal(t, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), t1.UTC())
}
