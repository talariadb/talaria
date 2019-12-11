// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestServeCancellation(t *testing.T) {
	assert.NotPanics(t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = Serve(ctx, 9999, nil)
	})
}

func TestNamedColumns(t *testing.T) {
	nc := make(NamedColumns, 2)

	// Fill level 1
	assert.True(t, nc.Append("a", int32(1)))
	assert.True(t, nc.Append("b", int32(2)))
	assert.False(t, nc.Append("x", complex128(1)))
	assert.Equal(t, 1, nc.Max())
	nc.FillNulls()

	// Fill level 2
	assert.True(t, nc.Append("a", int32(1)))
	assert.True(t, nc.Append("c", "hi"))
	assert.Equal(t, 2, nc.Max())
	nc.FillNulls()

	// Fill level 3
	assert.True(t, nc.Append("b", int32(1)))
	assert.True(t, nc.Append("c", "hi"))
	assert.True(t, nc.Append("d", float64(1.5)))
	assert.Equal(t, 3, nc.Max())
	nc.FillNulls()

	// Must have 3 levels with nulls in the middle
	assert.Equal(t, []int32{1, 1, 0}, nc["a"].AsBlock().IntegerData.Ints)
	assert.Equal(t, []bool{false, false, true}, nc["a"].AsBlock().IntegerData.Nulls)
	assert.Equal(t, []int32{2, 0, 1}, nc["b"].AsBlock().IntegerData.Ints)
	assert.Equal(t, []bool{false, true, false}, nc["b"].AsBlock().IntegerData.Nulls)
	assert.Equal(t, []byte{0x68, 0x69, 0x68, 0x69}, nc["c"].AsBlock().VarcharData.Bytes)
	assert.Equal(t, []int32{0, 2, 2}, nc["c"].AsBlock().VarcharData.Sizes)
	assert.Equal(t, []bool{true, false, false}, nc["c"].AsBlock().VarcharData.Nulls)
	assert.Equal(t, []float64{0, 0, 1.5}, nc["d"].AsBlock().DoubleData.Doubles)
	assert.Equal(t, []bool{true, true, false}, nc["d"].AsBlock().DoubleData.Nulls)
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
		o := toTime(tc.input, true)
		assert.Equal(t, tc.output, o.UTC())
	}
}

func TestNewColumn(t *testing.T) {
	tests := []struct {
		input  interface{}
		output interface{}
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
			input:  true,
			output: new(PrestoThriftBoolean),
		},
		{
			input:  time.Unix(1, 0),
			output: new(PrestoThriftTimestamp),
		},
		{
			input:  json.RawMessage(nil),
			output: new(PrestoThriftJson),
		},
	}

	for _, tc := range tests {
		rt, ok := typeof.FromType(reflect.TypeOf(tc.input))
		assert.True(t, ok)

		c := NewColumn(rt)
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
