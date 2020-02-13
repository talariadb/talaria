// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	nc := make(Columns, 2)
	data := []*Computed{newDataColumn(t)}
	assert.Nil(t, nc.Any())

	// Fill level 1
	assert.True(t, nc.Append("a", int32(1), typeof.Int32))
	assert.True(t, nc.Append("b", int32(2), typeof.Int32))
	assert.False(t, nc.Append("x", complex128(1), typeof.Unsupported))
	assert.Equal(t, 1, nc.Max())
	assert.Equal(t, 2, len(nc.LastRow()))
	nc.AppendComputed(data)
	nc.FillNulls()
	assert.NotNil(t, nc.Any())

	// Fill level 2
	assert.True(t, nc.Append("a", int32(1), typeof.Int32))
	assert.True(t, nc.Append("c", "hi", typeof.String))
	assert.Equal(t, 2, nc.Max())
	nc.AppendComputed(data)
	nc.FillNulls()

	// Fill level 3
	assert.True(t, nc.Append("b", int32(1), typeof.Int32))
	assert.True(t, nc.Append("c", "hi", typeof.String))
	assert.True(t, nc.Append("d", float64(1.5), typeof.Float64))
	assert.Equal(t, 3, nc.Max())
	nc.AppendComputed(data)
	nc.FillNulls()

	// Must have 3 levels with nulls in the middle
	assert.Equal(t, []int32{1, 1, 0}, nc["a"].AsThrift().IntegerData.Ints)
	assert.Equal(t, []bool{false, false, true}, nc["a"].AsThrift().IntegerData.Nulls)
	assert.Equal(t, []int32{2, 0, 1}, nc["b"].AsThrift().IntegerData.Ints)
	assert.Equal(t, []bool{false, true, false}, nc["b"].AsThrift().IntegerData.Nulls)
	assert.Equal(t, []byte{0x68, 0x69, 0x68, 0x69}, nc["c"].AsThrift().VarcharData.Bytes)
	assert.Equal(t, []int32{0, 2, 2}, nc["c"].AsThrift().VarcharData.Sizes)
	assert.Equal(t, []bool{true, false, false}, nc["c"].AsThrift().VarcharData.Nulls)
	assert.Equal(t, []float64{0, 0, 1.5}, nc["d"].AsThrift().DoubleData.Doubles)
	assert.Equal(t, []bool{true, true, false}, nc["d"].AsThrift().DoubleData.Nulls)
	assert.Equal(t, 5, len(nc.LastRow()))

}

func TestNewColumn(t *testing.T) {
	tests := []struct {
		input  interface{}
		output interface{}
	}{
		{
			input:  "hi",
			output: new(presto.PrestoThriftVarchar),
		},
		{
			input:  int64(1),
			output: new(presto.PrestoThriftBigint),
		},
		{
			input:  float64(1),
			output: new(presto.PrestoThriftDouble),
		},
		{
			input:  true,
			output: new(presto.PrestoThriftBoolean),
		},
		{
			input:  time.Unix(1, 0),
			output: new(presto.PrestoThriftTimestamp),
		},
		{
			input:  json.RawMessage(nil),
			output: new(presto.PrestoThriftJson),
		},
	}

	for _, tc := range tests {
		rt, ok := typeof.FromType(reflect.TypeOf(tc.input))
		assert.True(t, ok)

		c := NewColumn(rt)
		assert.Equal(t, tc.output, c)

		if c != nil {
			assert.Equal(t, 0, c.AsThrift().Size())
			assert.Equal(t, 0, c.Size())
		}
	}
}
