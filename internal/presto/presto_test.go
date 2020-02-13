// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServeCancellation(t *testing.T) {
	assert.NotPanics(t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = Serve(ctx, 9999, nil)
	})
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
