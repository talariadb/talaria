// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestTryParse(t *testing.T) {
	tests := []struct {
		input   string
		typ     typeof.Type
		expect  interface{}
		success bool
	}{
		{
			input:   "1234",
			typ:     typeof.Int64,
			expect:  int64(1234),
			success: true,
		},
		{
			input:   "1234",
			typ:     typeof.Int32,
			expect:  int32(1234),
			success: true,
		},
		{
			input: "1234XX",
			typ:   typeof.Int32,
		},
		{
			input:   "1234.00",
			typ:     typeof.Float64,
			expect:  float64(1234),
			success: true,
		},
		{
			input:   "1985-04-12T23:20:50.00Z",
			typ:     typeof.Timestamp,
			expect:  time.Unix(482196050, 0).UTC(),
			success: true,
		},
	}

	for _, tc := range tests {
		v, ok := tryParse(tc.input, tc.typ)
		assert.Equal(t, tc.expect, v)
		assert.Equal(t, tc.success, ok)
	}
}
