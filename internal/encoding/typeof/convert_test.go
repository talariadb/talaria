package typeof

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTryParse(t *testing.T) {
	tests := []struct {
		input   string
		typ     Type
		expect  interface{}
		success bool
	}{
		{
			input:   "1234",
			typ:     Int64,
			expect:  int64(1234),
			success: true,
		},
		{
			input:   "1234",
			typ:     Int32,
			expect:  int32(1234),
			success: true,
		},
		{
			input: "1234XX",
			typ:   Int32,
		},
		{
			input:   "1234.00",
			typ:     Float64,
			expect:  float64(1234),
			success: true,
		},
		{
			input:   "1985-04-12T23:20:50.00Z",
			typ:     Timestamp,
			expect:  time.Unix(482196050, 0).UTC(),
			success: true,
		},
	}

	for _, tc := range tests {
		v, ok := Parse(tc.input, tc.typ)
		assert.Equal(t, tc.expect, v)
		assert.Equal(t, tc.success, ok)
	}
}
