// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestTransform(t *testing.T) {
	dataColumn, err := newDataColumn()
	assert.NoError(t, err)

	// The original schema
	original := typeof.Schema{
		"a": typeof.String,
		"b": typeof.Timestamp,
		"c": typeof.Int32,
	}

	// The schema to filter
	filter := typeof.Schema{
		"b":    typeof.Timestamp,
		"c":    typeof.String, // Different type
		"data": typeof.JSON,   // Computed
	}

	// Create a new row
	in := newRow(original, 3)
	in.Set("a", "hello")
	in.Set("b", time.Unix(0, 0))
	in.Set("c", 123)

	// Run a transformation
	out := in.Transform([]column.Computed{dataColumn}, &filter)
	assert.NotNil(t, out)

	// Make sure input is not changed
	assert.Equal(t, 3, len(in.columns))
	assert.Equal(t, `[{"column":"a","type":"VARCHAR"},{"column":"b","type":"TIMESTAMP"},{"column":"c","type":"INTEGER"}]`, in.schema.String())

	// Assert the output
	assert.Equal(t, 2, len(out.columns))
	assert.Equal(t, `[{"column":"b","type":"TIMESTAMP"},{"column":"data","type":"JSON"}]`, out.schema.String())
}

func TestTransform_NoFilter(t *testing.T) {
	dataColumn, err := newDataColumn()
	assert.NoError(t, err)

	// The original schema
	original := typeof.Schema{
		"a": typeof.String,
		"b": typeof.Timestamp,
		"c": typeof.Int32,
	}

	// Create a new row
	in := newRow(original, 3)
	in.Set("a", "hello")
	in.Set("b", time.Unix(0, 0))
	in.Set("c", 123)

	// Run a transformation
	out := in.Transform([]column.Computed{dataColumn}, nil)
	assert.NotNil(t, out)

	// Make sure input is not changed
	assert.Equal(t, 3, len(in.columns))
	assert.Equal(t, `[{"column":"a","type":"VARCHAR"},{"column":"b","type":"TIMESTAMP"},{"column":"c","type":"INTEGER"}]`, in.schema.String())

	// Assert the output
	assert.Equal(t, 4, len(out.columns))
	assert.Equal(t, `[{"column":"a","type":"VARCHAR"},{"column":"b","type":"TIMESTAMP"},{"column":"c","type":"INTEGER"},{"column":"data","type":"JSON"}]`, out.schema.String())
}
