// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaString(t *testing.T) {
	s := Schema{
		"b": String,
		"a": Int32,
		"c": JSON,
	}

	assert.Equal(t, 3, len(s.Columns()))
	assert.Equal(t, `[{"column":"a","type":"INTEGER"},{"column":"b","type":"VARCHAR"},{"column":"c","type":"JSON"}]`, s.String())
}

func TestSchemaCompare(t *testing.T) {
	s1 := Schema{
		"b": String,
		"a": Int32,
	}

	s2 := Schema{
		"b": String,
		"a": Int64,
		"c": JSON,
	}

	delta, ok := s1.Compare(s2)
	assert.False(t, ok)
	assert.NotNil(t, delta)
	assert.Equal(t, `[{"column":"a","type":"BIGINT"},{"column":"c","type":"JSON"}]`, delta.String())

	subset := s1.Except(delta)
	assert.NotNil(t, subset)
	assert.Equal(t, `[{"column":"b","type":"VARCHAR"}]`, subset.String())
}

func TestSchemaUnion(t *testing.T) {
	s1 := Schema{
		"b": String,
		"a": Int32,
	}

	s2 := Schema{
		"a": Int32,
		"c": JSON,
	}

	merged, clean := s1.Union(s2)
	assert.True(t, clean)
	assert.Equal(t, `[{"column":"a","type":"INTEGER"},{"column":"b","type":"VARCHAR"},{"column":"c","type":"JSON"}]`, merged.String())
}

func TestSchemaUnion_Mismatch(t *testing.T) {
	s1 := Schema{
		"b": String,
		"a": Int32,
	}

	s2 := Schema{
		"b": String,
		"a": Int64,
		"c": JSON,
	}

	merged, clean := s1.Union(s2)
	assert.False(t, clean)
	assert.Equal(t, `[{"column":"a","type":"INTEGER"},{"column":"b","type":"VARCHAR"}]`, merged.String())
}
