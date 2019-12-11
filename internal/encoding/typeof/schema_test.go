// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
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
