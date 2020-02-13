// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"testing"
	"time"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestBlock_FromColumns(t *testing.T) {
	columns := make(column.Columns, 4)
	columns.Append("time", time.Unix(0, 0), typeof.Timestamp)
	columns.Append("address", "10.0.0.1", typeof.String)
	columns.Append("level", "warn", typeof.String)
	columns.Append("message", "hello", typeof.String)

	// Create a block from a set of columns
	block, err := FromColumns("A", columns)
	assert.NoError(t, err)
	assert.Equal(t, `[{"column":"address","type":"VARCHAR"},{"column":"level","type":"VARCHAR"},{"column":"message","type":"VARCHAR"},{"column":"time","type":"TIMESTAMP"}]`,
		block.Schema().String())

	// Encode
	encoded, err := block.Encode()
	assert.NoError(t, err)

	// Decode, must match
	decoded, err := FromBuffer(encoded)
	assert.NoError(t, err)
	assert.Equal(t, block.Size, decoded.Size)
}
