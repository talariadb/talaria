// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"github.com/grab/talaria/internal/column"
	"github.com/kelindar/binary"
	"github.com/kelindar/binary/nocopy"
)

// FromBuffer unmarshals a block from a in-memory buffer.
func FromBuffer(b []byte) (block Block, err error) {
	if err = binary.Unmarshal(b, &block); err != nil {
		return
	}
	return
}

// FromColumns creates a block from a set of presto named columns
func FromColumns(key string, columns column.Columns) (blk Block, err error) {
	blk = Block{Key: nocopy.String(key)}
	err = blk.writeColumns(columns)
	return
}
