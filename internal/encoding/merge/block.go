// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package merge

import (
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/presto"
)

// ToBlock merges multiple blocks together and outputs merged Block bytes
func ToBlock(blocks []block.Block, schema typeof.Schema) ([]byte, error) {
	// Acquire a buffer to be used during the merging process
	buffer := acquire()
	defer release(buffer)

	if len(blocks) == 0 {
		return nil, nil
	}
	key := string(blocks[0].Key)

	merged := column.MakeColumns(&schema)
	for _, blk := range blocks {
		cols, _ := blk.Select(schema)
		for name := range schema {
			col1 := merged[name]
			col2 := cols[name]
			col1.AppendBlock([]presto.Column{col2})
		}
	}

	mergedBlock, _ := block.FromColumns(key, merged)
	bytes, _ := mergedBlock.Encode()
	buffer.Write(bytes)
	return clone(buffer), nil
}
