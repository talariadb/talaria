// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"sort"

	"github.com/grab/talaria/internal/encoding/orc"
	"github.com/grab/talaria/internal/presto"
)

// FromOrc ...
func FromOrc(b []byte) (block *Block, err error) {
	i, err := orc.FromBuffer(b)
	if err != nil {
		return nil, err
	}

	// Get the list of columns in the ORC file
	var columns []string
	schema := i.Schema()
	for k := range schema {
		columns = append(columns, k)
	}

	// Sort the columns for consistency
	sort.Strings(columns)

	// Create presto columns
	blocks := make(map[string]presto.Column, len(columns))
	index := make([]string, 0, len(columns))
	for _, c := range columns {
		if kind, hasType := schema[c]; hasType {
			v, ok := presto.NewColumn(kind)
			if !ok {
				return nil, errSchemaMismatch
			}

			blocks[c] = v
			index = append(index, c)
		}
	}

	// Create a block
	block = new(Block)
	i.Range(func(i int, row []interface{}) bool {
		for i, v := range row {
			blocks[index[i]].Append(v)
		}
		return false
	}, columns...)

	// Write the columns into the block
	if err := block.writeColumns(blocks); err != nil {
		return nil, err
	}
	return
}

// FromOrcBy decodes a set of blocks from an orc file and repartitions
// it by the specified partition key.
func FromOrcBy(payload []byte, partitionBy string) (map[string]Block, error) {
	const chunks = 25000

	result := make(map[string]Block, 16)
	_, err := orc.SplitByColumn(payload, partitionBy, func(hashValue string, columnChunk []byte) bool {
		_, splitErr := orc.SplitBySize(columnChunk, chunks, func(chunk []byte) bool {
			blk, err := FromOrc(chunk)
			if err != nil {
				return true
			}

			result[hashValue] = *blk
			return false
		})
		return splitErr != nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
