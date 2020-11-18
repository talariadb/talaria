// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package block

import (
	"bytes"
	"encoding/csv"
	"io"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// FromCSVBy creates a block from a comma-separated file. It repartitions the batch by a given partition key at the same time.
func FromCSVBy(input []byte, partitionBy string, filter *typeof.Schema, apply applyFunc) ([]Block, error) {
	const max = 10000000 // 10MB

	rdr := csv.NewReader(bytes.NewReader(input))

	// Read the header first
	r, err := rdr.Read()
	header := r

	// Find the partition index
	partitionIdx, ok := findString(header, partitionBy)
	if !ok {
		return nil, nil // Skip the file if it has no partition column
	}

	// The resulting set of blocks, repartitioned and chunked
	blocks := make([]Block, 0, 128)

	// Create presto columns and iterate
	result, size := make(map[string]column.Columns, 16), 0
	for {
		r, err = rdr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if size >= max {
			pending, err := makeBlocks(result)
			if err != nil {
				return nil, err
			}

			size = 0 // Reset the size
			blocks = append(blocks, pending...)
			result = make(map[string]column.Columns, 16)
		}

		// Get the partition value, must be a string
		partition, ok := convertToString(r[partitionIdx])
		if !ok {
			return nil, errPartitionNotFound
		}

		// Skip the record if the partition is actually empty
		if partition == "" {
			continue
		}

		// Get the block for that partition
		columns, exists := result[partition]
		if !exists {
			columns = column.MakeColumns(filter)
			result[partition] = columns
		}

		// Prepare a row for transformation
		row := NewRow(filter.Clone(), len(r))
		for i, v := range r {
			row.Set(header[i], v)
		}

		// Append computed columns and fill nulls for the row
		out, _ := apply(row)
		size += out.AppendTo(columns)
		size += columns.FillNulls()
	}

	// Write the last chunk
	last, err := makeBlocks(result)
	if err != nil {
		return nil, err
	}

	blocks = append(blocks, last...)
	return blocks, nil
}
