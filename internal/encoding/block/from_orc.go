// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"encoding/json"
	"fmt"
	"strconv"

	orctype "github.com/crphang/orc"
	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/orc"
	"github.com/grab/talaria/internal/encoding/typeof"
)

// FromOrcBy decodes a set of blocks from an orc file and repartitions
// it by the specified partition key.
func FromOrcBy(payload []byte, partitionBy string, computed ...*column.Computed) ([]Block, error) {
	const max = 10000
	iter, err := orc.FromBuffer(payload)
	if err != nil {
		return nil, err
	}

	// Find the partition index
	schema := iter.Schema()
	cols := schema.Columns()
	partitionIdx, ok := findString(cols, partitionBy)
	if !ok {
		return nil, errPartitionNotFound
	}

	// The resulting set of blocks, repartitioned and chunked
	blocks := make([]Block, 0, 128)

	// Create presto columns and iterate
	result, count := make(map[string]column.Columns, 16), 0
	_, _ = iter.Range(func(rowIdx int, r []interface{}) bool {
		if count%max == 0 {
			pending, err := makeBlocks(result)
			if err != nil {
				return true
			}

			blocks = append(blocks, pending...)
			result = make(map[string]column.Columns, 16)
		}

		// Get the partition value, must be a string
		partition, ok := convertToString(r[partitionIdx])
		if !ok {
			return true
		}

		// Get the block for that partition
		columns, exists := result[partition]
		if !exists {
			columns = make(column.Columns, 16)
			result[partition] = columns
		}

		// Write the events into the block
		row := make(map[string]interface{}, len(r))
		for i, v := range r {
			columnName := cols[i]
			columnType := schema[columnName]

			// Encode to JSON
			if columnType == typeof.JSON {
				if encoded, ok := convertToJSON(v); ok {
					v = encoded
				}
			}

			row[columnName] = v
			columns.Append(columnName, v, columnType)
		}

		// Append computed columns and fill nulls for the row
		count++
		columns.AppendComputed(row, computed)
		columns.FillNulls()
		return false
	}, cols...)

	// Write the last chunk
	last, err := makeBlocks(result)
	if err != nil {
		return nil, err
	}

	blocks = append(blocks, last...)
	return blocks, nil
}

// Find the partition index
func findString(columns []string, partitionBy string) (int, bool) {
	for i, k := range columns {
		if k == partitionBy {
			return i, true
		}
	}
	return 0, false
}

// convertToJSON converts an ORC map/list/struct to JSON
func convertToJSON(value interface{}) (json.RawMessage, bool) {
	switch vt := value.(type) {
	case []orctype.MapEntry:
		remap := make(map[string]interface{}, len(vt))
		for _, v := range vt {
			remap[fmt.Sprintf("%v", v.Key)] = v.Value
		}
		value = remap
	case orctype.Struct:
	case []interface{}:
	case interface{}:
	default:
		return nil, false
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}

	return json.RawMessage(b), true
}

// convertToString converst value to string because currently all the keys in Badger are stored in the form of string before hashing to the byte array
func convertToString(value interface{}) (string, bool) {
	v, ok := value.(string)
	if ok {
		return v, true
	}
	valueInt, ok := value.(int64)
	if ok {
		return strconv.FormatInt(valueInt, 10), true
	}
	return "", false
}
