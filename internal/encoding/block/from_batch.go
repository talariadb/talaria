// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/typeof"
	talaria "github.com/grab/talaria/proto"
)

// FromBatchBy creates a block from a talaria protobuf-encoded batch. It
// repartitions the batch by a given partition key at the same time.
func FromBatchBy(batch *talaria.Batch, partitionBy string, computed ...*column.Computed) ([]Block, error) {
	if batch == nil || batch.Strings == nil || batch.Events == nil {
		return nil, errEmptyBatch
	}

	// Find the partition index
	partitionKey, ok := findParitionKey(batch.Strings, partitionBy)
	if !ok {
		return nil, errPartitionNotFound
	}

	result := make(map[string]column.Columns, 16)
	for _, event := range batch.Events {
		if event.Value == nil {
			continue
		}

		// Get the partition value
		partition, err := readPartition(event, batch.Strings, partitionKey)
		if err != nil {
			return nil, err
		}

		// Get the block for that partition
		columns, exists := result[partition]
		if !exists {
			columns = make(column.Columns, 16)
			result[partition] = columns
		}

		// Write the events into the block
		row := make(map[string]interface{}, len(event.Value))
		for k, v := range event.Value {
			columnName := stringAt(batch.Strings, k)
			columnValue, err := readValue(batch.Strings, v)
			if err != nil {
				return nil, err
			}

			typ, ok := typeof.FromType(reflect.TypeOf(columnValue))
			if !ok {
				continue // Skip
			}

			row[columnName] = columnValue
			columns.Append(columnName, columnValue, typ)
		}

		// Append computed columns and fill nulls for the row
		columns.AppendComputed(row, computed)
		columns.FillNulls()
	}

	// Write the columns into the block
	return makeBlocks(result)
}

// ------------------------------------------------------------------------------------------

// Reads a partition value from an event, at a specific partition key
func readPartition(event *talaria.Event, dict map[uint32][]byte, partitionKey uint32) (string, error) {
	v, ok := event.Value[partitionKey]
	if !ok {
		return "", errPartitionNotFound
	}

	pv, err := readValue(dict, v)
	if err != nil {
		return "", err
	}

	if v, ok := pv.(string); ok {
		return v, nil
	}
	return "", errPartitionInvalid
}

// Reads a value
func readValue(dict map[uint32][]byte, v *talaria.Value) (interface{}, error) {
	switch row := v.GetValue().(type) {
	case *talaria.Value_Int32:
		return row.Int32, nil
	case *talaria.Value_Int64:
		return row.Int64, nil
	case *talaria.Value_Float64:
		return row.Float64, nil
	case *talaria.Value_String_:
		return stringAt(dict, row.String_), nil
	case *talaria.Value_Bool:
		return row.Bool, nil
	case *talaria.Value_Time:
		return time.Unix(row.Time, 0), nil
	case *talaria.Value_Json:
		return json.RawMessage(binaryAt(dict, row.Json)), nil
	case nil: // The field is not set.
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", row)
	}
}

// Reads a string from an interned map
func stringAt(dict map[uint32][]byte, index uint32) string {
	if b, ok := dict[index]; ok {
		return binaryToString(&b)
	}
	return ""
}

// Reads bytes from an interned map
func binaryAt(dict map[uint32][]byte, index uint32) []byte {
	if b, ok := dict[index]; ok {
		return b
	}
	return []byte{}
}

// Find the partition index
func findParitionKey(dict map[uint32][]byte, partitionBy string) (uint32, bool) {
	for k, v := range dict {
		if binaryToString(&v) == partitionBy {
			return k, true
		}
	}
	return 0, false
}

// makeBlocks creates a set of blocks from a set of named columns
func makeBlocks(v map[string]column.Columns) ([]Block, error) {
	blocks := make([]Block, 0, len(v))
	for k, columns := range v {
		block, err := FromColumns(k, columns)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// Converts binary to string in a zero-alloc manner
func binaryToString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}
