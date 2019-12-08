// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/grab/talaria/internal/presto"
	talaria "github.com/grab/talaria/proto"
	"github.com/kelindar/binary/nocopy"
)

// FromBatchBy creates a block from a talaria protobuf-encoded batch. It
// repartitions the batch by a given partition key at the same time.
func FromBatchBy(batch *talaria.Batch, partitionBy string) ([]Block, error) {
	if batch == nil || batch.Strings == nil || batch.Events == nil {
		return nil, errEmptyBatch
	}

	// Find the partition index
	partitionKey, ok := findParitionKey(batch.Strings, partitionBy)
	if !ok {
		return nil, errPartitionNotFound
	}

	result := make(map[string]presto.NamedColumns, 16)
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
			columns = make(presto.NamedColumns, 16)
			result[partition] = columns
		}

		// Write the events into the block
		for k, v := range event.Value {
			columnValue, err := readValue(batch.Strings, v)
			if err != nil {
				return nil, err
			}
			columns.Append(stringAt(batch.Strings, k), columnValue)
		}

		columns.FillNulls()
	}

	// Write the columns into the block
	blocks := make([]Block, 0, len(result))
	for k, columns := range result {
		block := Block{Key: nocopy.String(k)}
		if err := block.writeColumns(columns); err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
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

// Converts binary to string in a zero-alloc manner
func binaryToString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}
