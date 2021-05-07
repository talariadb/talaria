package block

import (
	"fmt"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/parquet"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// FromParquetBy decodes a set of blocks from a Parquet file and repartitions
// it by the specified partition key.
func FromParquetBy(payload []byte, partitionBy string, filter *typeof.Schema, apply applyFunc) ([]Block, error) {
	const max = 10000000 // 10MB

	iter, err := parquet.FromBuffer(payload)
	if err != nil {
		return nil, err
	}

	// Find the partition index
	schema := iter.Schema()
	cols := schema.Columns()
	partitionIdx, ok := findString(cols, partitionBy)
	if !ok {
		return nil, nil // Skip the file if it has no partition column
	}

	// The resulting set of blocks, repartitioned and chunked
	blocks := make([]Block, 0, 128)

	// Create presto columns and iterate
	result, size := make(map[string]column.Columns, 16), 0
	_, _ = iter.Range(func(rowIdx int, r []interface{}) bool {
		if size >= max {
			pending, err := makeBlocks(result)
			if err != nil {
				return true
			}

			size = 0 // Reset the size
			blocks = append(blocks, pending...)
			result = make(map[string]column.Columns, 16)
		}

		// Get the partition value, must be a string
		partition, ok := convertToString(r[partitionIdx])
		if !ok {
			return true
		}

		// Skip the record if the partition is actually empty
		if partition == "" {
			return false
		}

		// Get the block for that partition
		columns, exists := result[partition]
		if !exists {
			columns = column.MakeColumns(filter)
			result[partition] = columns
		}

		// Prepare a row for transformation
		row := NewRow(schema, len(r))
		for i, v := range r {
			columnName := cols[i]
			columnType := schema[columnName]

			fieldHandler, _ := getReverseHandler(columnType.String())

			// Encode to JSON
			if columnType == typeof.JSON {
				if encoded, ok := convertToJSON(v); ok {
					v = encoded
				}
			}

			if fieldHandler != nil {
				v, _ = fieldHandler(v)
			}

			row.Set(columnName, v)
		}

		// Append computed columns and fill nulls for the row
		out, _ := apply(row)

		size += out.AppendTo(columns)
		size += columns.FillNulls()
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

type fieldHandler func(interface{}) (interface{}, error)

func getReverseHandler(typ string) (fieldHandler func(interface{}) (interface{}, error), err error) {

	switch typ {
	case "string":
		fieldHandler = stringHandler
	}

	return fieldHandler, nil
}

// Transform runs the computed Values and overwrites/appends them to the set.
func ApplyCastHandlers(r Row, fieldHandlers []fieldHandler) Row {
	// Create a new output row and copy the column values from the input
	schema := make(typeof.Schema, len(r.Schema))
	out := NewRow(schema, len(r.Values))

	i := 0
	for k, v := range r.Values {
		handler := fieldHandlers[i]

		if handler != nil {
			out.Values[k], _ = handler(v)
		}
		out.Schema[k] = r.Schema[k]
	}

	return out
}

func stringHandler(s interface{}) (interface{}, error) {

	switch s.(type) {
	case []byte:
		buf, ok := s.([]byte)
		if !ok {
			return nil, fmt.Errorf("Failed to get bytes from the interface")
		}

		return string(buf), nil
	}

	return nil, nil
}