// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package merge

import (
	"encoding/json"
	"fmt"
	"strconv"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/presto"
)

// ToParquet merges multiple blocks together and outputs a key and merged Parquet data
func ToParquet(blocks []block.Block, schema typeof.Schema) ([]byte, error) {
	parquetSchema, fieldHandlers, err := deriveSchema(schema)

	if err != nil {
		return nil, errors.Internal("merge: error generating parquet schema", err)
	}

	// Acquire a buffer to be used during the merging process
	buffer := acquire()
	defer release(buffer)

	writer := goparquet.NewFileWriter(buffer,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema),
		goparquet.WithCreator("write-lowlevel"),
	)

	for _, blk := range blocks {
		rows, err := blk.Select(blk.Schema())
		if err != nil {
			continue
		}

		// Fetch columns that is required by the static schema
		cols := make(column.Columns, 16)
		for name, typ := range schema {
			col, ok := rows[name]
			if !ok || (col.Kind() != typ && !isTypeCompatible(col, typ)) {
				col = column.NewColumn(typ)
			}

			cols[name] = col
		}

		cols.FillNulls()

		allCols := []column.Column{}
		for _, colName := range schema.Columns() {
			allCols = append(allCols, cols[colName])
		}

		for i := 0; i < allCols[0].Count(); i++ {
			data := make(map[string]interface{})

			for j, colName := range schema.Columns() {
				localCol := allCols[j]
				fieldHandler := fieldHandlers[j]
				finalData := localCol.At(i)

				if finalData != nil && fieldHandler != nil {
					finalData, _ = fieldHandler(localCol.At(i))
				}

				data[colName] = finalData
			}

			if err := writer.AddData(data); err != nil {
				return nil, errors.Internal("flush: error writing row", err)
				// TODO: should we ignore or continue?
			}
		}
	}

	if err := writer.Close(); err != nil {
		return nil, errors.Internal("flush: error closing writer", err)
	}

	// Always return a cloned buffer since we're reusing the working one
	return clone(buffer), nil
}

type fieldHandler func(interface{}) (interface{}, error)

func deriveSchema(inputSchema typeof.Schema) (schema *parquetschema.SchemaDefinition, fieldHandlers []fieldHandler, err error) {
	schema = &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name: "msg",
			},
		},
	}

	fieldHandlers = make([]fieldHandler, 0, len(inputSchema.Columns()))

	for _, field := range inputSchema.Columns() {
		typ := inputSchema[field]

		col, fieldHandler, err := createColumn(field, typ.String())
		if err != nil {
			return nil, nil, fmt.Errorf("toparquet: couldn't create column for field %s: %v", field, err)
		}

		fieldHandlers = append(fieldHandlers, fieldHandler)
		schema.RootColumn.Children = append(schema.RootColumn.Children, col)
	}

	if err := schema.Validate(); err != nil {
		return schema, nil, fmt.Errorf("toparquet: validation of generated schema failed: %w", err)
	}

	return schema, fieldHandlers, nil
}

func createColumn(field, typ string) (col *parquetschema.ColumnDefinition, fieldHandler func(interface{}) (interface{}, error), err error) {
	col = &parquetschema.ColumnDefinition{
		SchemaElement: &parquet.SchemaElement{},
	}
	col.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	col.SchemaElement.Name = field

	switch typ {
	case "string":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.STRING = &parquet.StringType{}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
		return col, optional(byteArrayHandler), nil
	case "byte_array":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		return col, optional(byteArrayHandler), nil
	case "boolean":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
		return col, optional(booleanHandler), nil
	case "int8":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 8, IsSigned: true}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)
		return col, optional(intHandler(8)), nil
	case "uint8":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 8, IsSigned: false}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
		return col, optional(uintHandler(8)), nil
	case "int16":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 16, IsSigned: true}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)
		return col, optional(intHandler(16)), nil
	case "uint16":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 16, IsSigned: false}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)
		return col, optional(uintHandler(16)), nil
	case "int32":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 32, IsSigned: true}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
		return col, optional(uintHandler(32)), nil
	case "uint32":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 32, IsSigned: false}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
		return col, optional(uintHandler(32)), nil
	case "int64":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: true}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
		return col, optional(intHandler(64)), nil
	case "uint64":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: false}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
		return col, optional(uintHandler(64)), nil
	case "float64":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_FLOAT)
		return col, optional(floatHandler), nil
	case "double":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_DOUBLE)
		return col, optional(doubleHandler), nil
	case "int":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: true}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
		return col, optional(intHandler(64)), nil
	case "json":
		col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.SchemaElement.LogicalType.JSON = &parquet.JsonType{}
		col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
		return col, optional(byteArrayHandler), nil
	default:
		return nil, nil, fmt.Errorf("toparquet: unsupported type %q", typ)
	}
}

func byteArrayHandler(s interface{}) (interface{}, error) {
	switch v := s.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case json.RawMessage:
		return []byte(v), nil
	default:
		return []byte(fmt.Sprintf("%v", s)), nil
	}
}

func booleanHandler(s interface{}) (interface{}, error) {
	switch v := s.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return nil, fmt.Errorf("toparquet: unable to parse as boolean %d", s)
	}
}

func uintHandler(bitSize int) func(interface{}) (interface{}, error) {
	return func(s interface{}) (interface{}, error) {
		localString := fmt.Sprintf("%v", s)
		i, err := strconv.ParseUint(localString, 10, bitSize)
		if err != nil {
			return nil, err
		}
		switch bitSize {
		case 8, 16, 32:
			return uint32(i), nil
		case 64:
			return i, nil
		default:
			return nil, fmt.Errorf("toparquet: invalid uint bit size %d", bitSize)
		}
	}
}

func intHandler(bitSize int) func(interface{}) (interface{}, error) {

	helperFunc := func(bitSize int, rawValue int64) (interface{}, error) {
		switch bitSize {
		case 8, 16, 32:
			return int32(rawValue), nil
		case 64:
			return rawValue, nil
		default:
			return nil, fmt.Errorf("toparquet: invalid integer bit size %d", bitSize)
		}
	}

	return func(s interface{}) (interface{}, error) {
		switch v := s.(type) {
		case int:
			return helperFunc(bitSize, int64(v))
		case int64:
			return helperFunc(bitSize, v)
		case int32:
			return helperFunc(bitSize, int64(v))
		case string:
			var intVal, err = strconv.ParseInt(v, 10, bitSize)
			if err != nil {
				return nil, err
			}

			return helperFunc(bitSize, intVal)
		default:
			return nil, fmt.Errorf("toparquet: invalid integer bit size %d", bitSize)
		}
	}
}

func floatHandler(s interface{}) (interface{}, error) {
	switch v := s.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	default:
		return nil, fmt.Errorf("toparquet: Error to parse as float32 %d", s)
	}
}

func doubleHandler(s interface{}) (interface{}, error) {
	switch v := s.(type) {
	case float64:
		return v, nil
	default:
		return nil, fmt.Errorf("toparquet: Error in parse as float64 %d", s)
	}
}

// Allows fieldHandlers to be chained
func optional(next fieldHandler) fieldHandler {
	return func(s interface{}) (interface{}, error) {
		if s == "" {
			return nil, nil
		}
		return next(s)
	}
}

func isTypeCompatible(c presto.Column, p typeof.Type) bool {
	if c.Kind() == typeof.String && p == typeof.JSON {
		// Intercompatible types
		return true
	}

	return false
}