// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"reflect"
	"strconv"
	"time"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// Row represents a single row on which we can perform transformations
type row struct {
	columns map[string]interface{}
	schema  typeof.Schema
}

// NewRow creates a new row with a schema and a capacity
func newRow(schema typeof.Schema, capacity int) row {
	if schema == nil {
		schema = make(typeof.Schema, capacity)
	}

	return row{
		columns: make(map[string]interface{}, capacity),
		schema:  schema,
	}
}

// Set sets the key/value pair
func (r row) Set(k string, v interface{}) {

	// If there's no schema defined, infer from the value itself
	typ, ok := r.schema[k]
	if !ok {
		typ, ok = typeof.FromType(reflect.TypeOf(v))
		if !ok {
			return // Skip
		}

		r.schema[k] = typ
	}

	// Set the value, we have a special-case handling for string types in case the value
	// is of different type and was encoded as a string.
	switch s := v.(type) {
	case string:
		if v, ok := tryParse(s, typ); ok {
			r.columns[k] = v
		}
	default:
		r.columns[k] = v
	}

}

// AppendTo appends the entire row to the column set
func (r row) AppendTo(cols column.Columns) (size int) {
	for k, v := range r.columns {
		size += cols.Append(k, v, r.schema[k])
	}
	return size
}

// Transform runs the computed columns and overwrites/appends them to the set.
func (r row) Transform(computed []*column.Computed, filter *typeof.Schema) row {

	// Create a new output row and copy the column values from the input
	schema := make(typeof.Schema, len(r.schema))
	out := newRow(schema, len(r.columns)+len(computed))
	for k, v := range r.columns {
		if filter == nil || filter.Contains(k, r.schema[k]) {
			out.columns[k] = v
			out.schema[k] = r.schema[k]
		}
	}

	// Compute the columns
	for _, c := range computed {
		if filter != nil && !filter.Contains(c.Name(), c.Type()) {
			continue // Skip computed columns which aren't part of the filter
		}

		// Compute the column
		v, err := c.Value(r.columns)
		if err != nil || v == nil {
			continue
		}

		// If the column with the same name is already present in the input row,
		// we need to overwrite this column and set a new type.
		out.schema[c.Name()] = c.Type()
		out.columns[c.Name()] = v
	}

	return out
}

// TryParse attempts to parse the string to a specific type
func tryParse(s string, typ typeof.Type) (interface{}, bool) {
	switch typ {

	// Happy Path, return the string
	case typeof.String, typeof.JSON:
		return s, true

	// Try and parse boolean value
	case typeof.Bool:
		if v, err := strconv.ParseBool(s); err == nil {
			return v, true
		}

	// Try and parse integer value
	case typeof.Int32:
		if v, err := strconv.ParseInt(s, 10, 32); err == nil {
			return int32(v), true
		}

	// Try and parse integer value
	case typeof.Int64:
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v, true
		}

	// Try and parse float value
	case typeof.Float64:
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v, true
		}

	case typeof.Timestamp:
		if v, err := time.Parse(time.RFC3339, s); err == nil {
			return v, true
		}
	}

	return nil, false
}
