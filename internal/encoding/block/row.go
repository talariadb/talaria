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

// applyFunc applies a transformation on a row and returns a new row
type applyFunc = func(Row) (Row, error)

// Row represents a single row on which we can perform transformations
type Row struct {
	Values map[string]interface{}
	Schema typeof.Schema
}

// NewRow creates a new row with a schema and a capacity
func NewRow(schema typeof.Schema, capacity int) Row {
	if schema == nil {
		schema = make(typeof.Schema, capacity)
	}

	return Row{
		Values: make(map[string]interface{}, capacity),
		Schema: schema,
	}
}

// Set sets the key/value pair
func (r Row) Set(k string, v interface{}) {

	// If there's no Schema defined, infer from the value itself
	typ, ok := r.Schema[k]
	if !ok {
		typ, ok = typeof.FromType(reflect.TypeOf(v))
		if !ok {
			return // Skip
		}

		r.Schema[k] = typ
	}

	// Set the value, we have a special-case handling for string types in case the value
	// is of different type and was encoded as a string.
	switch s := v.(type) {
	case string:
		if v, ok := tryParse(s, typ); ok {
			r.Values[k] = v
		}
	default:
		r.Values[k] = v
	}

}

// AppendTo appends the entire row to the column set
func (r Row) AppendTo(cols column.Columns) (size int) {
	for k, v := range r.Values {
		size += cols.Append(k, v, r.Schema[k])
	}
	return size
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
