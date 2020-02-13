// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"fmt"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/presto"
)

// Column contract represent a column that can be appended.
type Column = presto.Column

// ------------------------------------------------------------------------------------------------------------

// Columns represents a set of named columns
type Columns map[string]Column

// Append adds a value at a particular index to the block.
func (c Columns) Append(name string, value interface{}, typ typeof.Type) bool {
	if col, exists := c[name]; exists {
		return col.Append(value) > 0
	}

	// Skip unsupported types
	if typ == typeof.Unsupported {
		return false
	}

	// If column does not exist, create it and fill it with nulls up until the max - 1
	newColumn := NewColumn(typ)
	until := c.Max() - 1
	for i := 0; i < until; i++ {
		newColumn.Append(nil)
	}

	c[name] = newColumn
	return newColumn.Append(value) > 0
}

// Max finds the maximum count of a column in the set
func (c Columns) Max() (max int) {
	for _, column := range c {
		if count := column.Count(); count > max {
			max = count
		}
	}
	return
}

// LastRow returns the last row
func (c Columns) LastRow() map[string]interface{} {
	row := make(map[string]interface{}, len(c))
	for name, column := range c {
		row[name] = column.Last()
	}
	return row
}

// AppendComputed runs the computed columns and appends them to the set.
func (c Columns) AppendComputed(computed []*Computed) error {
	if len(computed) == 0 {
		return nil
	}

	row := c.LastRow()
	for _, s := range computed {
		v, err := s.Value(row)
		if err != nil {
			return err
		}
		c.Append(s.Name(), v, s.Type())
	}
	return nil
}

// FillNulls adds nulls onto all uneven columns left.
func (c Columns) FillNulls() {
	max := c.Max()
	for _, column := range c {
		delta := max - column.Count()
		for i := 0; i < delta; i++ {
			column.Append(nil)
		}
	}
}

// Size returns the space (in bytes) required for the set of blocks.
func (c Columns) Size() (size int) {
	for _, block := range c {
		size += block.Size()
	}
	return
}

// Any retrieves any column from the set.
func (c Columns) Any() Column {
	for _, block := range c {
		return block
	}
	return nil
}

// ------------------------------------------------------------------------------------------------------------

// NewColumn creates a new appendable column
func NewColumn(t typeof.Type) Column {
	switch t {
	case typeof.String:
		return new(presto.PrestoThriftVarchar)
	case typeof.Int32:
		return new(presto.PrestoThriftInteger)
	case typeof.Int64:
		return new(presto.PrestoThriftBigint)
	case typeof.Float64:
		return new(presto.PrestoThriftDouble)
	case typeof.Bool:
		return new(presto.PrestoThriftBoolean)
	case typeof.Timestamp:
		return new(presto.PrestoThriftTimestamp)
	case typeof.JSON:
		return new(presto.PrestoThriftJson)
	}

	panic(fmt.Errorf("presto: unknown type %v", t))
}
