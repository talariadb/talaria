// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// Transform runs the computed Values and overwrites/appends them to the set.
func Transform(filter *typeof.Schema, computed ...column.Computed) applyFunc {
	return func(r Row) (Row, error) {
		// Create a new output row and copy the column values from the input
		schema := make(typeof.Schema, len(r.Schema))
		out := NewRow(schema, len(r.Values)+len(computed))
		for k, v := range r.Values {
			if filter == nil || filter.HasConvertible(k, r.Schema[k]) {
				out.Values[k] = v
				out.Schema[k] = r.Schema[k]
			}
		}

		// Compute the Values
		for _, c := range computed {
			if filter != nil && !filter.Contains(c.Name(), c.Type()) {
				continue // Skip computed Values which aren't part of the filter
			}

			// Compute the column
			v, err := c.Value(r.Values)
			if err != nil || v == nil {
				continue
			}

			// If the column with the same name is already present in the input row,
			// we need to overwrite this column and set a new type.
			out.Schema[c.Name()] = c.Type()
			out.Values[c.Name()] = v
		}

		return out, nil
	}
}
