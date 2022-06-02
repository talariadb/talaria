// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"github.com/kelindar/talaria/internal/column/computed"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// Strip runs the computed Values and overwrites/appends them to the set.
func Strip(filter *typeof.Schema, computed ...computed.Computed) applyFunc {
	return func(r Row) (Row, error) {
		for _, c := range computed {
			v, err := c.Value(r.Values)
			if err != nil || v == nil {
				continue
			}
			if v.(bool) == true {
				if r.Values["bch"] == "test" {
					out := NewRow(nil, 0)
					return out, nil
				}
			}
		}

		return r, nil
		// // Create a new output row and copy the column values from the input
		// schema := make(typeof.Schema, len(r.Schema))
		// out := NewRow(schema, len(r.Values)+len(computed))
		// for k, v := range r.Values {
		// 	if filter == nil || filter.HasConvertible(k, r.Schema[k]) {
		// 		out.Values[k] = v
		// 		out.Schema[k] = r.Schema[k]
		// 	}
		// }

		// // Compute the Values
		// for _, c := range computed {
		// 	if filter != nil && !filter.Contains(c.Name(), c.Type()) {
		// 		continue // Skip computed Values which aren't part of the filter
		// 	}

		// 	// Compute the column
		// 	// v, err := c.Value(r.Values)
		// 	// if err != nil || v == nil {
		// 	// 	continue
		// 	// }

		// 	// If the column with the same name is already present in the input row,
		// 	// we need to overwrite this column and set a new type.
		// 	// out.Schema[c.Name()] = c.Type()
		// 	delete(out.Schema, "")
		// 	delete(out.Values, "")
		// }
		// return out, nil
	}
}
