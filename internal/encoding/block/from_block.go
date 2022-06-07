// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// FromBlockBy creates and returns a list of new block.Row for a block.
func FromBlockBy(blk Block, schema typeof.Schema) ([]Row, error) {
	cols, err := blk.Select(schema)
	if err != nil {
		return nil, err
	}
	rowCount := cols.Any().Count()
	rows := make([]Row, rowCount)
	for i := 0; i < rowCount; i++ {
		row := NewRow(schema, len(schema))
		for name := range schema {
			col := cols[name]
			val := col.At(i)
			if val == nil {
				continue
			}
			row.Set(name, val)
		}
		rows[i] = row
	}
	return rows, nil
}
