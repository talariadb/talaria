// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package merge

import (
	"compress/flate"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// ToOrc merges multiple blocks together and outputs a key and merged orc data
func ToOrc(input interface{}) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	if _, ok := input.([]block.Block); !ok {
		return nil, errors.Internal("ORC merge not supported. input must be []block.Block", nil)
	}
	blocks := input.([]block.Block)
	schema := blocks[0].Schema()
	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		return nil, errors.Internal("merge: error generating orc schema", err)
	}

	// Acquire a buffer to be used during the merging process
	buffer := acquire()
	defer release(buffer)

	// Create a new writer
	writer, err := eorc.NewWriter(buffer,
		eorc.SetSchema(orcSchema),
		eorc.SetCompression(eorc.CompressionZlib{Level: flate.DefaultCompression}))
	if err != nil {
		return nil, err
	}

	for _, blk := range blocks {
		rows, err := blk.Select(blk.Schema())
		if err != nil {
			continue
		}

		// Fetch columns that is required by the static schema
		cols := make(column.Columns, 16)
		for name, typ := range schema {
			col, ok := rows[name]
			if !ok || col.Kind() != typ {
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
			row := []interface{}{}
			for j := 0; j < len(allCols); j++ {
				row = append(row, allCols[j].At(i))
			}
			if err := writer.Write(row...); err != nil {
				//return nil, errors.Internal("flush: error writing row", err)
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
