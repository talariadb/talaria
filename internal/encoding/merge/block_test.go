// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package merge

import (
	"io/ioutil"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func setup() ([]block.Block, typeof.Schema) {

	const testFile = "../../../test/test4.csv"
	o, _ := ioutil.ReadFile(testFile)
	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := block.Transform(schema)
	b, _ := block.FromCSVBy(o, "raisedCurrency", &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}, apply)
	return b, *schema
}

func TestToBlock(t *testing.T) {
	blks, schema := setup()
	totalRowCount := 0
	for _, blk := range blks {
		cols, err := blk.Select(schema)
		assert.NoError(t, err)
		totalRowCount += cols.Any().Count()
	}
	mergedBytes, err := ToBlock(blks, schema)
	assert.NoError(t, err)
	mergedBlock, err := block.FromBuffer(mergedBytes)
	assert.NoError(t, err)
	mergedCols, err := mergedBlock.Select(schema)
	assert.NoError(t, err)
	assert.Equal(t, totalRowCount, mergedCols.Any().Count())
}
