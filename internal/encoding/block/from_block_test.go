// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package block

import (
	"io/ioutil"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func setup() (Block, typeof.Schema) {

	const testFile = "../../../test/test4.csv"
	o, _ := ioutil.ReadFile(testFile)
	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := Transform(schema)
	b, _ := FromCSVBy(o, "raisedCurrency", &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}, apply)
	if len(b) > 0 {
		return b[0], *schema
	}
	return Block{}, *schema
}

func TestFromBlock(t *testing.T) {
	blk, schema := setup()
	rows, err := FromBlockBy(blk, schema)
	assert.NoError(t, err)
	cols, err := blk.Select(schema)
	assert.NoError(t, err)
	rowCount := cols.Any().Count()
	// verify row count.
	assert.Equal(t, rowCount, len(rows))
	for _, row := range rows {
		// verify values
		assert.Contains(t, []string{"EUR", "CAD", "USD"}, row.Values["raisedCurrency"])
		// verify type
		assert.Equal(t, typeof.String, row.Schema["raisedCurrency"])
	}
}
