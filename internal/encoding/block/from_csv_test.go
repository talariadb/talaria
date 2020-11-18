// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package block

import (
	"io/ioutil"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestFromCSV(t *testing.T) {
	const testFile = "../../../test/test4.csv"

	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := Transform(schema)

	b, err := FromCSVBy(o, "raisedCurrency", &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}, apply)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(b))

	for _, v := range b {
		assert.Contains(t, []string{"EUR", "CAD", "USD"}, string(v.Key))
	}

	v, err := b[0].Select(typeof.Schema{"raisedAmt": typeof.String})
	assert.NoError(t, err)
	assert.True(t, v["raisedAmt"].Size() > 0)
	assert.Equal(t, typeof.Float64, v["raisedAmt"].Kind())
}

func TestFromCSV_EmptyPartition(t *testing.T) {
	const testFile = "../../../test/test4.csv"

	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := Transform(schema)

	b, err := FromCSVBy(o, "numEmps", &typeof.Schema{
		"numEmps": typeof.String,
		"company": typeof.String,
	}, apply)
	assert.NoError(t, err)
	assert.Equal(t, 72, len(b))

	for _, v := range b {
		assert.NotEqual(t, "", string(v.Key))
		col, err := v.Select(typeof.Schema{"company": typeof.String})
		assert.NoError(t, err)
		assert.NotEqual(t, "LifeLock", col.LastRow()["company"])
	}
}
