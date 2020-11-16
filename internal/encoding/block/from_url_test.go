// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package block

import (
	"path/filepath"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestFromURL(t *testing.T) {
	p, err := filepath.Abs("../../../test/test4.csv")
	assert.NoError(t, err)

	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := Transform(schema)
	b, err := FromURLBy("file:///"+p, "raisedCurrency", schema, apply)
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
