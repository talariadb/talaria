// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test1-zlib.orc"
const smallFile = "../../../test/test2.orc"

func TestFromOrc_Nested(t *testing.T) {
	o, err := ioutil.ReadFile(smallFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(nil)
	b, err := FromOrcBy(o, "string1", nil, apply)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(b))

	remapped, err := b[0].Select(typeof.Schema{"map": typeof.JSON})
	assert.NoError(t, err)
	assert.True(t, remapped["map"].Size() > 0)

}

func TestFromOrc_LargeFile(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(nil)
	b, err := FromOrcBy(o, "_col5", nil, apply)
	assert.NoError(t, err)
	assert.Equal(t, 56, len(b))
	assert.Equal(t, 9, len(b[0].Schema()))
}

func TestFromOrc_JSON(t *testing.T) {
	o, err := ioutil.ReadFile("../../../test/test5.orc")
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(&typeof.Schema{
		"ctx": typeof.JSON,
	})
	b, err := FromOrcBy(o, "event", &typeof.Schema{
		"ctx": typeof.JSON,
	}, apply)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(b))

	cols, err := b[0].Select(typeof.Schema{
		"ctx": typeof.JSON,
	})
	ctx := cols["ctx"]
	assert.NoError(t, err)
	assert.Equal(t, 3, ctx.Count())
	assert.NotEmpty(t, string(ctx.Last().(json.RawMessage)))
}
