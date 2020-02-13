// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test1-zlib.orc"
const smallFile = "../../../test/test2.orc"

func TestFromOrc_Nested(t *testing.T) {
	o, err := ioutil.ReadFile(smallFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrcBy(o, "string1")
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

	b, err := FromOrcBy(o, "_col5")
	assert.NoError(t, err)
	assert.Equal(t, 769, len(b))
}
