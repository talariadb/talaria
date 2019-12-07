// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFile = "../../../test/test1-zlib.orc"
const smallFile = "../../../test/test2.orc"

func TestFromOrcBy(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrcBy(o, "_col5")
	assert.NoError(t, err)
	assert.Equal(t, 1372, len(b))
}

func TestFromOrc(t *testing.T) {
	o, err := ioutil.ReadFile(testFile)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	b, err := FromOrc("test", o)
	assert.NoError(t, err)

	assert.Equal(t, int64(139395200), b.Size)

	const column = "_col5"
	result, err := b.Select([]string{column})
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))

	encoded, err := b.Encode()
	assert.NotEmpty(t, encoded)
	assert.NoError(t, err)

	back, err := FromBuffer(encoded)
	assert.NotNil(t, back)
	assert.NoError(t, err)

	result, err = back.Select([]string{column})
	assert.NoError(t, err)
	assert.Equal(t, 1920800, len(result[column].VarcharData.Sizes))
}
