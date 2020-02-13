// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"testing"

	"github.com/grab/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

func TestSplitCodec(t *testing.T) {
	assert.NotPanics(t, func() {
		id := encodeThriftID("roman", []byte("ABC"))
		assert.Equal(t, []byte{0x5, 0x72, 0x6f, 0x6d, 0x61, 0x6e, 0x3, 0x41, 0x42, 0x43}, id.Id)

		out, err := decodeThriftID(id, new(presto.PrestoThriftNullableToken))
		assert.NoError(t, err)
		assert.Equal(t, "roman", out.Table)
		assert.Equal(t, []byte("ABC"), out.Split)
	})
}
