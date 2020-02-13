// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package key

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewKey(t *testing.T) {
	assert.Equal(t, Key{0x3c, 0x25, 0x69, 0xb2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x32, 0x0, 0x0, 0x0, 0x1}, New("a", time.Unix(50, 0)))
	assert.Equal(t, Key{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, First())
	assert.Equal(t, Key{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, Last())
	assert.Equal(t, uint32(0x3c2569b2), HashOf(New("a", time.Unix(50, 0))))
}

func TestPrefixOf(t *testing.T) {
	assert.Equal(t, []byte{0x33}, PrefixOf(asKey("3000"), asKey("3999")))
	assert.Equal(t, []byte(nil), PrefixOf(asKey("300"), asKey("600")))
}

func TestClone(t *testing.T) {
	assert.Equal(t, asKey("3000"), Clone(asKey("3000")))
}

func asKey(s string) Key {
	return Key(s)
}
