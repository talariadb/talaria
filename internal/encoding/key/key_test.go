// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package key

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewKey(t *testing.T) {
	assert.Equal(t, []byte{0x3c, 0x25, 0x69, 0xb2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x32, 0x0, 0x0, 0x0, 0x1}, New("a", time.Unix(50, 0)))
}
