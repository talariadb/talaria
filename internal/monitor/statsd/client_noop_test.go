// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package statsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientNoopImplements(t *testing.T) {
	assert.Implements(t, (*Client)(nil), &clientNoop{})
}
