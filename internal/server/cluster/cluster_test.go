// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package cluster

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterNew(t *testing.T) {
	assert.NotPanics(t, func() {
		cluster := New(rand.Intn(30000) + 2000)

		err := cluster.Join("127.0.0.1")
		assert.NoError(t, err)

		members := cluster.Members()
		assert.NotEmpty(t, members)
		assert.NotEmpty(t, cluster.Addr())

	})
}
