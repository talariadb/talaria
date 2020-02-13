// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type closer func() error

func (c closer) Close() error {
	return c()
}

func TestClose(t *testing.T) {

	{ // Invalid types
		err := Close(1, 2, 3)
		assert.NoError(t, err)
	}

	{ // No error
		var c closer = func() error {
			return nil
		}
		err := Close(c)
		assert.NoError(t, err)
	}

	{ // With error
		var c closer = func() error {
			return fmt.Errorf("boom")
		}
		err := Close(c)
		assert.Error(t, err)
	}
}
