// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"testing"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestNullColumn(t *testing.T) {
	assert.Equal(t, 100, NullColumn(typeof.Int32, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.Bool, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.Int64, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.Float64, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.Timestamp, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.String, 100).Count())
	assert.Equal(t, 100, NullColumn(typeof.JSON, 100).Count())
}
