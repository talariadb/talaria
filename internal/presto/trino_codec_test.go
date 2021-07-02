// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file
package presto

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetWireNameFor(t *testing.T) {
	wireName := "trino.GetTableMetadata"
	assert.Equal(t, "presto.GetTableMetadata", getWireNameFor(wireName))
	wireName = "presto.GetTableMetadata"
	assert.Equal(t, "presto.GetTableMetadata", getWireNameFor(wireName))
}
