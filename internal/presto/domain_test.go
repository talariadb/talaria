// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDomain(t *testing.T) {
	d, err := NewDomain("event", "tsi", "event == 'test'")
	assert.NoError(t, err)
	assert.Equal(t, &PrestoThriftTupleDomain{
		Domains: map[string]*PrestoThriftDomain{
			"event": equalsHash("test"),
		},
	}, d)
}
