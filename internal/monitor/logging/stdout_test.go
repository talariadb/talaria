// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdLogger(t *testing.T) {
	assert.NotPanics(t, func() {
		l := NewStandard()
		l.Errorf("test %d", 1)
		l.Warningf("test %d", 1)
		l.Infof("test %d", 1)
		l.Debugf("test %d", 1)
	})
}
