// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

import (
	"testing"

	"github.com/stretchr/testify/mock"
)

func TestCompositeLogger(t *testing.T) {
	logger1 := &MockLogger{}
	logger1.On("Infof", "tag", mock.Anything).Once()
	logger2 := &MockLogger{}
	logger2.On("Infof", "tag", mock.Anything).Once()

	composite := NewComposite(logger1, logger2)
	composite.Infof("tag", "msg")
	logger1.AssertExpectations(t)
	logger2.AssertExpectations(t)
}
