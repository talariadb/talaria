// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombine(t *testing.T) {
	err1 := Unavailable("doom")
	err2 := Unavailable("gloom")

	err3 := Combine(err1, error(nil), err2)
	assert.NotNil(t, err3)
	assert.Equal(t, "2 errors occurred:\n\t* ServerError: target=monitor/errors/multi_test.go.13, reason=Service Unavailable, msg=doom\n\t* ServerError: target=monitor/errors/multi_test.go.14, reason=Service Unavailable, msg=gloom\n\n", err3.Error())
}

func TestCombine_Empty(t *testing.T) {
	err := Combine(error(nil), error(nil), error(nil))
	assert.Nil(t, err)
}
