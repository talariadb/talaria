// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestError(t *testing.T) {
	err := newErr()
	assert.Equal(t, codes.Unavailable, err.GRPC())
	assert.Equal(t, http.StatusServiceUnavailable, err.HTTP())

}

func newErr() *Error {
	return withMessage(codes.Unavailable, "test")
}
