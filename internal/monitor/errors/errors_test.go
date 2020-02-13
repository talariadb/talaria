// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.13, reason=Service Unavailable, msg=boom`, Unavailable("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.14, reason=Internal Server Error, msg=boom`, Internal("boom", nil).Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.15, reason=Bad Request, msg=boom`, InvalidArgument("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.16, reason=Conflict, msg=boom`, AlreadyExists("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.17, reason=Not Found, msg=boom`, NotFound("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.18, reason=Forbidden, msg=boom`, PermissionDenied("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.19, reason=Not Implemented, msg=boom`, Unimplemented("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.20, reason=Too Many Requests, msg=boom`, ResourceExhausted("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.21, reason=Unauthorized, msg=boom`, Unauthenticated("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.22, reason=Service Unavailable, msg=boom`, Unavailable("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.23, reason=Gateway Timeout, msg=boom`, DeadlineExceeded("boom").Error())
	assert.Equal(t, `ServerError: target=monitor/errors/errors_test.go.24, reason=, msg=boom`, Canceled("boom").Error())
}
