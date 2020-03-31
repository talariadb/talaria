// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error represents an error with context.
type Error struct {
	http    int        // The HTTP status of the error
	grpc    codes.Code // The GRPC error code
	tags    []Tag      // The set of tags
	err     error      // The error object
	Target  string     `json:"target"`  // The error target, with the line of code
	Reason  string     `json:"reason"`  // The error reason
	Message string     `json:"message"` // The error message
}

// HTTP returns a HTTP status code.
func (r *Error) HTTP() int {
	if r.http != 0 {
		return r.http
	}
	return 520 // Cloudflare has 520 Unknown Error
}

// GRPC returns a GRPC status code.
func (r *Error) GRPC() codes.Code {
	// codes.OK is the default value, but shall never used for errors
	if r.grpc != codes.OK {
		return r.grpc
	}
	return codes.Unknown
}

// Error ...
func (r *Error) Error() string {
	return fmt.Sprintf("ServerError: target=%v, reason=%v, msg=%v", r.Target, r.Reason, r.Message)
}

// RPCError used for encoding server errors
func (r *Error) RPCError() error {
	return status.Errorf(r.grpc, "target=%v, reason=%v, msg=%v", r.Target, r.Reason, r.Message)
}

// Makes an error with an internal error object.
func withError(code codes.Code, message string, err error, tags ...Tag) error {
	if e, ok := err.(*Error); ok {
		return e // Avoid double-wrapping the error
	}

	// Add explanation for the internal error
	if err != nil {
		message += " due to " + err.Error()
	}

	status := statusFor(code)
	return &Error{
		http:    status,
		grpc:    code,
		err:     err,
		Reason:  http.StatusText(status),
		Target:  caller(3), // withError -> (eg. Internal) -> X
		Message: message,
	}
}

// Makes an error with an error message.
func withMessage(code codes.Code, message string, tags ...Tag) *Error {
	status := statusFor(code)
	return &Error{
		http:    status,
		grpc:    code,
		Reason:  http.StatusText(status),
		Target:  caller(3), // makeError -> (eg. Internal) -> X
		Message: message,
	}
}

// Caller formats the caller
func caller(skip int) string {
	const watermark = "talaria/internal/"
	_, fn, line, _ := runtime.Caller(skip)
	idx := strings.LastIndex(fn, watermark)
	if idx > 0 {
		idx += len(watermark)
	}

	return fmt.Sprintf("%s.%d", fn[idx:], line)
}

func statusFor(code codes.Code) int {
	switch code {
	case codes.Canceled:
		return 499
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unknown:
		fallthrough
	default:
		return 520
	}
}
