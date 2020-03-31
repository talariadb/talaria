// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

// New creates a new internal error
func New(msg string) error {
	return withError(codes.Internal, msg, nil)
}

// Newf creates a new internal error with a message format
func Newf(f string, args ...interface{}) error {
	return withError(codes.Internal, fmt.Sprintf(f, args...), nil)
}

// Internal ...
func Internal(msg string, err error, tags ...Tag) error {
	return withError(codes.Internal, msg, err)
}

// InvalidArgument ...
func InvalidArgument(msg string, tags ...Tag) error {
	return withMessage(codes.InvalidArgument, msg, tags...)
}

// AlreadyExists ...
func AlreadyExists(msg string, tags ...Tag) error {
	return withMessage(codes.AlreadyExists, msg, tags...)
}

// NotFound ...
func NotFound(msg string, tags ...Tag) error {
	return withMessage(codes.NotFound, msg, tags...)
}

// PermissionDenied ...
func PermissionDenied(msg string, tags ...Tag) error {
	return withMessage(codes.PermissionDenied, msg, tags...)
}

// Unimplemented ...
func Unimplemented(msg string, tags ...Tag) error {
	return withMessage(codes.Unimplemented, msg, tags...)
}

// ResourceExhausted ...
func ResourceExhausted(msg string, tags ...Tag) error {
	return withMessage(codes.ResourceExhausted, msg, tags...)
}

// Unauthenticated ...
func Unauthenticated(msg string, tags ...Tag) error {
	return withMessage(codes.Unauthenticated, msg, tags...)
}

// Unavailable ...
func Unavailable(msg string, tags ...Tag) error {
	return withMessage(codes.Unavailable, msg, tags...)
}

// DeadlineExceeded ...
func DeadlineExceeded(msg string, tags ...Tag) error {
	return withMessage(codes.DeadlineExceeded, msg, tags...)
}

// Canceled ...
func Canceled(msg string, tags ...Tag) error {
	return withMessage(codes.Canceled, msg, tags...)
}
