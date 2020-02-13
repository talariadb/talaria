// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"github.com/hashicorp/go-multierror"
)

// Combine combines multiple errors together into a same error.
func Combine(errs ...error) error {
	var errCount int
	var result multierror.Error

	// Combine all the errors together
	for _, err := range errs {
		if err != nil {
			errCount++
			multierror.Append(&result, err)
		}
	}

	// If there's no errors, return nil error
	if errCount == 0 {
		return nil
	}
	return &result
}
