// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package async

import (
	"context"
	"log"
	"runtime/debug"
	"time"
)

// Repeat performs an action asynchronously on a predetermined interval.
func Repeat(ctx context.Context, interval time.Duration, action Work) Task {
	safeAction := func(ctx context.Context) (interface{}, error) {
		defer handlePanic()
		return action(ctx)
	}

	// Invoke the task timer
	return Invoke(ctx, func(context.Context) (interface{}, error) {
		timer := time.NewTicker(interval)
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, nil

			case <-timer.C:
				_, _ = safeAction(ctx)
			}
		}
	})
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %ss \n %s", r, debug.Stack())
	}
}
