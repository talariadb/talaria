// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package async

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRepeat(t *testing.T) {
	assert.NotPanics(t, func() {
		out := make(chan bool, 1)
		task := Repeat(context.TODO(), time.Nanosecond*10, func(context.Context) (interface{}, error) {
			out <- true
			return nil, nil
		})

		<-out
		v := <-out
		assert.True(t, v)
		task.Cancel()
	})
}

func ExampleRepeat() {
	out := make(chan bool, 1)
	task := Repeat(context.TODO(), time.Nanosecond*10, func(context.Context) (interface{}, error) {
		out <- true
		return nil, nil
	})

	<-out
	v := <-out
	fmt.Println(v)
	task.Cancel()

	// Output:
	// true
}
