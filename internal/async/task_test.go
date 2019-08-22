// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package async

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTasks(t *testing.T) {
	work := func(context.Context) (interface{}, error) {
		return 1, nil
	}

	tasks := NewTasks(work, work, work)
	assert.Equal(t, 3, len(tasks))
}

func TestOutcome(t *testing.T) {
	task := Invoke(context.Background(), func(context.Context) (interface{}, error) {
		return 1, nil
	})

	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			o, _ := task.Outcome()
			wg.Done()
			assert.Equal(t, o.(int), 1)
		}()
	}
	wg.Wait()
}

func TestOutcomeTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	task := Invoke(ctx, func(context.Context) (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return 1, nil
	})

	_, err := task.Outcome()
	assert.Equal(t, "context deadline exceeded", err.Error())
}

func TestContinueWithChain(t *testing.T) {
	task1 := Invoke(context.Background(), func(context.Context) (interface{}, error) {
		return 1, nil
	})

	ctx := context.TODO()
	task2 := task1.ContinueWith(ctx, func(result interface{}, _ error) (interface{}, error) {
		return result.(int) + 1, nil
	})

	task3 := task2.ContinueWith(ctx, func(result interface{}, _ error) (interface{}, error) {
		return result.(int) + 1, nil
	})

	result, err := task3.Outcome()
	assert.Equal(t, 3, result)
	assert.Nil(t, err)
}

func TestContinueTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	first := Invoke(ctx, func(context.Context) (interface{}, error) {
		return 5, nil
	})

	second := first.ContinueWith(ctx, func(result interface{}, err error) (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return result, err
	})

	r1, err1 := first.Outcome()
	assert.Equal(t, 5, r1)
	assert.Nil(t, err1)

	_, err2 := second.Outcome()
	assert.Equal(t, "context deadline exceeded", err2.Error())
}

func TestTaskCancelStarted(t *testing.T) {
	task := Invoke(context.Background(), func(context.Context) (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return 1, nil
	})

	task.Cancel()

	_, err := task.Outcome()
	assert.Equal(t, errCancelled, err)
}

func TestTaskCancelRunning(t *testing.T) {
	task := Invoke(context.Background(), func(context.Context) (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return 1, nil
	})

	time.Sleep(10 * time.Millisecond)

	task.Cancel()

	_, err := task.Outcome()
	assert.Equal(t, errCancelled, err)
}
