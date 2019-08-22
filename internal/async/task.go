// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package async

import (
	"context"
	"errors"
	"sync/atomic"
)

var errCancelled = errors.New("context canceled")

// Work represents a handler to execute
type Work func(context.Context) (interface{}, error)

// State represents the state enumeration for a task.
type State byte

// Various task states
const (
	IsCreated   State = iota // IsCreated represents a newly created task
	IsRunning                // IsRunning represents a task which is currently running
	IsCompleted              // IsCompleted represents a task which was completed successfully or errored out
	IsCancelled              // IsCancelled represents a task which was cancelled or has timed out
)

// Outcome of the task contains a result and an error
type outcome struct {
	result interface{} // The result of the work
	err    error       // The error
}

// Task represents a unit of work to be done
type task struct {
	state   int32              // This indicates whether the task is started or not
	cancel  context.CancelFunc // The cancellation function
	action  Work               // The work to do
	done    chan bool          // The outcome channel
	outcome outcome            // This is used to store the result
}

// Task represents a unit of work to be done
type Task interface {
	Run(ctx context.Context) Task
	Cancel()
	State() State
	Outcome() (interface{}, error)
	ContinueWith(ctx context.Context, nextAction func(interface{}, error) (interface{}, error)) Task
}

// NewTask creates a new task.
func NewTask(action Work) Task {
	return &task{
		action: action,
		done:   make(chan bool, 1),
	}
}

// NewTasks creates a set of new tasks.
func NewTasks(actions ...Work) []Task {
	tasks := make([]Task, 0, len(actions))
	for _, action := range actions {
		tasks = append(tasks, NewTask(action))
	}
	return tasks
}

// Invoke creates a new tasks and runs it asynchronously.
func Invoke(ctx context.Context, action Work) Task {
	return NewTask(action).Run(ctx)
}

// Outcome waits until the task is done and returns the final result and error.
func (t *task) Outcome() (interface{}, error) {
	<-t.done
	return t.outcome.result, t.outcome.err
}

// State returns the current state of the task. This operation is non-blocking.
func (t *task) State() State {
	v := atomic.LoadInt32(&t.state)
	return State(v)
}

// Run starts the task asynchronously.
func (t *task) Run(ctx context.Context) Task {
	ctx, t.cancel = context.WithCancel(ctx)
	go t.run(ctx)
	return t
}

// Cancel cancels a running task.
func (t *task) Cancel() {

	// If the task was created but never started, transition directly to cancelled state
	// and close the done channel and set the error.
	if t.changeState(IsCreated, IsCancelled) {
		t.outcome = outcome{err: errCancelled}
		close(t.done)
		return
	}

	// Attempt to cancel the task if it's in the running state
	if t.cancel != nil {
		t.cancel()
	}
}

// run starts the task synchronously.
func (t *task) run(ctx context.Context) {
	if !t.changeState(IsCreated, IsRunning) {
		return // Prevent from running the same task twice
	}

	// Notify everyone of the completion/error state
	defer close(t.done)

	// Execute the task
	outcomeCh := make(chan outcome, 1)
	go func() {
		r, e := t.action(ctx)
		outcomeCh <- outcome{result: r, err: e}
	}()

	select {

	// In case of the context timeout or other error, change the state of the
	// task to cancelled and return right away.
	case <-ctx.Done():
		t.outcome = outcome{err: ctx.Err()}
		t.changeState(IsRunning, IsCancelled)
		return

	// In case where we got an outcome (happy path)
	case o := <-outcomeCh:
		t.outcome = o
		t.changeState(IsRunning, IsCompleted)
		return
	}
}

// ContinueWith proceeds with the next task once the current one is finished.
func (t *task) ContinueWith(ctx context.Context, nextAction func(interface{}, error) (interface{}, error)) Task {
	return Invoke(ctx, func(context.Context) (interface{}, error) {
		result, err := t.Outcome()
		return nextAction(result, err)
	})
}

// Cancel cancels a running task.
func (t *task) changeState(from, to State) bool {
	return atomic.CompareAndSwapInt32(&t.state, int32(from), int32(to))
}
