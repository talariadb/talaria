// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3orc

import (
	"context"
	"errors"
	"time"

	"github.com/grab/async"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage"
)

// Assert contract compliance
var _ storage.Appender = new(Storage)
var _ storage.Merger = new(Storage)

// Storage represents s3/orc storage.
type Storage struct {
	monitor monitor.Client  // The monitor client
	workers async.Task      // The worker pool
	tasks   chan async.Task // The task channel to upload
}

// New creates a new storage implementation.
func New(monitor monitor.Client, concurrency int) *Storage {
	tasks := make(chan async.Task, concurrency)
	return &Storage{
		monitor: monitor,
		tasks:   tasks,
		workers: async.Consume(context.Background(), concurrency, tasks),
	}
}

// Append adds an event into the buffer.
func (s *Storage) Append(key, value []byte, ttl time.Duration) error {
	return errors.New("not implemented")
}

// Merge merges multiple blocks together and ouputs a valid orc file to be uploaded to s3.
func (s *Storage) Merge(blocks []block.Block, schema typeof.Schema) ([]byte, []byte) {
	return nil, nil
}

// Close is used to gracefully close storage.
func (s *Storage) Close() error {
	s.workers.Cancel()
	return nil
}
