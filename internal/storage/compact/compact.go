// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package compact

import (
	"context"
	"runtime"
	"time"

	"github.com/grab/async"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/storage"
)

// Assert contract compliance
var _ storage.Storage = new(Storage)

// Storage represents compactor storage.
type Storage struct {
	compact async.Task // The compaction worker

	monitor monitor.Monitor  // The monitor client
	merger  storage.Merger   // The merging (join) function
	buffer  storage.Storage  // The storage to use for buffering
	dest    storage.Appender // The compaction destination
}

// New creates a new storage implementation.
func New(buffer storage.Storage, dest storage.Appender, merger storage.Merger, monitor monitor.Monitor, interval time.Duration) *Storage {
	s := &Storage{
		monitor: monitor,
		merger:  merger,
		buffer:  buffer,
		dest:    dest,
	}
	s.compact = compactEvery(interval, s.Compact)
	return s
}

// compactEvery returns the task that compacts on a regular interval.
func compactEvery(interval time.Duration, compact async.Work) async.Task {
	return async.Invoke(context.Background(), func(ctx context.Context) (interface{}, error) {
		for {
			select {
			case <-ctx.Done():
				return nil, nil
			default:
				time.Sleep(interval)
				compact(ctx)
			}
		}
	})
}

// Append adds an event into the buffer.
func (s *Storage) Append(key key.Key, value []byte, ttl time.Duration) error {
	return s.buffer.Append(key, value, ttl)
}

// Range performs a range query against the storage. It calls f sequentially for each key and value present in
// the store. If f returns false, range stops the iteration. The API is designed to be very similar to the concurrent
// map. The implementation must guarantee that the keys are lexigraphically sorted.
func (s *Storage) Range(seek, until key.Key, f func(key, value []byte) bool) error {
	if iter, ok := s.dest.(storage.Iterator); ok {
		return iter.Range(seek, until, f)
	}

	// If the destination cannot be iterated over, read from the buffer instead
	return s.buffer.Range(seek, until, f)
}

// Delete deletes a key from the buffer.
func (s *Storage) Delete(keys ...key.Key) error {
	return s.buffer.Delete(keys...)
}

// Compact runs the compaction on the storage
func (s *Storage) Compact(ctx context.Context) (interface{}, error) {
	var hash uint32
	var blocks []block.Block
	var merged []key.Key

	concurrency := runtime.NumCPU()
	queue := make(chan async.Task, concurrency)
	wpool := async.Consume(context.Background(), concurrency, queue)

	// Iterate through all of the blocks in the storage
	schema := make(typeof.Schema, 4)
	if err := s.buffer.Range(key.First(), key.Last(), func(k, v []byte) bool {
		input, err := block.FromBuffer(v)
		if err != nil {
			s.monitor.Error(errors.Internal("compact: unable to read a buffer", err))
			return true
		}

		// Update the current hash
		previous := hash
		hash = key.HashOf(k)

		// If the hash is unchanged and schemas merge cleanly, accumulate...
		if mergedSchema, ok := schema.Union(input.Schema()); previous == 0 || (ok && hash == previous) {
			blocks = append(blocks, input)
			merged = append(merged, key.Clone(k))
			schema = mergedSchema
			return false
		}

		// Merge asynchronously and delete the keys on a successful merge
		queue <- s.merge(merged, blocks, schema)

		// Reset both the schema and the set of blocks
		schema = make(typeof.Schema, len(schema))
		blocks = make([]block.Block, 0, 16)
		merged = make([]key.Key, 0, 16)
		return false
	}); err != nil {
		return nil, err
	}

	// Merge one last time if we still have block
	if len(blocks) > 0 {
		queue <- s.merge(merged, blocks, schema)
	}

	// Wait for the pool to be close
	close(queue)
	return wpool.Outcome()
}

// merge adds an key-value pair to the underlying database
func (s *Storage) merge(keys []key.Key, blocks []block.Block, schema typeof.Schema) async.Task {
	return async.NewTask(func(ctx context.Context) (_ interface{}, err error) {

		if len(blocks) == 0 {
			return
		}
		// Get the max expiration time for merging
		now, max := time.Now().Unix(), int64(0)
		for _, b := range blocks {
			if b.Expires > max {
				max = b.Expires
			}
		}

		// Merge all blocks together
		key, value := s.merger.Merge(blocks, schema)

		// Append to the destination
		ttl := time.Duration(max-now) * time.Second
		if err = s.dest.Append(key, value, ttl); err != nil {
			s.monitor.Error(err)
			return
		}

		//  Delete all of the keys that we have appended
		if err = s.buffer.Delete(keys...); err != nil {
			s.monitor.Error(err)
		}
		return
	})
}

// Close is used to gracefully close storage.
func (s *Storage) Close() error {
	s.compact.Cancel()
	return storage.Close(s.buffer, s.dest)
}
