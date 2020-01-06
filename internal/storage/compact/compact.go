// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package compact

import (
	"context"
	"time"

	"github.com/grab/async"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/storage"
)

type joinFunc = func([]block.Block, typeof.Schema) ([]byte, []byte)

// Assert contract compliance
var _ storage.Storage = new(Storage)

// Storage represents compactor storage.
type Storage struct {
	compact async.Task      // The compaction task
	buffer  storage.Storage // The storage to use for buffering
	db      storage.Storage // The compaction destination
	join    joinFunc        // The compaction function
}

// New creates a new storage implementation.
func New(buffer, db storage.Storage, interval time.Duration, join joinFunc) *Storage {
	s := &Storage{
		buffer: buffer,
		db:     db,
		join:   join,
	}
	s.compact = async.Repeat(context.Background(), interval, s.Compact)
	return s
}

// Append adds an event into the buffer.
func (s *Storage) Append(key, value []byte, ttl time.Duration) error {
	return s.buffer.Append(key, value, ttl)
}

// Range performs a range query against the storage. It calls f sequentially for each key and value present in
// the store. If f returns false, range stops the iteration. The API is designed to be very similar to the concurrent
// map. The implementation must guarantee that the keys are lexigraphically sorted.
func (s *Storage) Range(seek, until []byte, f func(key, value []byte) bool) error {
	return s.buffer.Range(seek, until, f)
}

// Compact runs the compaction on the storage
func (s *Storage) Compact(ctx context.Context) (interface{}, error) {
	var schema typeof.Schema
	var hash uint32
	var blocks []block.Block

	// Iterate through all of the blocks in the storage
	var rangeErr error
	if err := s.buffer.Range(key.First(), key.Last(), func(k, v []byte) bool {
		input, err := block.FromBuffer(v)
		if err != nil {
			rangeErr = err
			return true
		}

		// Update the current hash
		previous := hash
		hash = key.HashOf(k)

		// If the hash is unchanged and schemas merge cleanly, accumulate...
		if previous == 0 || (hash == previous && schema.Union(input.Schema())) {
			blocks = append(blocks, input)
			return false
		}

		// Join the blocks together and append them
		if err := s.joinAndAppend(blocks, schema); err != nil {
			rangeErr = err
			return true
		}

		// Reset both the schema and the set of blocks
		schema = make(typeof.Schema, len(schema))
		blocks = blocks[:0]
		return false
	}); err != nil {
		return nil, err
	}

	// If we have encountered an error during the iteration, return the error
	if rangeErr != nil {
		return nil, rangeErr
	}

	// Join and append one last time
	return nil, s.joinAndAppend(blocks, schema)
}

// joinAndAppend adds an key-value pair to the underlying database
func (s *Storage) joinAndAppend(blocks []block.Block, schema typeof.Schema) error {

	// Join the blocks together
	key, value := s.join(blocks, schema)

	// TODO: figure out the TTL that should be set
	return s.db.Append(key, value, 0)
}

// Close is used to gracefully close storage.
func (s *Storage) Close() error {
	s.compact.Cancel()  // Cancel periodic compaction
	s.buffer.Close()    // Close the buffer
	return s.db.Close() // Close the database
}
