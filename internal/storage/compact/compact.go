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
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage"
)

// Assert contract compliance
var _ storage.Storage = new(Storage)

// Storage represents compactor storage.
type Storage struct {
	compact async.Task       // The compaction task
	monitor monitor.Client   // The monitor client
	merger  storage.Merger   // The merging (join) function
	buffer  storage.Storage  // The storage to use for buffering
	dest    storage.Appender // The compaction destination
}

// New creates a new storage implementation.
func New(buffer storage.Storage, dest storage.Appender, merger storage.Merger, monitor monitor.Client, interval time.Duration) *Storage {
	s := &Storage{
		monitor: monitor,
		merger:  merger,
		buffer:  buffer,
		dest:    dest,
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
	if iter, ok := s.dest.(storage.Iterator); ok {
		return iter.Range(seek, until, f)
	}

	// If the destination cannot be iterated over, read from the buffer instead
	return s.buffer.Range(seek, until, f)
}

// Delete deletes a key from the buffer.
func (s *Storage) Delete(key []byte) error {
	return s.buffer.Delete(key)
}

// Compact runs the compaction on the storage
func (s *Storage) Compact(ctx context.Context) (interface{}, error) {
	var hash uint32
	var blocks []block.Block
	var merged [][]byte

	// Iterate through all of the blocks in the storage
	schema := make(typeof.Schema, 4)
	if err := s.buffer.Range(key.First(), key.Last(), func(k, v []byte) bool {
		input, err := block.FromBuffer(v)
		if err != nil {
			s.monitor.Errorf("compact: unable to read a buffer, %v", err.Error())
			return true
		}

		// Update the current hash
		previous := hash
		hash = key.HashOf(k)

		// If the hash is unchanged and schemas merge cleanly, accumulate...
		if previous == 0 || (hash == previous && schema.Union(input.Schema())) {
			blocks = append(blocks, input)
			merged = append(merged, k)
			return false
		}

		// Join the blocks together and append them
		if err := s.joinAndAppend(blocks, schema); err != nil {
			s.monitor.Errorf("compact: unable to merge, %v", err.Error())
			return true
		}

		//  Delete all of the keys that we have appended
		for _, mergedKey := range merged {
			if err := s.buffer.Delete(mergedKey); err != nil {
				s.monitor.Errorf("compact: unable to delete the key %v, %v", mergedKey, err.Error())
			}
		}

		// Reset both the schema and the set of blocks
		schema = make(typeof.Schema, len(schema))
		blocks = blocks[:0]
		merged = merged[:0]
		return false
	}); err != nil {
		return nil, err
	}

	// Join and append one last time
	if err := s.joinAndAppend(blocks, schema); err != nil {
		s.monitor.Errorf("compact: unable to merge, %v", err.Error())
	}
	return nil, nil
}

// joinAndAppend adds an key-value pair to the underlying database
func (s *Storage) joinAndAppend(blocks []block.Block, schema typeof.Schema) error {

	// Join the blocks together
	key, value := s.merger.Merge(blocks, schema)

	// TODO: figure out the TTL that should be set
	return s.dest.Append(key, value, 0)
}

// Close is used to gracefully close storage.
func (s *Storage) Close() error {
	s.compact.Cancel()
	return storage.Close(s.buffer, s.dest)
}
