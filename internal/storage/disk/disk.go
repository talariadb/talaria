// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"bytes"
	"context"
	"log"
	"os"
	"runtime/debug"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/grab/async"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage"
)

const (
	ctxTag = "disk"
)

// Assert contract compliance
var _ storage.Storage = new(Storage)

// Storage represents disk storage.
type Storage struct {
	gc      async.Task     // Closing channel
	db      *badger.DB     // The underlying key-value store
	monitor monitor.Client // The stats client
}

// New creates a new disk-backed storage which internally uses badger KV.
func New(m monitor.Client) *Storage {
	return &Storage{
		monitor: m,
	}
}

// Open opens a directory.
func (s *Storage) Open(dir string) error {

	// Default to a /data directory
	if dir == "" {
		dir = "/data"
	}

	// Make sure we have a directory
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	// Create the options
	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MaxTableSize = 64 << 15
	opts.ValueLogMaxEntries = 5000
	opts.LevelOneSize = 1 << 16
	opts.LevelSizeMultiplier = 3
	opts.MaxLevels = 25
	opts.Truncate = true
	opts.Logger = s.monitor

	// Attempt to open the database
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	// Setup the database and start GC
	s.db = db
	s.gc = async.Repeat(context.Background(), 1*time.Minute, s.GC)
	return nil
}

// Append adds an event into the storage.
func (s *Storage) Append(key key.Key, value []byte, ttl time.Duration) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(&badger.Entry{
			Key:       key,
			Value:     value,
			ExpiresAt: uint64(time.Now().Add(ttl).Unix()),
		})
	})
}

// Range performs a range query against the storage. It calls f sequentially for each key and value present in
// the store. If f returns false, range stops the iteration. The API is designed to be very similar to the concurrent
// map. The implementation must guarantee that the keys are lexigraphically sorted.
func (s *Storage) Range(seek, until key.Key, f func(key, value []byte) bool) error {
	return s.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         key.PrefixOf(seek, until),
		})
		defer it.Close()

		// Seek the prefix and check the key so we can quickly exit the iteration.
		for it.Seek(seek); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Compare(key, until) > 0 {
				return nil // Stop if we're reached the end
			}

			// Fetch the value
			if value, ok := s.fetch(key, item); ok && f(key, value) {
				return nil
			}
		}
		return nil
	})
}

// load attempts to load an item from either cache or badger.
func (s *Storage) fetch(key []byte, item *badger.Item) ([]byte, bool) {
	value, err := item.ValueCopy(nil)
	return value, err == nil
}

// Purge clears out the data prior to GC, to avoid some old data being
func (s *Storage) purge() (deleted, total int) {
	_ = s.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})

		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			total++
			key := it.Item().Key()
			if it.Item().ExpiresAt() <= uint64(time.Now().Unix()) {
				if err := s.Delete(key); err == nil {
					deleted++
				}
			}
		}
		return nil
	})
	return
}

// Delete deletes one or multiple keys from the storage.
func (s *Storage) Delete(keys ...key.Key) error {
	return s.db.Update(func(tx *badger.Txn) error {
		for _, key := range keys {
			if err := tx.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// GC runs the garbage collection on the storage
func (s *Storage) GC(ctx context.Context) (interface{}, error) {
	const tag = "GC"
	const discardRatio = 0.3

	if s.gc != nil && s.gc.State() == async.IsCancelled {
		return nil, nil
	}

	deleted, total := s.purge()
	s.monitor.Debugf("deleted %v / %v items, available %v", deleted, total, total-deleted)
	s.monitor.Gauge(ctxTag, "GC.purge", float64(deleted), "type:deleted")
	s.monitor.Gauge(ctxTag, "GC.purge", float64(total), "type:total")

	for true {
		if s.db.RunValueLogGC(discardRatio) != nil {
			s.monitor.Count1(ctxTag, "vlog.GC", "type:stopped")
			return nil, nil
		}
		s.monitor.Count1(ctxTag, "vlog.GC", "type:completed")
		s.monitor.Debugf("cycle completed")
	}
	return nil, nil
}

// Close is used to gracefully close the connection.
func (s *Storage) Close() error {
	if s.gc != nil {
		s.gc.Cancel()
	}
	return s.db.Close()
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %ss \n %s", r, debug.Stack())
	}
}
