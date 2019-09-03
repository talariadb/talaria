// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

// Opens a new disk storage and runs a a test on it.
func runTest(test func(store *Storage)) {

	// Prepare a store
	dir, _ := ioutil.TempDir("", "test")
	store := New(monitor.NewNoop())
	_ = store.Open(dir)

	// Close once we're done and delete data
	defer func() { _ = os.RemoveAll(dir) }()
	defer func() { _ = store.Close() }()

	test(store)
}

func TestGC(t *testing.T) {
	runTest(func(store *Storage) {
		assert.NotPanics(t, func() {
			store.GC(context.Background())
		})
	})
}

func TestRange(t *testing.T) {
	runTest(func(store *Storage) {

		// Insert out of order
		_ = store.Append(b("1"), b("A"), 60*time.Second)
		_ = store.Append(b("3"), b("C"), 60*time.Second)
		_ = store.Append(b("2"), b("B"), 60*time.Second)

		// Iterate in order
		var values []string
		err := store.Range(b("1"), b("5"), func(k, v []byte) bool {
			values = append(values, string(v))
			return false
		})

		// Must be in order
		assert.NoError(t, err)
		assert.EqualValues(t, []string{"A", "B", "C"}, values)

		// Purge
		deleted, total := store.purge()
		assert.Equal(t, 0, deleted)
		assert.Equal(t, 3, total)
	})
}

func TestRangeWithPrefetch(t *testing.T) {
	runTest(func(store *Storage) {

		// Insert out of order
		_ = store.Append(b("1"), b("A"), 60*time.Second)
		_ = store.Append(b("3"), b("C"), 60*time.Second)
		_ = store.Append(b("5"), b("E"), 60*time.Second)
		_ = store.Append(b("4"), b("D"), 60*time.Second)
		_ = store.Append(b("2"), b("B"), 60*time.Second)
		_ = store.Append(b("6"), b("F"), 60*time.Second)

		// Iterate and stop at some point
		{
			var values []string
			err := store.Range(b("1"), b("5"), func(k, v []byte) bool {
				values = append(values, string(v))
				return string(k) == "3"
			})
			assert.NoError(t, err)
			assert.EqualValues(t, []string{"A", "B", "C"}, values)
		}

		// Force prefetch so we wait
		store.prefetch(b("1"), b("5"))

		// Second iteration should hit the prefetched values
		{
			var values []string
			err := store.Range(b("3"), b("5"), func(k, v []byte) bool {
				values = append(values, string(v))
				return false
			})
			assert.NoError(t, err)
			assert.EqualValues(t, []string{"C", "D", "E"}, values)
		}

	})
}

func b(s string) []byte {
	return []byte(s)
}
