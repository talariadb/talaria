// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/grab/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

// Opens a new disk storage and runs a a test on it.
func runTest(t *testing.T, test func(store *Storage)) {
	assert.NotPanics(t, func() {
		run(test)
	})
}

// Run runs a function on a temp store
func run(f func(store *Storage)) {
	dir, _ := ioutil.TempDir("", "test")
	store := New(monitor.NewNoop())
	_ = store.Open(dir)

	// Close once we're done and delete data
	defer func() { _ = os.RemoveAll(dir) }()
	defer func() { _ = store.Close() }()

	f(store)
}

func TestGC(t *testing.T) {
	runTest(t, func(store *Storage) {
		assert.NotPanics(t, func() {
			store.GC(context.Background())
		})
	})
}

func TestRange(t *testing.T) {
	runTest(t, func(store *Storage) {

		// Insert out of order
		_ = store.Append(asBytes("1"), asBytes("A"), 60*time.Second)
		_ = store.Append(asBytes("3"), asBytes("C"), 60*time.Second)
		_ = store.Append(asBytes("2"), asBytes("B"), 60*time.Second)

		// Iterate in order
		var values []string
		err := store.Range(asBytes("1"), asBytes("5"), func(k, v []byte) bool {
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
	runTest(t, func(store *Storage) {

		// Insert out of order
		_ = store.Append(asBytes("1"), asBytes("A"), 60*time.Second)
		_ = store.Append(asBytes("3"), asBytes("C"), 60*time.Second)
		_ = store.Append(asBytes("5"), asBytes("E"), 60*time.Second)
		_ = store.Append(asBytes("4"), asBytes("D"), 60*time.Second)
		_ = store.Append(asBytes("2"), asBytes("B"), 60*time.Second)
		_ = store.Append(asBytes("6"), asBytes("F"), 60*time.Second)

		// Iterate and stop at some point
		{
			var values []string
			err := store.Range(asBytes("1"), asBytes("5"), func(k, v []byte) bool {
				values = append(values, string(v))
				return string(k) == "3"
			})
			assert.NoError(t, err)
			assert.EqualValues(t, []string{"A", "B", "C"}, values)
		}

		// Force prefetch so we wait
		store.prefetch(asBytes("1"), asBytes("5"))

		// Second iteration should hit the prefetched values
		{
			var values []string
			err := store.Range(asBytes("3"), asBytes("5"), func(k, v []byte) bool {
				values = append(values, string(v))
				return false
			})
			assert.NoError(t, err)
			assert.EqualValues(t, []string{"C", "D", "E"}, values)
		}

	})
}

func asBytes(s string) []byte {
	return []byte(s)
}

// BenchmarkRange/range-8         	   12631	     93722 ns/op	    4072 B/op	     361 allocs/op
// BenchmarkRange/prefetch-8      	   10994	    108943 ns/op	    3550 B/op	     385 allocs/op
func BenchmarkRange(b *testing.B) {
	b.Run("range", func(b *testing.B) {
		run(func(store *Storage) {
			for i := 0; i < 1000; i++ {
				key := asBytes(fmt.Sprintf("%d", i))
				store.Append(key, key, 60*time.Second)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("300"), asBytes("600"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("prefetch", func(b *testing.B) {
		run(func(store *Storage) {
			for i := 0; i < 1000; i++ {
				key := asBytes(fmt.Sprintf("%d", i))
				store.Append(key, key, 60*time.Second)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("300"), asBytes("600"), func(k, v []byte) bool {
					return string(k) == "450"
				})

				b.StopTimer()
				store.prefetch(asBytes("300"), asBytes("600"))
				b.StartTimer()

				store.Range(asBytes("450"), asBytes("600"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})
}
