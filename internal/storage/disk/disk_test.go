// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

// Opens a new disk storage and runs a a test on it.
func runTest(t *testing.T, test func(store *Storage)) {
	assert.NotPanics(t, func() {
		run(t, test)
	})
}

// Run runs a function on a temp store
func run(tb testing.TB, f func(store *Storage)) {
	dir := tb.TempDir()
	store := New(monitor.NewNoop())
	_ = store.Open(dir, config.Badger{})

	// Close once we're done and delete data
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

func TestRange_Prefix(t *testing.T) {
	runTest(t, func(store *Storage) {
		populate(store)

		// Iterate in order
		count := 0
		err := store.Range(asBytes("3000"), asBytes("3999"), func(k, v []byte) bool {
			count++
			return false
		})

		// Must be in order
		assert.NoError(t, err)
		assert.Equal(t, 1000, count)
	})
}

func TestRange_NoPrefix(t *testing.T) {
	runTest(t, func(store *Storage) {
		populate(store)

		// Iterate in order
		count := 0
		err := store.Range(asBytes("3000"), asBytes("5999"), func(k, v []byte) bool {
			count++
			return false
		})

		// Must be in order
		assert.NoError(t, err)
		assert.Equal(t, 3000, count)
	})
}

func TestDelete(t *testing.T) {
	const count = 10000
	runTest(t, func(store *Storage) {

		// Write some data
		var keys []key.Key
		for i := 0; i < count; i++ {
			k := key.Key(strconv.Itoa(i))
			keys = append(keys, k)
			_ = store.Append(k, k, 60*time.Second)
		}

		// Delete the keys
		assert.Equal(t, count, countKeys(store))
		assert.NoError(t, store.Delete(keys...))
		assert.Equal(t, 0, countKeys(store))
	})
}

// countKeys counts the number of keys in the store
func countKeys(store *Storage) (count int) {
	store.Range(key.First(), key.Last(), func(k, v []byte) bool {
		count++
		return false
	})
	return
}

func asBytes(s string) []byte {
	return []byte(s)
}

// BenchmarkRange/head-1-pass-8         	    5568	    205270 ns/op	   10365 B/op	    1054 allocs/op
// BenchmarkRange/head-2-pass-8         	    5583	    208472 ns/op	   11836 B/op	    1089 allocs/op
// BenchmarkRange/mid-1-pass-8          	    5626	    203329 ns/op	   10368 B/op	    1055 allocs/op
// BenchmarkRange/mid-2-pass-8          	    5449	    209387 ns/op	   11851 B/op	    1090 allocs/op
// BenchmarkRange/tail-1-pass-8         	    6411	    171898 ns/op	    9353 B/op	    1023 allocs/op
// BenchmarkRange/tail-2-pass-8         	    4495	    260039 ns/op	   14442 B/op	    1546 allocs/op
func BenchmarkRange(b *testing.B) {
	b.Run("head-1-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("1000"), asBytes("1999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("head-2-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("1000"), asBytes("1999"), func(k, v []byte) bool {
					return string(k) == "1500"
				})

				store.Range(asBytes("1500"), asBytes("1999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("mid-1-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("3000"), asBytes("3999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("mid-2-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("3000"), asBytes("3999"), func(k, v []byte) bool {
					return string(k) == "3500"
				})

				store.Range(asBytes("3500"), asBytes("3999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("tail-1-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("9000"), asBytes("9999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})

	b.Run("tail-2-pass", func(b *testing.B) {
		run(b, func(store *Storage) {
			populate(store)

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				store.Range(asBytes("9000"), asBytes("9999"), func(k, v []byte) bool {
					return string(k) == "3500"
				})

				store.Range(asBytes("9500"), asBytes("9999"), func(k, v []byte) bool {
					return false
				})
			}
		})
	})
}

func populate(store *Storage) {
	for i := 1000; i < 10000; i++ {
		key := asBytes(fmt.Sprintf("%d", i))
		store.Append(key, key, 60*time.Second)
	}
}

func TestOpen(t *testing.T) {
	assert.NotPanicsf(t, func() {
		syncWrites := false
		disk := Open(t.TempDir(), "test-table", monitor.NewNoop(), config.Badger{
			SyncWrites: &syncWrites,
		})
		assert.NotNil(t, disk)
		disk.Close()
	}, "Panic while creating disk storage and opening the directory")
}
