// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"time"
	"unsafe"

	"github.com/allegro/bigcache/v2"
)

const (
	maxCacheSize  = 1 << 31 // 2 GB of memory to be used max
	maxEntrySize  = 1 << 24 // 16 MB per key max
	maxCacheItems = maxCacheSize / maxEntrySize
)

type cache struct {
	lru *bigcache.BigCache
}

// NewCache creates a new cache
func newCache() *cache {
	config := bigcache.DefaultConfig(5 * time.Second)
	config.Verbose = false
	config.HardMaxCacheSize = 2024 // MB
	c, err := bigcache.NewBigCache(config)
	if err != nil {
		panic(err)
	}

	return &cache{
		lru: c,
	}
}

// Get gets a value from the cache
func (c *cache) Get(key []byte) ([]byte, bool) {
	v, err := c.lru.Get(binaryToString(&key))
	if err != nil || v == nil {
		return nil, false
	}
	return v, true
}

// Set adds a key/value pair to the cache
func (c *cache) Set(key, value []byte) {
	c.lru.Set(string(key), value)
}

// Contains checks if a key is present in the cache
func (c *cache) Contains(key []byte) bool {
	_, contains := c.Get(key)
	return contains
}

func binaryToString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}
