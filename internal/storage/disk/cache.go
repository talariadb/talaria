// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	lru "github.com/hashicorp/golang-lru"
)

const (
	maxMem    = 1 << 31 // 2 GB of memory to be used max
	maxKey    = 1 << 24 // 16 MB per key max
	cacheSize = maxMem / maxKey
)

type cache struct {
	lru *lru.ARCCache
}

// NewCache creates a new cache
func newCache() *cache {
	c, err := lru.NewARC(cacheSize)
	if err != nil {
		panic(err)
	}

	return &cache{
		lru: c,
	}
}

// Get gets a value from the cache
func (c *cache) Get(key []byte) ([]byte, bool) {
	if v, ok := c.lru.Get(string(key)); ok {
		return v.([]byte), true
	}
	return nil, false
}

// Set adds a key/value pair to the cache
func (c *cache) Set(key, value []byte) {
	c.lru.Add(string(key), value)
}

// Contains checks if a key is present in the cache
func (c *cache) Contains(key []byte) bool {
	isHit := c.lru.Contains(string(key))
	return isHit
}
