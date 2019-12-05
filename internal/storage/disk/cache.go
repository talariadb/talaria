// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package disk

import (
	"github.com/coocood/freecache"
)

const (
	cacheSize  = 1 << 31 // 2 GB of memory to be used max
	defaultTTL = 180     // 5 minutes of TTL
)

type cache struct {
	lru *freecache.Cache
}

// NewCache creates a new cache
func newCache() *cache {
	return &cache{
		lru: freecache.NewCache(cacheSize),
	}
}

// Get gets a value from the cache
func (c *cache) Get(key []byte) ([]byte, bool) {
	v, err := c.lru.Get(key)
	if err != nil || v == nil {
		return nil, false
	}
	return v, true
}

// Set adds a key/value pair to the cache
func (c *cache) Set(key, value []byte) {
	c.lru.Set(key, value, defaultTTL)
}

// Contains checks if a key is present in the cache
func (c *cache) Contains(key []byte) bool {
	_, contains := c.Get(key)
	return contains
}
