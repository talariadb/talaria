// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package storage

import (
	"context"
	"io"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// Storage represents a contract that supports both iteration and append.
type Storage interface {
	io.Closer
	Iterator
	Appender
	Compacter
	Delete(...key.Key) error
}

// Iterator represents a contract that allows iterating over a storage.
type Iterator interface {
	Range(seek, until key.Key, f func(key, value []byte) bool) error
}

// Appender represents a contract that allows appending to a storage.
type Appender interface {
	Append(key key.Key, value []byte, ttl time.Duration) error
}

// Compact...
type Compacter interface {
	Compact(ctx context.Context) (interface{}, error)
}

// Merger represents a contract that merges two or more blocks together.
type Merger interface {
	Merge([]block.Block, typeof.Schema) ([]byte, []byte)
}

// Streamer represents a contract that streams out a row of data.
type Streamer interface {
	Stream(block.Row) error
}

// Close attempts to close one or multiple storages
func Close(objs ...interface{}) error {
	var result error
	for _, obj := range objs {
		if closer, ok := obj.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				result = multierror.Append(result, err)
			}
		}
	}
	return result
}
