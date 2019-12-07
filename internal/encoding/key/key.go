// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package key

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
)

// Next is used as a sequence number, it's okay to overflow.
var next uint32

// New generates a new key for the storage.
func New(eventName string, tsi time.Time) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint32(out[0:4], murmur3.Sum32([]byte(eventName)))
	binary.BigEndian.PutUint64(out[4:12], uint64(tsi.Unix()))
	binary.BigEndian.PutUint32(out[12:16], atomic.AddUint32(&next, 1))
	return out
}
