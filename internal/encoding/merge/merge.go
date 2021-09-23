// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package merge

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
)

// Func represents merge function
type Func func(interface{}) ([]byte, error)

// New creates a new merge function
func New(mergeFunc string) (map[string]Func, error) {
	encoder := make(map[string]Func)
	blockEncoder, err := newBlockEncoder(mergeFunc)
	if err != nil {
		return nil, err
	}
	rowEncoder, err := newRowEncoder(mergeFunc)
	if err != nil {
		return nil, err
	}
	encoder["block"] = blockEncoder
	encoder["row"] = rowEncoder
	return encoder, nil
}

func newBlockEncoder(mergeFunc string) (Func, error) {
	switch strings.ToLower(mergeFunc) {
	case "orc":
		return ToOrc, nil
	case "parquet":
		return ToParquet, nil
	case "block":
		return ToBlock, nil
	case "": // Default to "orc" so we don't break existing configs
		return ToOrc, nil
	}
	return nil, nil
}

func newRowEncoder(mergeFunc string) (Func, error) {
	switch mergeFunc {
	case "json":
		return Func(json.Marshal), nil
	default:
		return nil, nil
	}
}

// ----------------------------------------------------------------------------

// Clone clones the buffer into one which can be returned
func clone(b *bytes.Buffer) []byte {
	output := make([]byte, len(b.Bytes()))
	copy(output, b.Bytes())
	return output
}

// A memory pool for reusable temporary buffers
var buffers = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 16*1<<20))
	},
}

// Acquire gets a buffer from the pool
func acquire() *bytes.Buffer {
	return buffers.Get().(*bytes.Buffer)
}

// Release releases the buffer back to the pool
func release(buffer *bytes.Buffer) {
	buffer.Reset()
	buffers.Put(buffer)
}
