// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"testing"

	talaria "github.com/grab/talaria/proto"
	"github.com/stretchr/testify/assert"
)

var testBatch = &talaria.Batch{
	Strings: map[uint32][]byte{
		1:  []byte("a"),
		2:  []byte("b"),
		3:  []byte("c"),
		4:  []byte("d"),
		5:  []byte("event1"),
		6:  []byte("event2"),
		7:  []byte("hello"),
		8:  []byte("e"),
		9:  []byte("event3"),
		10: []byte(`{"name": "roman"}`),
	},
	Events: []*talaria.Event{
		{Value: map[uint32]*talaria.Value{
			1: {Value: &talaria.Value_Int64{Int64: 10}},
			2: {Value: &talaria.Value_Int64{Int64: 20}},
			4: {Value: &talaria.Value_String_{String_: 5}}, // event1
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_Int64{Int64: 20}},
			4: {Value: &talaria.Value_String_{String_: 5}}, // event1
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_String_{String_: 7}},
			4: {Value: &talaria.Value_String_{String_: 6}}, // event2
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_String_{String_: 7}},
			4: {Value: &talaria.Value_String_{String_: 6}}, // event2
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_String_{String_: 7}},
			4: {Value: &talaria.Value_String_{String_: 6}}, // event2
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_Time{Time: 100}},
			8: {Value: &talaria.Value_Json{Json: 10}},
			4: {Value: &talaria.Value_String_{String_: 9}}, // event3
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_String_{String_: 7}},
			4: {Value: &talaria.Value_String_{String_: 6}}, // event2
		}},
		{Value: map[uint32]*talaria.Value{
			2: {Value: &talaria.Value_String_{String_: 7}},
			4: {Value: &talaria.Value_String_{String_: 6}}, // event2
		}},
	},
}

func TestBlock_FromBatch(t *testing.T) {
	blocks, err := FromBatchBy(testBatch, "d")
	assert.NoError(t, err)
	assert.Len(t, blocks, 3) // Number of partitions

	// Find the event2 block
	var block Block
	for _, b := range blocks {
		if string(b.Key) == "event3" {
			block = b
		}
	}

	assert.Equal(t, "event3", string(block.Key))

}
