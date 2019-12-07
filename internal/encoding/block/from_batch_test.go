// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"testing"

	talaria "github.com/grab/talaria/proto"
	"github.com/stretchr/testify/assert"
)

var testBatch = &talaria.Batch{
	Strings: map[uint32][]byte{
		1: []byte("a"),
		2: []byte("b"),
		3: []byte("c"),
		4: []byte("d"),
		5: []byte("event1"),
		6: []byte("event2"),
		7: []byte("hello"),
	},
	Events: []*talaria.Event{
		{Value: map[uint32]*talaria.Value{
			1: &talaria.Value{Value: &talaria.Value_Int{Int: 10}},
			2: &talaria.Value{Value: &talaria.Value_Int{Int: 20}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 5}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_Int{Int: 20}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 5}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_String_{String_: 7}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 6}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_String_{String_: 7}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 6}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_String_{String_: 7}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 6}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_String_{String_: 7}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 6}},
		}},
		{Value: map[uint32]*talaria.Value{
			2: &talaria.Value{Value: &talaria.Value_String_{String_: 7}},
			4: &talaria.Value{Value: &talaria.Value_String_{String_: 6}},
		}},
	},
}

func TestBlock_FromBatch(t *testing.T) {
	blocks, err := FromBatchBy(testBatch, "d")
	assert.NoError(t, err)
	assert.Len(t, blocks, 2) // Number of partitions
}
