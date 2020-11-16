// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"encoding/json"
	"testing"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	script "github.com/kelindar/talaria/internal/scripting"
	talaria "github.com/kelindar/talaria/proto"
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
		11: []byte("1234"),
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
			1: {Value: &talaria.Value_String_{String_: 11}}, // Int as string
			2: {Value: &talaria.Value_Time{Time: 1585549847}},
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
	dataColumn, err := newDataColumn()
	assert.NoError(t, err)

	// The schema to filter
	filter := typeof.Schema{
		"a":       typeof.Int64,
		"b":       typeof.Timestamp,
		"d":       typeof.String,
		"data":    typeof.JSON,
		"another": typeof.Int64,
	}

	// Create blocks
	apply := Transform(&filter, dataColumn)
	blocks, err := FromBatchBy(testBatch, "d", &filter, apply)
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

	// Select all of the columns
	columns, err := block.Select(block.Schema())
	assert.NoError(t, err)
	assert.Equal(t, `[{"column":"a","type":"BIGINT"},{"column":"another","type":"BIGINT"},{"column":"b","type":"TIMESTAMP"},{"column":"d","type":"VARCHAR"},{"column":"data","type":"JSON"}]`, block.Schema().String())

	// Get the last row
	row := columns.LastRow()
	assert.Equal(t, int64(1585549847000), row["b"].(int64)) // Note: Presto Thrift time is in Unix Milliseconds
	assert.Equal(t, `event3`, row["d"].(string))
	assert.Contains(t, string(row["data"].(json.RawMessage)), "event3")
}

func newDataColumn() (column.Computed, error) {
	return column.NewComputed("data", typeof.JSON, `
	local json = require("json")

	function main(input)
		return json.encode(input)
	end`, script.NewLoader(nil))
}
