// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package client

import (
	"encoding/json"
	"testing"
	"time"

	pb "github.com/grab/talaria/proto"
	"github.com/stretchr/testify/assert"
)

type A struct {
	Name string
}

func TestEncoder(t *testing.T) {
	testTime := time.Now()
	testEvents := []Event{
		{
			"event": "abc",
		},
		{
			"value": 123,
		},
		{
			"mutable": true,
		},
		{
			"time": testTime,
		},
		{
			"misc": json.RawMessage("{1:2}"),
		},
		{
			"misc": &A{Name: "test"},
		},
		{
			"event": "xyz",
			"time":  456,
			"color": "yellow",
			"topic": "movie",
		},
	}

	encoder := newEncoder()
	res := encoder.Encode(testEvents)
	assert.Len(t, res.Events, 7)
	assert.Len(t, res.Strings, 13)
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{1: {Value: &pb.Value_String_{String_: 2}}}}, res.Events[0])
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{3: {Value: &pb.Value_Int64{Int64: 123}}}}, res.Events[1])
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{4: {Value: &pb.Value_Bool{Bool: true}}}}, res.Events[2])
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{5: {Value: &pb.Value_Time{Time: testTime.Unix()}}}}, res.Events[3])
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{6: {Value: &pb.Value_Json{Json: 7}}}}, res.Events[4])
	assert.Equal(t, &pb.Event{Value: map[uint32]*pb.Value{6: {Value: &pb.Value_String_{String_: 8}}}}, res.Events[5])
}
