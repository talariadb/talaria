// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package client

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	pb "github.com/grab/talaria/proto"
)

type encoder struct {
	next       uint32            // The next index for dictionary buffer
	dictionary map[string]uint32 // The header contains the dictionary strings
	batch      *pb.Batch
}

func newEncoder() *encoder {
	return &encoder{
		dictionary: map[string]uint32{},
	}
}

// Encode implements formatter interface
func (e *encoder) Encode(events []Event) *pb.Batch {
	// reset buffer at start
	e.next = 0
	e.dictionary = make(map[string]uint32, len(events))
	e.batch = &pb.Batch{Events: make([]*pb.Event, 0, len(events))}

	for _, ev := range events {
		encoded := e.encodeEvent(ev)
		e.batch.Events = append(e.batch.Events, encoded)
	}

	// Write the interned strings
	e.writeDictionary()

	return e.batch
}

// updateDict maps a string to an integer
func (e *encoder) updateDict(str string) uint32 {
	// Fetch the value we previously dictionary first
	if idx, ok := e.dictionary[str]; ok {
		return idx
	}

	// Increment the index, updateDict and return it
	e.next++
	e.dictionary[str] = e.next
	return e.next
}

// Writes the reference to the string
func (e *encoder) encodeEvent(event Event) *pb.Event {
	res := &pb.Event{
		Value: make(map[uint32]*pb.Value, len(event)),
	}
	for k, v := range event {
		keyRef := e.updateDict(k)
		val, err := e.encodeValue(v)
		if err != nil {
			// should we just stop processing the whole event/batch?
			continue
		}
		res.Value[keyRef] = val
	}
	return res
}

func (e *encoder) writeDictionary() {
	e.batch.Strings = make(map[uint32][]byte, len(e.dictionary))
	for str, ref := range e.dictionary {
		e.batch.Strings[ref] = stringToBinary(str)
	}
}

// encodeValue converts a interface value to a Talaria protobuf value according to its type
func (e *encoder) encodeValue(v interface{}) (*pb.Value, error) {
	switch val := v.(type) {
	case int8:
		return &pb.Value{Value: &pb.Value_Int32{Int32: int32(val)}}, nil
	case int16:
		return &pb.Value{Value: &pb.Value_Int32{Int32: int32(val)}}, nil
	case int32:
		return &pb.Value{Value: &pb.Value_Int32{Int32: val}}, nil
	case uint8:
		return &pb.Value{Value: &pb.Value_Int64{Int64: int64(val)}}, nil
	case uint16:
		return &pb.Value{Value: &pb.Value_Int64{Int64: int64(val)}}, nil
	case uint32:
		return &pb.Value{Value: &pb.Value_Int64{Int64: int64(val)}}, nil
	case int:
		return &pb.Value{Value: &pb.Value_Int64{Int64: int64(val)}}, nil
	case int64:
		return &pb.Value{Value: &pb.Value_Int64{Int64: val}}, nil
	case float32:
		return &pb.Value{Value: &pb.Value_Float64{Float64: float64(val)}}, nil
	case float64:
		return &pb.Value{Value: &pb.Value_Float64{Float64: val}}, nil
	case string:
		valueRef := e.updateDict(val)
		return &pb.Value{Value: &pb.Value_String_{String_: valueRef}}, nil
	case bool:
		return &pb.Value{Value: &pb.Value_Bool{Bool: val}}, nil
	case time.Time:
		return &pb.Value{Value: &pb.Value_Time{Time: val.Unix()}}, nil
	case json.RawMessage:
		valueRef := e.updateDict(string(val))
		return &pb.Value{Value: &pb.Value_Json{Json: valueRef}}, nil
	case nil: // The field is not set.
		return nil, nil
	default:
		valueRef := e.updateDict(fmt.Sprintf("%v", v))
		return &pb.Value{Value: &pb.Value_String_{String_: valueRef}}, nil
	}
}

func stringToBinary(v string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(v)
	byteHeader.Len = l
	byteHeader.Cap = l
	return
}
