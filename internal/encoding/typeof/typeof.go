// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/scritchley/orc"
)

// The types of the columns supported
const (
	Unsupported = Type(iota)
	Int32
	Int64
	Float64
	String
	Bool
	Timestamp
	JSON
)

var (
	reflectOfInt32     = reflect.TypeOf(int32(0))
	reflectOfInt64     = reflect.TypeOf(int64(0))
	reflectOfFloat64   = reflect.TypeOf(float64(0))
	reflectOfString    = reflect.TypeOf("")
	reflectOfBool      = reflect.TypeOf(true)
	reflectOfTimestamp = reflect.TypeOf(time.Unix(0, 0))
	reflectOfJSON      = reflect.TypeOf(json.RawMessage(nil))
)

// --------------------------------------------------------------------------------------------------

// Type represents the type enum
type Type byte

// List of supported types, from the list below:
/*
	0:  "BOOLEAN",
	1:  "BYTE",
	2:  "SHORT",
	3:  "INT",
	4:  "LONG",
	5:  "FLOAT",
	6:  "DOUBLE",
	7:  "STRING",
	8:  "BINARY",
	9:  "TIMESTAMP",
	10: "LIST",
	11: "MAP",
	12: "STRUCT",
	13: "UNION",
	14: "DECIMAL",
	15: "DATE",
	16: "VARCHAR",
	17: "CHAR",
*/
var supported = map[string]Type{
	"BOOLEAN":   Bool,
	"INT":       Int32,
	"LONG":      Int64,
	"DOUBLE":    Float64,
	"STRING":    String,
	"TIMESTAMP": Timestamp,
	"VARCHAR":   String,
}

// FromOrc maps the orc type description to our type.
func FromOrc(desc *orc.TypeDescription) (Type, bool) {
	kind := desc.Type().GetKind().String()
	t, ok := supported[kind]
	return t, ok
}

// Reflect returns the corresponding reflect.Type
func (t Type) Reflect() reflect.Type {
	switch t {
	case Int32:
		return reflectOfInt32
	case Int64:
		return reflectOfInt64
	case Float64:
		return reflectOfFloat64
	case String:
		return reflectOfString
	case Bool:
		return reflectOfBool
	case Timestamp:
		return reflectOfTimestamp
	case JSON:
		return reflectOfJSON
	}
	return nil
}

// Category returns the corresponding orc.Category
func (t Type) Category() orc.Category {
	switch t {
	case Int32:
		return orc.CategoryInt
	case Int64:
		return orc.CategoryLong
	case Float64:
		return orc.CategoryDouble
	case String:
		return orc.CategoryString
	case Bool:
		return orc.CategoryBoolean
	case Timestamp:
		return orc.CategoryTimestamp
	case JSON:
		return orc.CategoryString
	}
	panic(fmt.Errorf("typeof: unsupported type %v", t))
}