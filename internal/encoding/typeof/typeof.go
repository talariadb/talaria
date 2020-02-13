// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/crphang/orc"
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
	"LIST":      JSON,
	"MAP":       JSON,
	"STRUCT":    JSON,
}

// FromOrc maps the orc type description to our type.
func FromOrc(desc *orc.TypeDescription) (Type, bool) {
	kind := desc.Type().GetKind().String()
	t, ok := supported[kind]
	return t, ok
}

// FromType gets the type from a reflect.Type
func FromType(rt reflect.Type) (Type, bool) {
	switch rt.Name() {
	case "int32":
		return Int32, true
	case "int64":
		return Int64, true
	case "float64":
		return Float64, true
	case "string":
		return String, true
	case "bool":
		return Bool, true
	case "Time":
		return Timestamp, true
	case "RawMessage":
		return JSON, true
	}

	return Unsupported, false
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

	panic(fmt.Errorf("typeof: orc type for %v is not found", t))
}

// SQL converts reflect type to SQL type
func (t Type) SQL() string {
	switch t {
	case Int32:
		return "INTEGER"
	case Int64:
		return "BIGINT"
	case Float64:
		return "DOUBLE"
	case String:
		return "VARCHAR"
	case Bool:
		return "BOOLEAN"
	case Timestamp:
		return "TIMESTAMP"
	case JSON:
		return "JSON"
	}

	panic(fmt.Errorf("typeof: sql type for %v is not found", t))
}

// String returns the name of the type
func (t Type) String() string {
	switch t {
	case Int32:
		return "int32"
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	case String:
		return "string"
	case Bool:
		return "bool"
	case Timestamp:
		return "timestamp"
	case JSON:
		return "json"
	default:
		return "unsupported"
	}
}

// UnmarshalText unmarshals the type from text
func (t *Type) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	default:
		*t = Unsupported
	case "int32", "integer", "uint32":
		*t = Int32
	case "int64", "bigint", "long", "uint64":
		*t = Int64
	case "float64", "double":
		*t = Float64
	case "string", "text", "varchar":
		*t = String
	case "bool", "boolean":
		*t = Bool
	case "timestamp", "time":
		*t = Timestamp
	case "json", "map":
		*t = JSON
	}
	return nil
}

// MarshalText encodes the type to text
func (t *Type) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// UnmarshalJSON decodes the json-encoded type
func (t *Type) UnmarshalJSON(b []byte) error {
	var text string
	if err := json.Unmarshal(b, &text); err != nil {
		return err
	}

	return t.UnmarshalText([]byte(text))
}

// MarshalJSON encodes the type to JSON
func (t Type) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
