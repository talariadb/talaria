package typeof

import (
	"strconv"
	"time"
)

type types = map[Type]void
type void = struct{}

// orc       block
// string -> json
// json   -> string
// string -> int32
// string -> int64
// string -> timestamp
// string -> bool

var convertible = map[Type]types{
	String: {
		String:    void{},
		JSON:      void{},
		Bool:      void{},
		Int32:     void{},
		Int64:     void{},
		Float64:   void{},
		Timestamp: void{},
	},
	JSON: {
		String: void{},
		JSON:   void{},
	},
}

// Parse attempts to parse the string to a specific type
func Parse(s string, typ Type) (interface{}, bool) {
	switch typ {

	// Happy Path, return the string
	case String, JSON:
		return s, true

	// Try and parse boolean value
	case Bool:
		if v, err := strconv.ParseBool(s); err == nil {
			return v, true
		}

	// Try and parse integer value
	case Int32:
		if v, err := strconv.ParseInt(s, 10, 32); err == nil {
			return int32(v), true
		}

	// Try and parse integer value
	case Int64:
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v, true
		}

	// Try and parse float value
	case Float64:
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v, true
		}

	case Timestamp:
		if v, err := time.Parse(time.RFC3339, s); err == nil {
			return v, true
		}
	}

	return nil, false
}
