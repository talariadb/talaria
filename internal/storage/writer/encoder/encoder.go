package encoder

import (
	"encoding/json"
	"fmt"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
)

// Func encodes the payload
type Func func(interface{}) ([]byte, error)

// Writer is to filter and encode row of events
type Writer struct {
	filter string
	name   string
	encode Func
}

// New creates a new encoder
func New(filter, encoderFunc string, loader *script.Loader) (*Writer, error) {
	if encoderFunc == "" {
		encoderFunc = "json"
	}

	// Extendable encoder functions
	var encoder Func
	switch encoderFunc {
	case "json":
		encoder = Func(json.Marshal)
	default:
		return nil, errors.Newf("encoder: unsupported encoder '%s'", encoderFunc)
	}

	// TODO: load a LUA-based encoder

	return newWithEncoder(encoderFunc, filter, encoder)
}

// newWithEncoder will generate a new encoder for a writer
func newWithEncoder(name, filter string, encoder Func) (*Writer, error) {
	if encoder == nil {
		encoder = Func(json.Marshal)
	}

	return &Writer{
		name:   name,
		filter: filter,
		encode: encoder,
	}, nil
}

// Encode will encode a row to the format the user specifies
func (w *Writer) Encode(input interface{}) ([]byte, error) {

	// TODO: make this work for block.Block and block.Row

	// If it's a row, take the value map
	if r, ok := input.(block.Row); ok {
		input = r.Values
	}

	// Double check to NOT do any copies when putting in a byte slice
	dataString, err := w.encode(input)
	if err != nil {
		return nil, errors.Internal(fmt.Sprintf("encoder: could not marshal to %s", w.name), err)
	}
	return dataString, nil
}
