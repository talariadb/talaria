package base

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Func encodes the payload
type Func func(interface{}) ([]byte, error)

// FilterFunc used for filter
type FilterFunc func(map[string]interface{}) (interface{}, error)

// Writer is to filter and encode row of events
type Writer struct {
	task    async.Task
	Process func(context.Context) error
	filter  FilterFunc
	name    string
	encode  Func
}

// New creates a new encoder
func New(encoderFunc string, filter FilterFunc) (*Writer, error) {
	if encoderFunc == "" {
		encoderFunc = "json"
	}
	if filter == nil {
		filter = func(map[string]interface{}) (interface{}, error) {
			return true, nil
		}
	}

	// Extendable encoder functions
	var encoder Func
	switch encoderFunc {
	case "json":
		encoder = Func(json.Marshal)
	default:
		return nil, errors.Newf("encoder: unsupported encoder '%s'", encoderFunc)
	}

	return newWithEncoder(encoderFunc, filter, encoder)
}

// newWithEncoder will generate a new encoder for a writer
func newWithEncoder(name string, filter FilterFunc, encoder Func) (*Writer, error) {
	if encoder == nil {
		encoder = Func(json.Marshal)
	}

	return &Writer{
		name:   name,
		filter: filter,
		encode: encoder,
	}, nil
}

// Run task to process work using async invoke
func (w *Writer) Run(ctx context.Context) (async.Task, error) {
	if w.Process == nil {
		return nil, errors.New("base: no process defined for this stream")
	}
	w.task = async.Invoke(ctx, func(innerctx context.Context) (interface{}, error) {
		return nil, w.Process(innerctx)
	})
	return w.task, nil
}

// Close invoked task
func (w *Writer) Close() error {
	if w.task != nil {
		w.task.Cancel()
	}
	return nil
}

// Encode will encode a row to the format the user specifies
func (w *Writer) Encode(input interface{}) ([]byte, error) {
	// TODO: make this work for block.Block and block.Row

	// If it's a row, take the value map
	if r, ok := input.(block.Row); ok {
		if w.applyFilter(&r) {
			input = r.Values
		} else {
			return nil, nil
		}
	}

	encodedData, err := w.encode(input)
	if err != nil {
		return nil, errors.Internal(fmt.Sprintf("encoder: could not marshal to %s", w.name), err)
	}
	return encodedData, nil
}

// applyFilter filters out a row if needed
func (w *Writer) applyFilter(row *block.Row) bool {
	if w.filter == nil {
		return true
	}

	// Runs the lua script
	out, err := w.filter(row.Values)
	if err != nil || !out.(bool) {
		return false
	}
	return true
}
