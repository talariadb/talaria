package base

import (
	"context"
	"fmt"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/column/computed"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/merge"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// FilterFunc used for filter
type FilterFunc func(map[string]interface{}) (interface{}, error)

// Writer is to filter and encode row of events
type Writer struct {
	task    async.Task
	Process func(context.Context) error
	filter  FilterFunc
	name    string
	encoder map[string]merge.Func
}

// New creates a new encoder
func New(filter, encoderFunc string, monitor monitor.Monitor) (*Writer, error) {
	var filterF FilterFunc = nil
	if filter != "" {
		computed, err := computed.NewComputed("", "", typeof.Bool, filter, monitor)
		if err != nil {
			return nil, err
		}
		filterF = computed.Value
	}

	// Extendable encoder functions
	encoder, err := merge.New(encoderFunc)
	if err != nil {
		return nil, err
	}

	return newWithEncoder(encoderFunc, filterF, encoder)
}

// newWithEncoder will generate a new encoder for a writer
func newWithEncoder(name string, filter FilterFunc, encoder map[string]merge.Func) (*Writer, error) {
	return &Writer{
		name:    name,
		filter:  filter,
		encoder: encoder,
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

// Encode will encode a row/block to the format the user specifies
func (w *Writer) Encode(input interface{}) ([]byte, error) {
	// TODO: make this work for block.Block and block.Row

	encoderType := "block"
	if _, ok := input.(block.Row); ok {
		encoderType = "row"
	} else {
		if len(input.([]block.Block)) == 0 {
			return []byte{}, nil
		}
	}

	encodedData, err := w.encoder[encoderType](input)
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

// Filter filters out a row/blocks
func (w *Writer) Filter(input interface{}) (interface{}, error) {
	// If it's a row, take the value map
	if r, ok := input.(block.Row); ok {
		if w.applyFilter(&r) {
			return input, nil
		}
	}
	// If it's a slice of rows, applyFilter for each row
	if rows, ok := input.([]block.Row); ok {
		filtered := make([]block.Row, 0)
		for _, row := range rows {
			if w.applyFilter(&row) {
				filtered = append(filtered, row)
			}
		}
		return filtered, nil
	}
	return nil, nil
}
