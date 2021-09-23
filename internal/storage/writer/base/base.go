package base

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/merge"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
)

// Writer is to filter and encode row of events
type Writer struct {
	task    async.Task
	Process func(context.Context) error
	filter  *lua.Script
	name    string
	encoder map[string]merge.Func
}

// New creates a new encoder
func New(filter, encoderFunc string, loader *script.Loader) (*Writer, error) {
	// Extendable encoder functions
	encoder, err := merge.New(encoderFunc)
	if err != nil {
		return nil, err
	}

	// If no filter was specified, create a base writer without a filter
	if filter == "" {
		return newWithEncoder(encoderFunc, nil, encoder)
	}

	// Load the filter script if required
	script, err := loader.Load(filter, filter)
	if err != nil {
		return nil, err
	}

	return newWithEncoder(encoderFunc, script, encoder)
}

// newWithEncoder will generate a new encoder for a writer
func newWithEncoder(name string, filter *lua.Script, encoder map[string]merge.Func) (*Writer, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Runs the lua script
	out, err := w.filter.Run(ctx, row.Values)
	if err != nil || !out.(lua.Bool) {
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
		} else {
			return nil, nil
		}
	}
	return nil, nil
}

func Setup() ([]block.Block, typeof.Schema) {

	const testFile = "../../../../test/test4.csv"
	o, _ := ioutil.ReadFile(testFile)
	schema := &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}
	apply := block.Transform(schema)
	b, _ := block.FromCSVBy(o, "raisedCurrency", &typeof.Schema{
		"raisedCurrency": typeof.String,
		"raisedAmt":      typeof.Float64,
	}, apply)
	return b, *schema
}
