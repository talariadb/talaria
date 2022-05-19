package base

import (
	"context"
	"testing"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/column/computed"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	row := block.Row{
		Values: map[string]interface{}{
			"test": "Hello Talaria",
			"age":  30,
		},
	}

	filter := `function main(row) 
		return row['age'] > 10
	end`
	computedFilter, err := computed.NewComputed("", "", typeof.Bool, filter, nil)
	assert.NoError(t, err)
	enc1, _ := New("json", computedFilter.Value)
	data, err := enc1.Encode(row)

	assert.Equal(t, `{"Values":{"age":30,"test":"Hello Talaria"},"Schema":null}`, string(data))
	assert.NoError(t, err)

	filter2 := `function main(row)
		return row['age'] < 10
	end`
	loader2 := script.NewLoader(nil)
	enc2, _ := New(filter2, "json", loader2)

	filtered, err := enc2.Filter(row)
	assert.Nil(t, err)

	assert.Nil(t, filtered)
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	w, _ := New("json", nil)
	w.Process = func(context.Context) error {
		return nil
	}
	w.Run(ctx)
	_, err := w.task.Outcome()
	assert.NoError(t, err)
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	w, _ := New("json", nil)
	w.Process = func(context.Context) error {
		time.Sleep(1 * time.Second)
		return nil
	}
	w.Run(ctx)
	w.Close()
	state := w.task.State()
	assert.Equal(t, async.IsCancelled, state)
}

func TestEmptyFilter(t *testing.T) {
	enc1, err := New("json", nil)
	assert.NotNil(t, enc1)
	assert.NoError(t, err)
}
