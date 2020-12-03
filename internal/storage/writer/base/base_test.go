package base

import (
	"context"
	"testing"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	script "github.com/kelindar/talaria/internal/scripting"
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
	loader := script.NewLoader(nil)
	enc1, _ := New(filter, "json", loader)
	data, err := enc1.Encode(row)

	assert.Equal(t, `{"age":30,"test":"Hello Talaria"}`, string(data))
	assert.NoError(t, err)

	filter2 := `function main(row)
		return row['age'] < 10
	end`
	loader2 := script.NewLoader(nil)
	enc2, _ := New(filter2, "json", loader2)
	data2, err := enc2.Encode(row)

	assert.Nil(t, data2)
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	loader := script.NewLoader(nil)
	w, _ := New("", "json", loader)
	w.Process = func(context.Context) error {
		return nil
	}
	w.Run(ctx)
	_, err := w.task.Outcome()
	assert.NoError(t, err)
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	loader := script.NewLoader(nil)
	w, _ := New("", "json", loader)
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
	loader := script.NewLoader(nil)
	enc1, err := New("", "json", loader)
	assert.NotNil(t, enc1)
	assert.NoError(t, err)
}
