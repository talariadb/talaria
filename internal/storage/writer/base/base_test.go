package base

import (
	"context"
	"testing"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	row := block.Row{
		Values: map[string]interface{}{
			"test": "Hello Talaria",
			"age":  30,
		},
	}

	filter := `function main(row)
		return row['age'] > 10
	end`
	enc1, _ := New(filter, "json", m)
	data, err := enc1.Encode(row)

	assert.Equal(t, `{"Values":{"age":30,"test":"Hello Talaria"},"Schema":null}`, string(data))
	assert.NoError(t, err)

	filter2 := `function main(row)
		return row['age'] < 10
	end`
	enc2, _ := New(filter2, "json", m)

	filtered, err := enc2.Filter(row)
	assert.Nil(t, err)

	assert.Nil(t, filtered)
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	w, _ := New("", "json", m)
	w.Process = func(context.Context) error {
		return nil
	}
	w.Run(ctx)
	_, err := w.task.Outcome()
	assert.NoError(t, err)
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	w, _ := New("", "json", m)
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
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	enc1, err := New("", "json", m)
	assert.NotNil(t, enc1)
	assert.NoError(t, err)
}
