package multi

import (
	"context"
	"testing"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
)

type MockWriter func(key key.Key, val []byte) error

func (w MockWriter) Write(key key.Key, val []byte) error {
	return w(key, val)
}

type MockWriterFull struct {
	Count int
}

func (w *MockWriterFull) Write(key key.Key, val []byte) error {
	w.Count++
	return nil
}

func (w *MockWriterFull) Stream(block.Row) error {
	w.Count++
	return nil
}

func (w *MockWriterFull) Run(ctx context.Context) (async.Task, error) {
	return nil, nil
}

func TestMulti(t *testing.T) {
	var count int
	sub := MockWriter(func(key key.Key, val []byte) error {
		count++
		return nil
	})

	multiWriter := New(sub, sub, sub)
	assert.NoError(t, multiWriter.Write(nil, nil))
	assert.Equal(t, 3, count)

	mock1 := MockWriterFull{Count: 0}
	mock2 := MockWriterFull{Count: 5}

	multiWriter2 := New(&mock1, &mock2)
	multiWriter2.Run(context.Background())
	res := multiWriter2.Stream(block.Row{})

	assert.NoError(t, res)
	assert.Equal(t, 1, mock1.Count)
	assert.Equal(t, 6, mock2.Count)
}
