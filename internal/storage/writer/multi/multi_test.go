package multi

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
)

type MockWriter func(key key.Key, blk []block.Block) error

func (w MockWriter) Write(key key.Key, blk []block.Block) error {
	return w(key, blk)
}

type MockWriterFull struct {
	Count int
}

func (w *MockWriterFull) Write(key key.Key, blk []block.Block) error {
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
	var count int64
	sub := MockWriter(func(key key.Key, val []block.Block) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	multiWriter := New(sub, sub, sub)
	assert.NoError(t, multiWriter.Write(nil, nil))
	assert.EqualValues(t, 3, count)

	mock1 := MockWriterFull{Count: 0}
	mock2 := MockWriterFull{Count: 5}

	multiWriter2 := New(&mock1, &mock2)
	multiWriter2.Run(context.Background())
	res := multiWriter2.Stream(block.Row{})

	assert.NoError(t, res)
	assert.Equal(t, 1, mock1.Count)
	assert.Equal(t, 6, mock2.Count)
}
