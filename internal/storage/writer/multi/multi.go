package multi

import (
	"context"

	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"golang.org/x/sync/errgroup"
)

// SubWriter represents the sub-writer
type SubWriter interface {
	Write(key.Key, []byte) error
}

// streamer represents the sub-streamer
type streamer interface {
	Stream(row block.Row) error
	Run(ctx context.Context) (async.Task, error)
}

// Writer represents a writer that writes into multiple sub-writers.
type Writer struct {
	writers   []SubWriter
	streamers []streamer
}

// New ...
func New(writers ...SubWriter) *Writer {
	streamers := make([]streamer, 0, len(writers))
	for _, v := range writers {
		if streamer, ok := v.(streamer); ok {
			streamers = append(streamers, streamer)
		}
	}

	return &Writer{
		writers:   writers,
		streamers: streamers,
	}
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	eg := new(errgroup.Group)
	for _, w := range w.writers {
		w := w
		eg.Go(func() error {
			return w.Write(key, val)
		})
	}
	// Wait blocks until all finished, and returns the first non-nil error (if any) from them
	return eg.Wait()
}

// Run launches the asynchronous infinite loop for streamers to start streaming data
func (w *Writer) Run(ctx context.Context) (async.Task, error) {
	for _, w := range w.streamers {
		_, err := w.Run(ctx)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Stream streams the data to the sink
func (w *Writer) Stream(row block.Row) error {
	for _, w := range w.streamers {
		// When a stream fails, the following streams will not be run
		if err := w.Stream(row); err != nil {
			return err
		}
	}

	return nil
}
