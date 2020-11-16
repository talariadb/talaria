package multi

import (
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
)

// SubWriter represents the sub-writer
type SubWriter interface {
	Write(key.Key, []byte) error
}

// streamer represents the sub-streamer
type streamer interface {
	Stream(row block.Row) error
	startProcess()
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
			go streamer.startProcess()
		}
	}

	return &Writer{
		writers:   writers,
		streamers: streamers,
	}
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	for _, w := range w.writers {
		if err := w.Write(key, val); err != nil {
			return err
		}
	}

	return nil
}

// StartProcess launches a goroutine to start the infinite loop for streaming
func (w *Writer) startProcess() {
	for _, w := range w.streamers {
		go w.startProcess()
	}

	return
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
