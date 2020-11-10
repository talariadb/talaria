package multi

import (
	"github.com/kelindar/talaria/internal/encoding/key"
)

// SubWriter represents the sub-writer
type SubWriter interface {
	Write(key.Key, []byte) error
}

// streamer represents the sub-streamer
type streamer interface {
	Stream(row *map[string]interface{}) error
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
	for _, w := range w.writers {
		if err := w.Write(key, val); err != nil {
			return err
		}
	}

	return nil
}

// Stream streams the data to the sink
func (w *Writer) Stream(row *map[string]interface{}) error {
	for _, w := range w.streamers {
		if err := w.Stream(row); err != nil {
			return err
		}
	}

	return nil
}
