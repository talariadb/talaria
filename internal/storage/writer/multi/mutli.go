package multi

import (
	"github.com/kelindar/talaria/internal/encoding/key"
)

// SubWriter represents the sub-writer
type SubWriter interface {
	Write(key.Key, []byte) error
}

// Writer represents a writer that writes into multiple sub-writers.
type Writer struct {
	writers []SubWriter
}

// New ...
func New(writers ...SubWriter) *Writer {
	return &Writer{
		writers: writers,
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
