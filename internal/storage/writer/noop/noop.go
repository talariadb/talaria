package noop

import (
	"github.com/kelindar/talaria/internal/encoding/key"
)

// Writer represents a writer that does not do anything.
type Writer struct{}

// New ...
func New() *Writer {
	return &Writer{}
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	return nil
}
