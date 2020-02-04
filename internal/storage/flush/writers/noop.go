package writers

import (
	"github.com/grab/talaria/internal/encoding/key"
)

type noopWriter struct{}

// NewNoop ...
func NewNoop() Writer {
	return &noopWriter{}
}

func (w *noopWriter) Write(key key.Key, val []byte) error {
	return nil
}