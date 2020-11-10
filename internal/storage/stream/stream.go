package stream

import (
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/storage"
)

// applyFunc applies a transformation on a row and returns a new row
type applyFunc = func(block.Row) block.Row

func Publish(streamer storage.Streamer) applyFunc {
	return func(r block.Row) block.Row {
		streamer.Stream(r)
		// TODO: log the error
		return r
	}
}
