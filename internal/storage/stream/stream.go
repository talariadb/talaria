package stream

import (
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/storage"
)

// applyFunc applies a transformation on a row and returns a new row
type applyFunc = func(block.Row) (block.Row, error)

// Publish will publish the row to the topic
func Publish(streamer storage.Streamer, monitor monitor.Monitor) applyFunc {
	return func(r block.Row) (block.Row, error) {
		err := streamer.Stream(r)
		if err != nil {
			monitor.Error(err)
			return r, err
		}
		return r, nil
	}
}
