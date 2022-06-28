package console

import (
	"fmt"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
)

type Writer struct {
	*base.Writer
}

func New(filter, encoding string, monitor monitor.Monitor) (*Writer, error) {
	baseWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, errors.Newf("file: %v", err)
	}
	return &Writer{
		Writer: baseWriter,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	rows := make([]block.Row, 0)
	for _, each := range blocks {
		row, err := block.FromBlockBy(each, each.Schema())
		if err != nil {
			return err
		}
		rows = append(rows, row...)
	}
	for _, each := range rows {
		m, err := w.Writer.Encode(each)
		if err != nil {
			return err
		}
		fmt.Println(string(m))
	}
	return nil
}

// Stream streams the data to the sink
func (w *Writer) Stream(row block.Row) error {
	return nil
}
