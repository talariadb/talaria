// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/storage"
)

// Writer represents a sink for the flusher.
type Writer interface {
	Write(key key.Key, blocks []block.Block) error
}

// Flusher represents a flusher/merger.
type Flusher struct {
	monitor      monitor.Monitor // The monitor client
	writer       Writer          // The underlying block writer
	fileNameFunc func(map[string]interface{}) (string, error)
	streamer     storage.Streamer // The underlying row writer
}

// ForCompaction creates a new storage implementation.
func ForCompaction(monitor monitor.Monitor, writer Writer, fileNameFunc func(map[string]interface{}) (string, error)) (*Flusher, error) {

	return &Flusher{
		monitor:      monitor,
		writer:       writer,
		fileNameFunc: fileNameFunc,
	}, nil
}

// TODO: ForStreaming

// WriteBlock writes a one or multiple blocks to the underlying writer.
func (s *Flusher) WriteBlock(blocks []block.Block, schema typeof.Schema) error {
	if s.writer == nil || len(blocks) == 0 {
		return nil
	}

	// Merge the blocks based on the specified merging function
	// buffer, err := s.merge(blocks, schema)
	// if err != nil {
	//     return err
	// }

	// Generate the file name and write the data to the underlying writer
	return s.writer.Write(s.generateFileName(blocks[0]), blocks)
}

// WriteRow writes a single row to the underlying writer (i.e. streamer).
func (s *Flusher) WriteRow(r block.Row) error {
	if s.streamer == nil {
		return nil
	}

	// Stream the row
	return s.streamer.Stream(r)
}

func (s *Flusher) generateFileName(b block.Block) []byte {
	row, err := b.LastRow()
	if err != nil {
		return []byte{}
	}
	output, err := s.fileNameFunc(row)
	if err != nil {
		s.monitor.Error(err)
		return []byte{}
	}

	return []byte(output)
}

// Close is used to gracefully close storage.
func (s *Flusher) Close() error {
	return nil
}
