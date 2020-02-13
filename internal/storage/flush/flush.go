// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"bytes"
	"compress/flate"
	"sync"
	"time"

	eorc "github.com/crphang/orc"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/encoding/orc"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/flush/writers"


)

// Assert contract compliance
var _ storage.Appender = new(Storage)
var _ storage.Merger = new(Storage)

// Storage represents s3/flush storage.
type Storage struct {
	monitor      monitor.Monitor // The monitor client
	writer       writers.Writer
	memoryPool   *sync.Pool
	fileNameFunc func(map[string]interface{}) (string, error)
}

// New creates a new storage implementation.
func New(monitor monitor.Monitor, writer writers.Writer, fileNameFunc func(map[string]interface{}) (string, error)) *Storage {
	memoryPool := &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1<<20))
		},
	}

	return &Storage{
		monitor: monitor,
		writer: writer,
		memoryPool: memoryPool,
		fileNameFunc: fileNameFunc,
	}
}

// Append flushes the merged blocks to S3
func (s *Storage) Append(key key.Key, value []byte, ttl time.Duration) error {
	return s.writer.Write(key, value)
}

// Merge merges multiple blocks together and outputs a key and merged orc data
func (s *Storage) Merge(blocks []block.Block, schema typeof.Schema) ([]byte, []byte) {
	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		s.monitor.Error(errors.Internal("flush: error generating orc schema", err))
		return nil, nil
	}

	buffer := s.memoryPool.Get().(*bytes.Buffer)
	writer, err := eorc.NewWriter(buffer,
		eorc.SetSchema(orcSchema),
		eorc.SetCompression(eorc.CompressionZlib{Level: flate.DefaultCompression}))

	for _, blk := range blocks {
		rows, err := blk.Select(blk.Schema())
		if err != nil {
			continue
		}

		// Fetch columns that is required by the static schema
		cols := make(column.Columns, 16)
		for name, typ := range schema {
			col := rows[name]
			if col.Kind() != typ {
				col = column.NewColumn(typ)
			}

			cols[name] = col
		}

		cols.FillNulls()

		colIterator := []eorc.ColumnIterator{}
		for _, colName := range schema.Columns() {
			colIterator = append(colIterator, cols[colName])
		}

		if err := writer.WriteColumns(colIterator); err != nil {
			s.monitor.Error(errors.Internal("flush: error writing columns", err))
			return nil, nil
		}
	}

	if err := writer.Close(); err != nil {
		s.monitor.Error(errors.Internal("flush: error closing writer", err))
		return nil, nil
	}

	output := buffer.Bytes()
	buffer.Reset()
	s.memoryPool.Put(buffer)

	return s.generateFileName(blocks[0]), output
}

func (s *Storage) generateFileName(b block.Block) []byte {
	row, err := b.LastRow()
	if err != nil {
		return []byte{}
	}
	output, err := s.fileNameFunc(row)
	if err != nil {
		return []byte{}
	}

	return []byte(output)
}

// Close is used to gracefully close storage.
func (s *Storage) Close() error {
	return nil
}
