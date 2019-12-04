// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries

import (
	"io"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/grab/talaria/internal/block"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/orc"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/table"
)

const (
	ctxTag  = "server"
	errTag  = "error"
	funcTag = "func"
)

// Assert the contract
var _ table.Table = new(Table)

// Storage represents an underlying storage layer.
type Storage interface {
	io.Closer
	Append(key, value []byte, ttl time.Duration) error
	Range(seek, until []byte, f func(key, value []byte) bool) error
}

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
}

// Table represents a timeseries table.
type Table struct {
	name       string         // The name of the table
	keyColumn  string         // The name of the key column
	timeColumn string         // The name of the time column
	ttl        time.Duration  // The default TTL
	store      Storage        // The storage to use
	schema     atomic.Value   // The latest schema
	cluster    Membership     // The membership list to use
	monitor    monitor.Client // The monitoring client
}

// New creates a new table implementation.
func New(name, keyColumn, timeColumn string, ttl time.Duration, store Storage, cluster Membership, monitor monitor.Client) *Table {
	return &Table{
		name:       name,
		store:      store,
		keyColumn:  keyColumn,
		timeColumn: timeColumn,
		ttl:        ttl,
		cluster:    cluster,
		monitor:    monitor,
	}
}

// Close implements io.Closer interface.
func (t *Table) Close() error {
	return t.store.Close()
}

// Name returns the name of the table.
func (t *Table) Name() string {
	return t.name
}

// Schema retrieves the metadata for the table
func (t *Table) Schema() (map[string]reflect.Type, error) {
	return t.getSchema(), nil
}

// GetSplits retrieves the splits
func (t *Table) GetSplits(desiredColumns []string, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int) ([]table.Split, error) {

	// Create a new query and validate it
	queries, err := parseThriftDomain(outputConstraint, t.keyColumn, t.timeColumn)
	if err != nil {
		t.monitor.Count1(ctxTag, errTag, "tag:parse_domain")
		return nil, err
	}

	// We need to generate as many splits as we have nodes in our cluster. Each split needs to contain the IP address of the
	// node containing that split, so Presto can reach it and request the data.
	splits := make([]table.Split, 0, 16)
	for _, m := range t.cluster.Members() {
		for _, q := range queries {
			splits = append(splits, table.Split{
				Key:   q.Encode(),
				Addrs: []string{m},
			})
		}
	}
	return splits, nil
}

// GetRows retrieves the data
func (t *Table) GetRows(splitID []byte, columns []string, maxBytes int64) (result *table.PageResult, err error) {
	result = &table.PageResult{
		Columns: make([]presto.Column, 0, len(columns)),
	}

	// Create a set of appenders to use
	for _, c := range columns {
		schema := t.getSchema()
		if kind, hasType := schema[c]; hasType {
			column, ok := presto.NewColumn(kind)
			if !ok {
				t.monitor.ErrorWithStats(ctxTag, "schema_mismatch", "no such column")
				return nil, errSchemaMismatch
			}

			result.Columns = append(result.Columns, column)
		}
	}

	// Parse the incoming query
	query, err := decodeQuery(splitID)
	if err != nil {
		t.monitor.ErrorWithStats(ctxTag, "decode_query", "[error:%s] decoding query failed", err)
		return nil, err
	}

	// Range through the keys in our data store
	bytesLeft := int(maxBytes)
	frames := map[string]presto.Columns{}
	if err = t.store.Range(query.Begin, query.Until, func(key, value []byte) bool {

		// Read the data frame from the specified offset
		frame, readError := t.readDataFrame(columns, value, bytesLeft)

		// Set the next token if we don't have enough to process
		if readError == io.ErrShortBuffer {
			query.Begin = key // Continue from the current key (at 0 offset)
			result.NextToken = query.Encode()
			return true
		}

		// Append each column to the map (we'll merge later)
		for i, columnName := range columns {
			frames[columnName] = append(frames[columnName], frame[i])
		}

		bytesLeft -= frame.Size()
		return readError != io.EOF
	}); err != nil {
		t.monitor.WarnWithStats(ctxTag, "range_key", "[error:%s] range through the key failed", err)
		return
	}

	// Merge columns together at once, reducing allocations
	for i, columnName := range columns {
		result.Columns[i].AppendBlock(frames[columnName]...)
	}

	return
}

// ReadDataFrame reads a column data frame and returns the set of columns requested.
func (t *Table) readDataFrame(columns []string, buffer []byte, maxBytes int) (presto.Columns, error) {
	blk, err := block.FromBuffer(buffer)
	if err != nil {
		return nil, err
	}

	res, err := blk.Select(columns...)
	if err != nil {
		return nil, err
	}

	// First check if we have enough space
	var result presto.Columns
	for _, columnName := range columns {
		result = append(result, res[columnName])

	}

	// If we don't have enough space, skip this data frame and stop here
	if result.Size() > maxBytes {
		return nil, io.ErrShortBuffer
	}

	return result, io.EOF
}

// Append appends a set of events to the storage. It needs ingestion time and an event name to create
// a key which will then be used for retrieval.
func (t *Table) Append(payload []byte) error {
	const tag = "Append"
	const chunks = 25000
	keyColumn, timeColumn := t.keyColumn, t.timeColumn

	// Split the incoming payload in chunks of 10K rows
	defer t.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)
	schema, err := orc.SplitByColumn(payload, keyColumn, func(keyColumnValue string, columnChunk []byte) bool {
		_, splitErr := orc.SplitBySize(columnChunk, chunks, func(chunk []byte) bool {

			// Get the first event and tsi from the sub-file
			v, err := orc.First(chunk, timeColumn)
			if err != nil {
				return true
			}

			// We must have event and tsi to proceed...
			tsi, hasTsi := v[0].(int64)
			if !hasTsi {
				return true
			}

			// Create a new block to store from orc buffer
			blk, err := block.FromOrc(chunk)
			if err != nil {
				return true
			}

			// Encode the block
			buffer, err := blk.Encode()
			if err != nil {
				return true
			}

			// Append this block to the store
			_ = t.store.Append(newKey(keyColumnValue, time.Unix(0, tsi)), buffer, t.ttl)
			return false
		})
		return splitErr != nil
	})
	if err != nil {
		t.monitor.ErrorWithStats(tag, "split_by_column", "[error:%s] split failed", err)
		return err
	}

	// Store the latest schema
	t.schema.Store(schema)
	return nil
}

// getSchema gets the latest ingested schema.
func (t *Table) getSchema() map[string]reflect.Type {
	v := t.schema.Load()
	if v != nil {
		if schema, ok := v.(map[string]reflect.Type); ok {
			return schema
		}
	}
	return map[string]reflect.Type{}
}
