// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries

import (
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table"
)

const (
	ctxTag  = "timeseries"
	errTag  = "error"
	funcTag = "func"
)

// Assert the contract
var _ table.Table = new(Table)

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
}

// Table represents a timeseries table.
type Table struct {
	name       string          // The name of the table
	keyColumn  string          // The name of the key column
	timeColumn string          // The name of the time column
	ttl        time.Duration   // The default TTL
	store      storage.Storage // The storage to use
	schema     atomic.Value    // The latest schema
	cluster    Membership      // The membership list to use
	monitor    monitor.Client  // The monitoring client
}

// New creates a new table implementation.
func New(name string, cfg *config.Storage, dataDir string, cluster Membership, monitor monitor.Client) *Table {
	store := disk.New(monitor)
	tableDir := path.Join(dataDir, name)
	err := store.Open(tableDir)
	if err != nil {
		panic(err)
	}

	return &Table{
		name:       name,
		store:      store,
		keyColumn:  cfg.KeyColumn,
		timeColumn: cfg.TimeColumn,
		ttl:        time.Duration(cfg.TTLInSec) * time.Second,
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
func (t *Table) Schema() (typeof.Schema, error) {
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
func (t *Table) GetRows(splitID []byte, requestedColumns []string, maxBytes int64) (result *table.PageResult, err error) {
	result = &table.PageResult{
		Columns: make([]presto.Column, 0, len(requestedColumns)),
	}

	// Create a set of appenders to use
	tableSchema := t.getSchema()
	localSchema := make(typeof.Schema, len(requestedColumns))
	for _, c := range requestedColumns {
		if typ, hasType := tableSchema[c]; hasType {
			localSchema[c] = typ
			continue
		}

		return nil, fmt.Errorf("timeseries: table %s does not contain column %s", t.Name(), c)
	}

	// Parse the incoming query
	query, err := decodeQuery(splitID)
	if err != nil {
		t.monitor.ErrorWithStats(ctxTag, "decode_query", "[error:%s] decoding query failed", err)
		return nil, err
	}

	// Range through the keys in our data store
	bytesLeft := int(maxBytes)
	frames := make(map[string][]presto.Column, len(requestedColumns))
	if err = t.store.Range(query.Begin, query.Until, func(key, value []byte) bool {

		// Read the data frame from the specified offset
		frame, readError := t.readDataFrame(localSchema, value, bytesLeft)

		// Set the next token if we don't have enough to process
		if readError == io.ErrShortBuffer {
			query.Begin = key // Continue from the current key (at 0 offset)
			result.NextToken = query.Encode()
			return true
		}

		// Append each column to the map (we'll merge later)
		for _, columnName := range requestedColumns {
			f := frame[columnName]
			frames[columnName] = append(frames[columnName], f)
		}

		bytesLeft -= frame.Size()
		return readError != io.EOF
	}); err != nil {
		t.monitor.WarnWithStats(ctxTag, "range_key", "[error:%s] range through the key failed", err)
		return
	}

	// Merge columns together at once, reducing allocations
	result.Columns = make([]presto.Column, 0, len(requestedColumns))
	for _, columnName := range requestedColumns {
		column := presto.NewColumn(localSchema[columnName])
		column.AppendBlock(frames[columnName])
		result.Columns = append(result.Columns, column)
	}

	return
}

// ReadDataFrame reads a column data frame and returns the set of columns requested.
func (t *Table) readDataFrame(schema typeof.Schema, buffer []byte, maxBytes int) (presto.NamedColumns, error) {
	result, err := block.Read(buffer, schema)
	if err != nil {
		return nil, err
	}

	// If we don't have enough space, skip this data frame and stop here
	if result.Size() > maxBytes {
		return nil, io.ErrShortBuffer
	}

	return result, io.EOF
}

// Append appends a block to the store.
func (t *Table) Append(block block.Block) error {

	// Get the min timestamp of the block
	ts, hasTs := block.Min(t.timeColumn)
	if !hasTs || ts < 0 {
		ts = 0
	}

	// Encode the block
	buffer, err := block.Encode()
	if err != nil {
		return err
	}

	// Store the latest schema
	t.schema.Store(block.Schema())

	// Append the block to the store
	return t.store.Append(key.New(string(block.Key), time.Unix(0, ts)), buffer, t.ttl)
}

// getSchema gets the latest ingested schema.
func (t *Table) getSchema() typeof.Schema {
	v := t.schema.Load()
	if v != nil {
		if schema, ok := v.(typeof.Schema); ok {
			return schema
		}
	}
	return typeof.Schema{}
}
