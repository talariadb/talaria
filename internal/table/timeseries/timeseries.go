// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/loader"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/presto"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/table"
	"gopkg.in/yaml.v2"
)

const (
	ctxTag = "timeseries"
	errTag = "error"
)

// Assert the contracts
var _ table.Table = new(Table)
var _ table.Appender = new(Table)

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
}

// Table represents a timeseries table.
type Table struct {
	name         string           // The name of the table
	hashBy       string           // The name of the key column
	sortBy       string           // The name of the time column
	ttl          time.Duration    // The default TTL
	store        storage.Storage  // The storage to use
	schema       atomic.Value     // The latest schema
	loader       *loader.Loader   // The loader used to watch schema updates
	cluster      Membership       // The membership list to use
	monitor      monitor.Monitor  // The monitoring client
	staticSchema *typeof.Schema   // The static schema of the timeseries table
	stream       storage.Streamer // The streams that a table has
}

// New creates a new table implementation.
func New(name string, cluster Membership, monitor monitor.Monitor, store storage.Storage, cfg *config.Table, stream storage.Streamer) *Table {
	t := &Table{
		name:    name,
		store:   store,
		hashBy:  cfg.HashBy,
		sortBy:  cfg.SortBy,
		ttl:     time.Duration(cfg.TTL) * time.Second,
		cluster: cluster,
		monitor: monitor,
		loader:  loader.New(),
		stream:  stream,
	}

	t.staticSchema = t.loadStaticSchema(cfg.Schema)
	return t
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
func (t *Table) Schema() (typeof.Schema, bool) {
	return t.getSchema(), t.staticSchema != nil
}

// HashBy returns the column by which the table should be hashed.
func (t *Table) HashBy() string {
	return t.hashBy
}

// SortBy returns the column by which the table should be sorted.
func (t *Table) SortBy() string {
	return t.sortBy
}

// Stream will stream the row and return errors if any
func (t *Table) Stream(row block.Row) error {
	return t.stream.Stream(row)
}

func (t *Table) loadStaticSchema(uriOrSchema string) *typeof.Schema {
	staticSchema := &typeof.Schema{}

	if _, err := url.Parse(uriOrSchema); err != nil {
		if err := yaml.Unmarshal([]byte(uriOrSchema), &staticSchema); err != nil { // Assumes it is inline schema schema
			t.monitor.Warning(errors.Internal("error parsing static schema", err))
			return nil
		}

		return staticSchema
	}

	// Start watching on the URL
	updates := t.loader.Watch(context.Background(), uriOrSchema, 5*time.Minute)
	u := <-updates

	// Check if given uri has error
	if u.Err != nil {
		t.monitor.Warning(errors.Internal("error reading from uri", u.Err))
		return nil
	}

	// Check if given schema is malformed
	err := yaml.Unmarshal(u.Data, &staticSchema)
	if err != nil {
		return nil
	}

	// Read the updates asynchronously
	async.Invoke(context.Background(), func(ctx context.Context) (interface{}, error) {
		for u := range updates {
			if u.Err != nil {
				t.monitor.Warning(errors.Internal("error reading from uri", u.Err))
				continue
			}
			staticSchema := &typeof.Schema{}
			err := yaml.Unmarshal(u.Data, staticSchema)
			if err != nil {
				t.monitor.Warning(errors.Internal("error parsing static schema", err))
				continue
			}
			t.staticSchema = staticSchema
		}
		return nil, nil
	})

	return staticSchema
}

// GetSplits retrieves the splits
func (t *Table) GetSplits(desiredColumns []string, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int) ([]table.Split, error) {

	// Create a new query and validate it
	queries, err := parseThriftDomain(outputConstraint, t.hashBy, t.sortBy)
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
		t.monitor.Error(errors.Internal("decoding query failed", err))
		return nil, err
	}

	// Range through the keys in our data store
	bytesLeft := int(float64(maxBytes) * 0.95) // Leave 5% buffer in case we estimating the size poorly
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

		// Skip empty frames, should not happen most of the time
		if frame.Size() == 0 {
			return false // Ignore
		}

		// Append each column to the map (we'll merge later)
		for _, columnName := range requestedColumns {
			f := frame[columnName]
			frames[columnName] = append(frames[columnName], f)
		}

		bytesLeft -= frame.Size()
		return readError != io.EOF
	}); err != nil {
		t.monitor.Warning(errors.Internal("range through the key failed", err))
		return
	}

	// Merge columns together at once, reducing allocations
	result.Columns = make([]presto.Column, 0, len(requestedColumns))
	for _, columnName := range requestedColumns {
		column := column.NewColumn(localSchema[columnName])
		column.AppendBlock(frames[columnName])
		result.Columns = append(result.Columns, column)
	}

	return
}

// ReadDataFrame reads a column data frame and returns the set of columns requested.
func (t *Table) readDataFrame(schema typeof.Schema, buffer []byte, maxBytes int) (column.Columns, error) {
	result, err := block.Read(buffer, schema)
	if err != nil {
		return nil, errors.Internal("block read failed", err)
	}

	// Log the data frame read
	t.monitor.Debug("reading a data frame size=%v bytesLeft=%v", result.Size(), maxBytes)

	// If we don't have enough space, skip this data frame and stop here
	if result.Size() >= maxBytes {
		return nil, io.ErrShortBuffer
	}

	return result, io.EOF
}

// Append appends a block to the store.
func (t *Table) Append(block block.Block) error {

	// Get the min timestamp of the block
	ts, hasTs := block.Min(t.sortBy)
	if !hasTs || ts < 0 {
		ts = 0
	}

	// Encode the block
	block.Expires = time.Now().Add(t.ttl).Unix()
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
	if s := t.staticSchema; s != nil && len(*s) > 0 {
		return *s
	}

	if s := t.schema.Load(); s != nil {
		if schema, ok := s.(typeof.Schema); ok {
			return schema
		}
	}

	return typeof.Schema{}
}
