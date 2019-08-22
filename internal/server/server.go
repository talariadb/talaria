// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"io"
	"reflect"
	"sync/atomic"
	"time"

	"gitlab.myteksi.net/grab-x/talaria/internal/block"
	"gitlab.myteksi.net/grab-x/talaria/internal/config"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor"
	"gitlab.myteksi.net/grab-x/talaria/internal/orc"
	"gitlab.myteksi.net/grab-x/talaria/internal/presto"
)

const (
	ctxTag  = "server"
	errTag  = "error"
	funcTag = "func"
)

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
}

// Storage represents an eventlog storage contract.
type Storage interface {
	io.Closer
	Append(key, value []byte, ttl time.Duration) error
	Range(seek, until []byte, f func(key, value []byte) bool) error
}

// ------------------------------------------------------------------------------------------------------------

// New creates a new talaria server.
func New(port int32, cluster Membership, store Storage, prestoCfg *config.PrestoConfig, storageCfg *config.StorageConfig, monitor monitor.Client) *Server {
	return &Server{
		port:       port,
		cluster:    cluster,
		store:      store,
		prestoCfg:  prestoCfg,
		storageCfg: storageCfg,
		monitor:    monitor,
	}
}

// Server represents the talaria server which should implement presto thrift interface.
type Server struct {
	port       int32        // The port number to use
	cluster    Membership   // The membership list to use
	store      Storage      // The storage to use
	schema     atomic.Value // The latest schema
	prestoCfg  *config.PrestoConfig
	storageCfg *config.StorageConfig
	monitor    monitor.Client
}

// Schema gets the latest ingested schema.
func (s *Server) Schema() map[string]reflect.Type {
	v := s.schema.Load()
	if v != nil {
		if schema, ok := v.(map[string]reflect.Type); ok {
			return schema
		}
	}
	return map[string]reflect.Type{}
}

// Listen starts listening on presto RPC.
func (s *Server) Listen(ctx context.Context) error {
	return presto.Serve(ctx, s.port, s)
}

// Close closes the server and related resources.
func (s *Server) Close() {
	_ = s.store.Close()
}

// PrestoGetIndexSplits returns a batch of index splits for the given batch of keys.
func (s *Server) PrestoGetIndexSplits(schemaTableName *presto.PrestoThriftSchemaTableName, indexColumnNames []string, outputColumnNames []string, keys *presto.PrestoThriftPageResult, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	return nil, nil
}

// PrestoGetSplits returns a batch of splits.
func (s *Server) PrestoGetSplits(schemaTableName *presto.PrestoThriftSchemaTableName, desiredColumns *presto.PrestoThriftNullableColumnSet, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (batch *presto.PrestoThriftSplitBatch, err error) {
	const tag = "PrestoGetSplits"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	s.monitor.Info(tag, "[schemaTable:%s][desiredColumns:%v][outputConstraint:%+v][maxSplit:%d][nextToken:%+v]", schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken)
	// Create a new query and validate it
	queries, err := parseThriftDomain(outputConstraint)
	if err != nil {
		s.monitor.Count1(ctxTag, errTag, "tag:parse_domain")
		return nil, err
	}
	s.monitor.Info(tag, "[queries:%+v]", queries)

	// We need to generate as many splits as we have nodes in our cluster. Each split needs to contain the IP address of the
	// node containing that split, so Presto can reach it and request the data.
	batch = new(presto.PrestoThriftSplitBatch)
	for _, m := range s.cluster.Members() {
		for _, q := range queries {
			batch.Splits = append(batch.Splits, &presto.PrestoThriftSplit{
				SplitId: q.Encode(),
				Hosts:   []*presto.PrestoThriftHostAddress{{Host: m, Port: s.port}},
			})
		}
	}
	s.monitor.Info(tag, "[splits:%+v]", batch.Splits)
	return
}

// PrestoGetTableMetadata returns metadata for a given table.
func (s *Server) PrestoGetTableMetadata(schemaTableName *presto.PrestoThriftSchemaTableName) (*presto.PrestoThriftNullableTableMetadata, error) {
	const tag = "PrestoGetTableMetadata"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	s.monitor.Info(tag, "[schemaTable:%s]", schemaTableName)
	var columns []*presto.PrestoThriftColumnMetadata
	for k, v := range s.Schema() {
		columns = append(columns, &presto.PrestoThriftColumnMetadata{
			Name: k,
			Type: toSQLType(v),
		})
	}

	// Prepare metadata result
	return &presto.PrestoThriftNullableTableMetadata{
		TableMetadata: &presto.PrestoThriftTableMetadata{
			SchemaTableName: &presto.PrestoThriftSchemaTableName{SchemaName: s.prestoCfg.Schema, TableName: s.prestoCfg.Table},
			Columns:         columns,
		},
	}, nil
}

// PrestoListSchemaNames returns available schema names.
func (s *Server) PrestoListSchemaNames() ([]string, error) {
	const tag = "PrestoListSchemaNames"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	s.monitor.Info(tag, "[schema:%s]", s.prestoCfg.Schema)
	return []string{s.prestoCfg.Schema}, nil
}

// PrestoListTables returns tables for the given schema name.
func (s *Server) PrestoListTables(schemaNameOrNull *presto.PrestoThriftNullableSchemaName) ([]*presto.PrestoThriftSchemaTableName, error) {
	const tag = "PrestoListTables"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	return []*presto.PrestoThriftSchemaTableName{
		{SchemaName: s.prestoCfg.Schema, TableName: s.prestoCfg.Table},
	}, nil
}

// Append appends a set of events to the storage. It needs ingestion time and an event name to create
// a key which will then be used for retrieval.
func (s *Server) Append(payload []byte) error {
	const tag = "Append"
	const chunks = 25000
	const column = "event"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	// Split the incoming payload in chunks of 10K rows
	schema, err := orc.SplitByColumn(payload, column, func(event string, columnChunk []byte) bool {
		_, splitErr := orc.SplitBySize(columnChunk, chunks, func(chunk []byte) bool {

			// Get the first event and tsi from the sub-file
			v, err := orc.First(chunk, "tsi")
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
			_ = s.store.Append(
				newKey(event, time.Unix(0, tsi)),
				buffer,
				time.Second*time.Duration(s.storageCfg.TTLInSec),
			)
			return false
		})
		return splitErr != nil
	})
	if err != nil {
		s.monitor.ErrorWithStats(tag, "split_by_column", "[error:%s] split failed", err)
		return err
	}

	// Store the latest schema
	s.schema.Store(schema)
	return nil
}

// PrestoGetRows returns a batch of rows for the given split.
func (s *Server) PrestoGetRows(splitID *presto.PrestoThriftId, columns []string, maxBytes int64, nextToken *presto.PrestoThriftNullableToken) (result *presto.PrestoThriftPageResult, err error) {
	const tag = "PrestoGetRows"
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:"+tag)

	s.monitor.Info(tag, "[splitID:%s][columns:%v][maxBytes:%d][nextToken:%+v]", splitID, columns, maxBytes, nextToken)
	// Create a set of appenders to use
	var blocks []presto.Column
	for _, c := range columns {
		schema := s.Schema()
		if kind, hasType := schema[c]; hasType {
			appender, ok := presto.NewColumn(kind)
			if !ok {
				s.monitor.ErrorWithStats(ctxTag, "schema_mismatch", "no such column")
				return nil, errSchemaMismatch
			}

			blocks = append(blocks, appender)
		}
	}

	// Parse the incoming query
	query, err := decodeQuery(splitID, nextToken)
	if err != nil {
		s.monitor.ErrorWithStats(ctxTag, "decode_query", "[error:%s] decoding query failed", err)
		return nil, err
	}

	// Prepare the result response
	result = new(presto.PrestoThriftPageResult)
	frames := map[string]presto.Columns{}

	// Range through the keys in our data store
	bytesLeft := int(maxBytes)
	if err = s.store.Range(query.Begin, query.Until, func(key, value []byte) bool {

		// Read the data frame from the specified offset
		frame, readError := s.readDataFrame(columns, value, bytesLeft)

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
		s.monitor.WarnWithStats(ctxTag, "range_key", "[error:%s] range through the key failed", err)
		return
	}

	// Merge columns together at once, reducing allocations
	for i, columnName := range columns {
		blocks[i].AppendBlock(frames[columnName]...)
	}

	// Return the result set to
	for _, b := range blocks {
		result.ColumnBlocks = append(result.ColumnBlocks, b.AsBlock())
		result.RowCount = int32(b.Count())
	}

	s.monitor.Info(tag, "[result:%+v]", result)
	return
}

// ReadDataFrame reads a column data frame and returns the set of columns requested.
func (s *Server) readDataFrame(columns []string, buffer []byte, maxBytes int) (presto.Columns, error) {
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
