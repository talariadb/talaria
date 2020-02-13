// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"time"

	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/presto"
)

// PrestoGetIndexSplits returns a batch of index splits for the given batch of keys.
func (s *Server) PrestoGetIndexSplits(schemaTableName *presto.PrestoThriftSchemaTableName, indexColumnNames []string, outputColumnNames []string, keys *presto.PrestoThriftPageResult, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	return nil, nil
}

// PrestoGetSplits returns a batch of splits.
func (s *Server) PrestoGetSplits(schemaTableName *presto.PrestoThriftSchemaTableName, desiredColumns *presto.PrestoThriftNullableColumnSet, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_splits")

	// Retrieve the table
	table, err := s.getTable(schemaTableName.TableName)
	if err != nil {
		return nil, err
	}

	// Retrieve desired columns
	var columns []string
	if desiredColumns != nil && desiredColumns.Columns != nil {
		for column := range desiredColumns.Columns {
			columns = append(columns, column)
		}
	}

	// Get the splits
	splits, err := table.GetSplits(columns, outputConstraint, int(maxSplitCount))
	if err != nil {
		return nil, err
	}

	// Convert the response to Presto response
	batch := new(presto.PrestoThriftSplitBatch)
	for _, split := range splits {
		tsplit := &presto.PrestoThriftSplit{
			SplitId: encodeThriftID(table.Name(), []byte(split.Key)),
			Hosts:   make([]*presto.PrestoThriftHostAddress, 0, len(split.Addrs)),
		}

		for _, addr := range split.Addrs {
			tsplit.Hosts = append(tsplit.Hosts, &presto.PrestoThriftHostAddress{Host: addr, Port: s.conf().Readers.Presto.Port})
		}
		batch.Splits = append(batch.Splits, tsplit)
	}
	return batch, nil
}

// PrestoGetTableMetadata returns metadata for a given table.
func (s *Server) PrestoGetTableMetadata(schemaTableName *presto.PrestoThriftSchemaTableName) (*presto.PrestoThriftNullableTableMetadata, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_table_metadata")

	// Retrieve the table
	table, err := s.getTable(schemaTableName.TableName)
	if err != nil {
		return nil, err
	}

	// Load the schema
	schema, err := table.Schema()
	if err != nil {
		return nil, err
	}

	// Convert to SQL types
	var columns []*presto.PrestoThriftColumnMetadata
	for k, v := range schema {
		columns = append(columns, &presto.PrestoThriftColumnMetadata{
			Name: k,
			Type: v.SQL(),
		})
	}

	// Prepare metadata result
	return &presto.PrestoThriftNullableTableMetadata{
		TableMetadata: &presto.PrestoThriftTableMetadata{
			SchemaTableName: &presto.PrestoThriftSchemaTableName{SchemaName: s.conf().Readers.Presto.Schema, TableName: table.Name()},
			Columns:         columns,
		},
	}, nil
}

// PrestoListSchemaNames returns available schema names.
func (s *Server) PrestoListSchemaNames() ([]string, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_schemas")

	return []string{s.conf().Readers.Presto.Schema}, nil
}

// PrestoListTables returns tables for the given schema name.
func (s *Server) PrestoListTables(schemaNameOrNull *presto.PrestoThriftNullableSchemaName) ([]*presto.PrestoThriftSchemaTableName, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_tables")

	// Return all of the tables configured in the server
	tables := make([]*presto.PrestoThriftSchemaTableName, 0, len(s.tables))
	for _, table := range s.tables {
		tables = append(tables, &presto.PrestoThriftSchemaTableName{
			SchemaName: s.conf().Readers.Presto.Schema,
			TableName:  table.Name(),
		})
	}
	return tables, nil
}

// PrestoGetRows returns a batch of rows for the given split.
func (s *Server) PrestoGetRows(splitID *presto.PrestoThriftId, columns []string, maxBytes int64, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftPageResult, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_rows")

	// Parse the incoming thriftID
	id, err := decodeThriftID(splitID, nextToken)
	if err != nil {
		return nil, errors.Internal("decoding query failed", err)
	}

	// Retrieve the table
	table, err := s.getTable(id.Table)
	if err != nil {
		return nil, errors.Internal("unable to retrieve a table", err)
	}

	// Retrieve the rows for the table
	result := new(presto.PrestoThriftPageResult)
	page, err := table.GetRows(id.Split, columns, maxBytes)
	if err != nil {
		return nil, errors.Internal("unable to get rows from a table", err)
	}

	// If a page has a token, we need to create a split to continue iterating
	if page.NextToken != nil {
		result.NextToken = encodeThriftID(table.Name(), page.NextToken)
	}

	// Return the result set
	for _, b := range page.Columns {
		result.ColumnBlocks = append(result.ColumnBlocks, b.AsThrift())
		result.RowCount = int32(b.Count())
	}
	return result, nil
}
