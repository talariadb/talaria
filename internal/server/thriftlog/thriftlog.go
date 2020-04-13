// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package thriftlog

import (
	"encoding/json"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/presto"
)

// Service represents a PrestoThriftService with logging of request/response.
type Service struct {
	Service presto.PrestoThriftService
	Monitor monitor.Monitor
}

// Request information with additional data
type requestPrestoGetIndexSplits struct {
	SchemaTableName   *presto.PrestoThriftSchemaTableName `json:"schemaTableName,omitempty"`
	IndexColumnNames  []string                            `json:"indexColumnNames,omitempty"`
	OutputColumnNames []string                            `json:"outputColumnNames,omitempty"`
	Keys              *presto.PrestoThriftPageResult      `json:"keys,omitempty"`
	OutputConstraint  *presto.PrestoThriftTupleDomain     `json:"outputConstraint,omitempty"`
	MaxSplitCount     int32                               `json:"maxSplitCount,omitempty"`
	NextToken         *presto.PrestoThriftNullableToken   `json:"nextToken,omitempty"`
}

// PrestoGetIndexSplits returns a batch of index splits for the given batch of keys.
func (s *Service) PrestoGetIndexSplits(schemaTableName *presto.PrestoThriftSchemaTableName, indexColumnNames []string, outputColumnNames []string, keys *presto.PrestoThriftPageResult, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	resp, err := s.Service.PrestoGetIndexSplits(schemaTableName, indexColumnNames, outputColumnNames, keys, outputConstraint, maxSplitCount, nextToken)
	s.trace("PrestoGetIndexSplits", &requestPrestoGetIndexSplits{
		schemaTableName, indexColumnNames, outputColumnNames, keys, outputConstraint, maxSplitCount, nextToken,
	}, resp, err)
	return resp, err
}

// Request information with additional data
type requestPrestoGetSplits struct {
	SchemaTableName  *presto.PrestoThriftSchemaTableName   `json:"schemaTableName,omitempty"`
	DesiredColumns   *presto.PrestoThriftNullableColumnSet `json:"desiredColumns,omitempty"`
	OutputConstraint *presto.PrestoThriftTupleDomain       `json:"outputConstraint,omitempty"`
	MaxSplitCount    int32                                 `json:"maxSplitCount,omitempty"`
	NextToken        *presto.PrestoThriftNullableToken     `json:"nextToken,omitempty"`
}

// PrestoGetSplits returns a batch of splits.
func (s *Service) PrestoGetSplits(schemaTableName *presto.PrestoThriftSchemaTableName, desiredColumns *presto.PrestoThriftNullableColumnSet, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	resp, err := s.Service.PrestoGetSplits(schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken)
	s.trace("PrestoGetSplits", &requestPrestoGetSplits{
		schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken,
	}, resp, err)
	return resp, err
}

// Request information with additional data
type requestPrestoGetRows struct {
	SplitID   *presto.PrestoThriftId            `json:"splitID,omitempty"`
	Columns   []string                          `json:"columns,omitempty"`
	MaxBytes  int64                             `json:"maxBytes,omitempty"`
	NextToken *presto.PrestoThriftNullableToken `json:"nextToken,omitempty"`
}

// Response with details of a the response, but without the entire body
type responsePrestoGetRows struct {
	*presto.PrestoThriftPageResult
	ColumnBlocks []columnBlock `json:"columnBlocks"` // Hide the data
	TotalSize    int           `json:"totalSize"`    // Estimated total size
}

// columnBlock is used to print debug information about a block
type columnBlock struct {
	Type  typeof.Type `json:"type,omitempty"`
	Size  int         `json:"size,omitempty"`
	Count int         `json:"count,omitempty"`
}

// PrestoGetRows returns a batch of rows for the given split.
func (s *Service) PrestoGetRows(splitID *presto.PrestoThriftId, columns []string, maxBytes int64, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftPageResult, error) {
	resp, err := s.Service.PrestoGetRows(splitID, columns, maxBytes, nextToken)
	meta := &responsePrestoGetRows{
		PrestoThriftPageResult: resp,
	}

	// Copy column blocks without the data
	if resp != nil {
		for _, b := range resp.ColumnBlocks {
			meta.TotalSize += b.Size()
			meta.ColumnBlocks = append(meta.ColumnBlocks, columnBlock{
				Type:  b.Type(),
				Size:  b.Size(),
				Count: b.Count(),
			})
		}
	}

	s.trace("PrestoGetRows", &requestPrestoGetRows{
		splitID, columns, maxBytes, nextToken,
	}, meta, err) // Note: don't log the full response, otherwise the logger will die
	return resp, err
}

// PrestoGetTableMetadata returns metadata for a given table.
func (s *Service) PrestoGetTableMetadata(schemaTableName *presto.PrestoThriftSchemaTableName) (*presto.PrestoThriftNullableTableMetadata, error) {
	resp, err := s.Service.PrestoGetTableMetadata(schemaTableName)
	s.trace("PrestoGetTableMetadata", schemaTableName, resp, err)
	return resp, err
}

// PrestoListSchemaNames returns available schema names.
func (s *Service) PrestoListSchemaNames() ([]string, error) {
	resp, err := s.Service.PrestoListSchemaNames()
	s.trace("PrestoListSchemaNames", nil, resp, err)
	return resp, err
}

// PrestoListTables returns tables for the given schema name.
func (s *Service) PrestoListTables(schemaNameOrNull *presto.PrestoThriftNullableSchemaName) ([]*presto.PrestoThriftSchemaTableName, error) {
	resp, err := s.Service.PrestoListTables(schemaNameOrNull)
	s.trace("PrestoListTables", schemaNameOrNull, resp, err)
	return resp, err
}

// ------------------------------------------------------------------------------------------------------------

type target struct {
	Func     string      `json:"func,omitempty"`
	Request  interface{} `json:"request,omitempty"`
	Response interface{} `json:"response,omitempty"`
	Error    error       `json:"error,omitempty"`
}

// trace logs a single request/response in a JSON format
func (s *Service) trace(function string, request, response interface{}, responseErr error) {
	if responseErr != nil {
		s.Monitor.Error(responseErr)
	}

	if r, err := json.Marshal(&target{
		Func:     function,
		Request:  request,
		Response: response,
		Error:    responseErr,
	}); err == nil {
		s.Monitor.Debug("%v", string(r))
	}
}
