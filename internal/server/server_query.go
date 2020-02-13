// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"time"

	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/table"
	talaria "github.com/grab/talaria/proto"
)

// Describe returns the list of schema/table combinations and the metadata
func (s *Server) Describe(ctx context.Context, _ *talaria.DescribeRequest) (*talaria.DescribeResponse, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:describe")

	tables := make([]*talaria.TableMeta, 0, len(s.tables))
	for _, table := range s.tables {
		schema, err := table.Schema()
		if err != nil {
			return nil, err
		}

		// Populate the column metadata
		var columns []*talaria.ColumnMeta
		for k, v := range schema {
			columns = append(columns, &talaria.ColumnMeta{
				Name: k,
				Type: v.SQL(),
			})
		}

		tables = append(tables, &talaria.TableMeta{
			Schema:  s.conf().Readers.Presto.Schema,
			Table:   table.Name(),
			Columns: columns,
		})
	}

	return &talaria.DescribeResponse{
		Tables: tables,
	}, nil
}

// GetSplits returns the list of splits for a particular table/filter combination
func (s *Server) GetSplits(ctx context.Context, request *talaria.GetSplitsRequest) (*talaria.GetSplitsResponse, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_splits")

	// Retrieve the table
	table, err := s.getTable(request.Table)
	if err != nil {
		return nil, err
	}

	// Build the domain
	hashKey, sortKey := s.conf().Tables.Timeseries.HashBy, s.conf().Tables.Timeseries.SortBy
	domain, err := presto.NewDomain(hashKey, sortKey, request.Filters...)
	if err != nil {
		return nil, err
	}

	// Get the splits
	splits, err := table.GetSplits(request.Columns, domain, int(request.MaxSplits))
	if err != nil {
		return nil, err
	}

	// Prepare the response
	response := new(talaria.GetSplitsResponse)
	for _, split := range splits {
		tsplit := talaria.Split{
			SplitID: encodeID(table.Name(), []byte(split.Key)),
			Hosts:   make([]*talaria.Endpoint, 0, len(split.Addrs)),
		}

		for _, addr := range split.Addrs {
			tsplit.Hosts = append(tsplit.Hosts, &talaria.Endpoint{Host: addr, Port: s.conf().Readers.Presto.Port})
		}
		response.Splits = append(response.Splits, &tsplit)
	}

	return response, nil
}

// GetRows returns the rows for a particular split
func (s *Server) GetRows(ctx context.Context, request *talaria.GetRowsRequest) (*talaria.GetRowsResponse, error) {
	defer s.handlePanic()
	defer s.monitor.Duration(ctxTag, funcTag, time.Now(), "func:get_rows")

	// Parse the incoming thriftID
	id, err := decodeID(request.SplitID, request.NextToken)
	if err != nil {
		return nil, errors.Internal("decoding query failed", err)
	}

	// Retrieve the table
	table, err := s.getTable(id.Table)
	if err != nil {
		return nil, errors.Internal("unable to retrieve a table", err)
	}

	// Retrieve the rows for the table
	result := new(talaria.GetRowsResponse)
	page, err := table.GetRows(id.Split, request.Columns, request.MaxBytes)
	if err != nil {
		return nil, errors.Internal("unable to get rows from a table", err)
	}

	// If a page has a token, we need to create a split to continue iterating
	if page.NextToken != nil {
		result.NextToken = encodeID(table.Name(), page.NextToken)
	}

	// Return the result set
	for _, b := range page.Columns {
		result.Columns = append(result.Columns, b.AsProto())
		result.RowCount = int32(b.Count())
	}
	return result, nil
}

// getTable returns the table or errors out
func (s *Server) getTable(name string) (table.Table, error) {
	table, ok := s.tables[name]
	if !ok {
		return nil, errors.Newf("table %s not found", name)
	}
	return table, nil
}
