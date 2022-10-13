// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"fmt"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/storage/stream"
	"github.com/kelindar/talaria/internal/table"
	talaria "github.com/kelindar/talaria/proto"
)

// applyFunc applies a transformation on a row and returns a new row
type applyFunc = func(block.Row) (block.Row, error)

const ingestErrorKey = "ingest.error"

// Ingest implements ingress.IngressServer
func (s *Server) Ingest(ctx context.Context, request *talaria.IngestRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()

	// Iterate through all of the appenders and append the blocks to them
	for _, t := range s.tables {

		// check whether current table implement appender interface
		appender, ok := t.(table.Appender)
		if !ok {
			continue
		}

		// Set the filter only if the schema is static
		var filter *typeof.Schema
		if schema, static := t.Schema(); static {
			filter = &schema
		}

		// Functions to be applied
		funcs := []applyFunc{block.Transform(filter, s.computed...)}

		// Partition the request for the table
		blocks, err := block.FromRequestBy(request, appender.HashBy(), filter, funcs...)
		if err != nil {
			s.monitor.Count1(ctxTag, ingestErrorKey, "type:convert")
			return nil, errors.Internal("unable to read the block", err)
		}

		// If table supports streaming, then stream
		if streamer, ok := t.(storage.Streamer); ok {
			s := stream.Publish(streamer, s.monitor)

			for _, each := range blocks {
				rows, err := block.FromBlockBy(each, each.Schema())
				if err != nil {
					return nil, err
				}
				for _, row := range rows {
					s(row)
				}
			}
		}

		// Append all of the blocks
		for _, block := range blocks {
			if err := appender.Append(block); err != nil {
				s.monitor.Count1(ctxTag, ingestErrorKey, "type:append")
				return nil, err
			}
		}

		s.monitor.Count("server", fmt.Sprintf("%s.ingest.count", t.Name()), int64(len(blocks)))
	}

	return nil, nil
}

func (s *Server) IngestWithTable(ctx context.Context, request *talaria.IngestWithTableRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()

	tableMap := make(map[string]string, len(request.Tables))
	for _, each := range request.Tables {
		tableMap[each] = each
	}

	// Iterate through all of the appenders and append the blocks to them
	for _, t := range s.tables {

		// check whether the current table is present in the requested tables
		_, exist := tableMap[t.Name()]

		// check whether current table implement appender interface
		appender, ok := t.(table.Appender)
		if !ok || !exist {
			continue
		}

		// Set the filter only if the schema is static
		var filter *typeof.Schema
		if schema, static := t.Schema(); static {
			filter = &schema
		}

		// Functions to be applied
		funcs := []applyFunc{block.Transform(filter, s.computed...)}

		// Partition the request for the table
		blocks, err := block.FromRequestWithTable(request, appender.HashBy(), filter, funcs...)
		if err != nil {
			s.monitor.Count1(ctxTag, ingestErrorKey, "type:convert")
			return nil, errors.Internal("unable to read the block", err)
		}

		// If table supports streaming, then stream
		if streamer, ok := t.(storage.Streamer); ok {
			s := stream.Publish(streamer, s.monitor)

			for _, each := range blocks {
				rows, err := block.FromBlockBy(each, each.Schema())
				if err != nil {
					return nil, err
				}
				for _, row := range rows {
					s(row)
				}
			}
		}

		// Append all of the blocks
		for _, block := range blocks {
			if err := appender.Append(block); err != nil {
				s.monitor.Count1(ctxTag, ingestErrorKey, "type:append")
				return nil, err
			}
		}

		s.monitor.Count("server", fmt.Sprintf("%s.ingestWithTable.count", t.Name()), int64(len(blocks)))
	}

	return nil, nil
}
