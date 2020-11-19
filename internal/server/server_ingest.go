// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"fmt"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/stream"
	"github.com/kelindar/talaria/internal/table"
	talaria "github.com/kelindar/talaria/proto"
)

const ingestErrorKey = "ingest.error"

// Ingest implements ingress.IngressServer
func (s *Server) Ingest(ctx context.Context, request *talaria.IngestRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()

	// Iterate through all of the appenders and append the blocks to them
	for _, t := range s.tables {
		appender, ok := t.(table.Appender)
		if !ok {
			continue
		}

		// Set the filter only if the schema is static
		var filter *typeof.Schema
		if schema, static := t.Schema(); static {
			filter = &schema
		}

		// Partition the request for the table
		blocks, err := block.FromRequestBy(request, appender.HashBy(), filter, block.Transform(filter, s.computed...), stream.Publish(t.Streams, s.monitor))
		if err != nil {
			s.monitor.Count1(ctxTag, ingestErrorKey, "type:convert")
			return nil, errors.Internal("unable to read the block", err)
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
