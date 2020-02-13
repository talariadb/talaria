// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"

	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/table"
	talaria "github.com/grab/talaria/proto"
)

// Ingest implements ingress.IngressServer
func (s *Server) Ingest(ctx context.Context, request *talaria.IngestRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()

	// Read blocks and repartition by the specified key
	blocks, err := block.FromRequestBy(request, s.conf().Tables.Timeseries.HashBy, s.computed...)
	if err != nil {
		return nil, errors.Internal("unable to read the block", err)
	}

	// Iterate through all of the appenders and append the blocks to them
	for _, t := range s.tables {
		if appender, ok := t.(table.Appender); ok {
			for _, block := range blocks {
				if err := appender.Append(block); err != nil {
					return nil, err
				}
			}
		}
	}

	return nil, nil
}
