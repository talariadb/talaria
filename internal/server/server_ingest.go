// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"context"
	"time"

	"github.com/grab/talaria/internal/table"
	talaria "github.com/grab/talaria/proto"
)

// Ingest implements ingress.IngressServer
func (s *Server) Ingest(ctx context.Context, request *talaria.IngestRequest) (*talaria.IngestResponse, error) {
	defer s.handlePanic()
	const ttl = 10 * time.Minute // TODO: longer

	// Add the time
	//now := time.Now()

	// Read blocks and repartition by the specified key
	/*blocks, err := block.FromRequestBy(request, s.keyColumn)
	if err != nil {
		return nil, err
	}

	for _, block := range blocks {
		b, err := block.Encode()
		if err != nil {
			return nil, err
		}

		if err := s.store.Append(key.New(string(block.Key), now), b, ttl); err != nil {
			return nil, err
		}
	}
	*/
	return nil, nil
}

// Append appends a set of events to the storage. It needs ingestion time and an event name to create
// a key which will then be used for retrieval.
func (s *Server) Append(payload []byte) error {
	defer s.handlePanic()
	for _, t := range s.tables {
		if appender, ok := t.(table.Appender); ok {
			if err := appender.Append(payload); err != nil {
				return err
			}
		}
	}
	return nil
}
