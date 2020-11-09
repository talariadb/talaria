// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"fmt"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/storage/stream"
	talaria "github.com/kelindar/talaria/proto"
)

// FromRequestBy creates a block from a talaria protobuf-encoded request. It
// repartitions the batch by a given partition key at the same time.
func FromRequestBy(request *talaria.IngestRequest, partitionBy string, filter *typeof.Schema, streams stream.Streamer, computed ...column.Computed) ([]Block, error) {
	switch data := request.GetData().(type) {
	case *talaria.IngestRequest_Batch:
		return FromBatchBy(data.Batch, partitionBy, filter, streams, computed...)
	case *talaria.IngestRequest_Orc:
		return FromOrcBy(data.Orc, partitionBy, filter, computed...)
	case *talaria.IngestRequest_Csv:
		return FromCSVBy(data.Csv, partitionBy, filter, computed...)
	case *talaria.IngestRequest_Url:
		return FromURLBy(data.Url, partitionBy, filter, computed...)
	case nil: // The field is not set.
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported data type %T", data)
	}
}
