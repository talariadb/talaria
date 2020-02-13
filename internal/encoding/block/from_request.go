// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package block

import (
	"fmt"

	"github.com/grab/talaria/internal/column"
	talaria "github.com/grab/talaria/proto"
)

// FromRequestBy creates a block from a talaria protobuf-encoded request. It
// repartitions the batch by a given partition key at the same time.
func FromRequestBy(request *talaria.IngestRequest, partitionBy string, computed ...*column.Computed) ([]Block, error) {
	switch data := request.GetData().(type) {
	case *talaria.IngestRequest_Batch:
		return FromBatchBy(data.Batch, partitionBy, computed...)
	case *talaria.IngestRequest_Orc:
		return FromOrcBy(data.Orc, partitionBy, computed...)
	case nil: // The field is not set.
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported data type %T", data)
	}
}
