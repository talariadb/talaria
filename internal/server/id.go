// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"github.com/grab/talaria/internal/presto"
	"github.com/kelindar/binary"
)

// SplitID represents a split key along with the table name
type SplitID struct {
	Table string // The name of the table
	Split []byte // The encoded split key
}

// encodeThriftID creates a split ID by encoding a query.
func encodeThriftID(table string, split []byte) *presto.PrestoThriftId {
	return &presto.PrestoThriftId{
		Id: encodeID(table, split),
	}
}

// decodeThriftID unmarshals thrift ID back to a struct.
func decodeThriftID(id *presto.PrestoThriftId, token *presto.PrestoThriftNullableToken) (out *SplitID, err error) {
	if token.Token != nil {
		id = token.Token
	}

	out = new(SplitID)
	err = binary.Unmarshal(id.Id, out)
	return
}

// encodeID creates a split ID by encoding a query.
func encodeID(table string, split []byte) []byte {
	b, err := binary.Marshal(&SplitID{
		Table: table,
		Split: split,
	})
	if err != nil {
		panic(err)
	}

	return b
}

// decodeID unmarshals thrift ID back to a struct.
func decodeID(id, token []byte) (out *SplitID, err error) {
	if token != nil {
		id = token
	}

	out = new(SplitID)
	err = binary.Unmarshal(id, out)
	return
}
