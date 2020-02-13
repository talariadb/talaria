// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/grab/talaria/internal/encoding/key"
	"github.com/grab/talaria/internal/presto"
	"github.com/kelindar/binary"
)

var (
	errInvalidDomain = errors.New("your query must contain 'event' constraint")
)

// ------------------------------------------------------------------------------------------------------------

// Query represents a serialized query object.
type query struct {
	Begin  []byte // The first key of the range
	Until  []byte // The last key of the range
	Offset int64  // The last offset of the file we need to process
}

// Encode creates a split ID by encoding a query.
func (q *query) Encode() []byte {
	b, err := binary.Marshal(q)
	if err != nil {
		panic(err)
	}

	return b
}

// NewQuery creates a new query
func newQuery(keyColumn string, from, until time.Time) query {
	t0 := key.New(keyColumn, from)
	t1 := key.New(keyColumn, until)
	return query{
		Begin: t0[0:12],
		Until: t1[0:12],
	}
}

// decodeQuery unmarshals split ID back to a query.
func decodeQuery(splitKey []byte) (out *query, err error) {
	out = new(query)
	err = binary.Unmarshal(splitKey, out)
	return
}

// parseThriftDomain creates a set of queries from the presto constraint
func parseThriftDomain(req *presto.PrestoThriftTupleDomain, hashKey, sortKey string) ([]query, error) {
	if req.Domains == nil {
		return nil, fmt.Errorf("your query must contain Presto thrift domains")
	}

	// Retrieve necessary constraints
	if len(hashKey) == 0 {
		return parseWithSort(req, sortKey)
	}

	return parseWithHashAndSort(req, hashKey, sortKey)
}

// parse the request given hashKey is present, and also check whether the bound of sortKey (time) is provided
func parseWithHashAndSort(req *presto.PrestoThriftTupleDomain, hashKey, sortKey string) ([]query, error) {
	keyColumnDomain, hasEvent := req.Domains[hashKey]
	if !hasEvent || keyColumnDomain.ValueSet == nil || keyColumnDomain.ValueSet.RangeValueSet == nil {
		return nil, fmt.Errorf("your query must contain '%s' constraint", hashKey)
	}

	// Get time bounds
	from := time.Unix(0, 0)
	until := time.Unix(math.MaxInt64, 0)
	if tsi, hasTsi := req.Domains[sortKey]; hasTsi && tsi.ValueSet != nil && tsi.ValueSet.RangeValueSet != nil {
		if len(keyColumnDomain.ValueSet.RangeValueSet.Ranges) == 1 {
			if t0, t1, ok := tsi.ValueSet.RangeValueSet.Ranges[0].AsTimeRange(); ok {
				from = t0
				until = t1
			}
		}
	}

	// Iterate through all of the ranges
	var queries []query
	for _, r := range keyColumnDomain.ValueSet.RangeValueSet.Ranges {
		if r.Low.Bound == presto.PrestoThriftBoundExactly && r.High.Bound == presto.PrestoThriftBoundExactly {
			if value := r.Low.Value.VarcharData; value != nil && len(value.Sizes) == 1 {
				queries = append(queries, newQuery(string(value.Bytes), from, until))
			}
			if value := r.Low.Value.BigintData; value != nil && len(value.Longs) > 0 {
				queries = append(queries, newQuery(strconv.FormatInt(value.Longs[0], 10), from, until))
			}
		}
	}
	return queries, nil
}

// parse the request given hashKey is not present, check whether the bound of sortKey (time) is provided
func parseWithSort(req *presto.PrestoThriftTupleDomain, sortKey string) ([]query, error) {
	// Get time bounds
	from := time.Unix(0, 0)
	until := time.Unix(math.MaxInt64, 0)
	if tsi, hasTsi := req.Domains[sortKey]; hasTsi && tsi.ValueSet != nil && tsi.ValueSet.RangeValueSet != nil {
		if t0, t1, ok := tsi.ValueSet.RangeValueSet.Ranges[0].AsTimeRange(); ok {
			from = t0
			until = t1
		}
	}

	return []query{newQuery("", from, until)}, nil
}
