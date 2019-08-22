// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"gitlab.myteksi.net/grab-x/talaria/internal/presto"
)

var (
	errSchemaMismatch = errors.New("mismatch between internal schema and requested columns")
	errInvalidDomain  = errors.New("your query must contain 'event' constraint")
)

// ------------------------------------------------------------------------------------------------------------

// Next is used as a sequence number, it's okay to overflow.
var next uint32

// NewKey generates a new key for the storage.
func newKey(eventName string, tsi time.Time) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint32(out[0:4], murmur3.Sum32([]byte(eventName)))
	binary.BigEndian.PutUint64(out[4:12], uint64(tsi.Unix()))
	binary.BigEndian.PutUint32(out[12:16], atomic.AddUint32(&next, 1))
	return out
}

// ------------------------------------------------------------------------------------------------------------

// Query represents a serialized query object.
type query struct {
	Begin  []byte `json:"b"`
	Until  []byte `json:"u"`
	Offset int64  `json:"o,omitempty"` // The last offset of the file we need to process
}

// Encode creates a split ID by encoding a query.
func (q *query) Encode() *presto.PrestoThriftId {
	b, err := json.Marshal(q)
	if err != nil {
		panic(err)
	}

	return &presto.PrestoThriftId{
		Id: b,
	}
}

// NewQuery creates a new query
func newQuery(event string, from, until time.Time) query {
	t0 := newKey(event, from)
	t1 := newKey(event, until)
	return query{
		Begin: t0[0:12],
		Until: t1[0:12],
	}
}

// decodeQuery unmarshals split ID back to a query.
func decodeQuery(id *presto.PrestoThriftId, token *presto.PrestoThriftNullableToken) (out *query, err error) {
	if token.Token != nil {
		id = token.Token
	}

	out = new(query)
	err = json.Unmarshal(id.Id, out)
	return
}

// parseThriftDomain creates a set of queries from the presto constraint
func parseThriftDomain(req *presto.PrestoThriftTupleDomain) ([]query, error) {
	if req.Domains == nil {
		return nil, errInvalidDomain
	}

	// Retrieve necessary constraints
	event, hasEvent := req.Domains["event"]
	if !hasEvent || event.ValueSet == nil || event.ValueSet.RangeValueSet == nil {
		return nil, errInvalidDomain
	}

	// Get time bounds
	from := time.Unix(0, 0)
	until := time.Unix(math.MaxInt64, 0)
	if tsi, hasTsi := req.Domains["tsi"]; hasTsi && tsi.ValueSet != nil && tsi.ValueSet.RangeValueSet != nil {
		if len(event.ValueSet.RangeValueSet.Ranges) == 1 {
			if t0, t1, ok := tsi.ValueSet.RangeValueSet.Ranges[0].AsTimeRange(); ok {
				println("time bound", t0.String(), " until ", t1.String())
				from = t0
				until = t1
			}
		}
	}

	// Iterate through all of the ranges
	var queries []query
	for _, r := range event.ValueSet.RangeValueSet.Ranges {
		if r.Low.Bound == presto.PrestoThriftBoundExactly && r.High.Bound == presto.PrestoThriftBoundExactly {
			if value := r.Low.Value.VarcharData; value != nil && len(value.Sizes) == 1 {
				queries = append(queries, newQuery(string(value.Bytes), from, until))
			}
		}
	}

	return queries, nil
}

// ------------------------------------------------------------------------------------------------------------

// Converts reflect type to SQL type
func toSQLType(t reflect.Type) string {
	switch t.Name() {
	case "string":
		return "varchar"
	case "int32":
		return "int"
	case "int64":
		return "bigint"
	case "float64":
		return "double"
	}

	panic(fmt.Errorf("sql type for %v is not found, did you forget to add it", t.Name()))
}
