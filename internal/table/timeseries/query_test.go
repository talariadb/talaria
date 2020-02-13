// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package timeseries

import (
	"testing"

	"github.com/grab/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

func TestSplitCodec(t *testing.T) {
	assert.NotPanics(t, func() {
		q := new(query)
		q.Begin = []byte("ABC")

		id := q.Encode()
		assert.Equal(t, []byte{0x3, 0x41, 0x42, 0x43, 0x0, 0x0}, id)

		out, err := decodeQuery(id)
		assert.NoError(t, err)
		assert.Equal(t, []byte("ABC"), out.Begin)
	})
}

func getColumn(column string) func() string {
	return func() string {
		return column
	}
}

func TestParse(t *testing.T) {
	domain := newSplitQuery("test")
	table := &Table{keyColumn: "_col5", timeColumn: "NA"}
	queries, err := parseThriftDomain(domain, table.keyColumn, table.timeColumn)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)
}

func TestParseWithoutKeyColumn(t *testing.T) {
	domain := newSplitQuery("test")
	table := &Table{keyColumn: "col6", timeColumn: "NA"}
	queries, err := parseThriftDomain(domain, table.keyColumn, table.timeColumn)
	assert.Error(t, err)
	assert.Nil(t, queries)
}

func TestParseKeyColDisabled(t *testing.T) {
	domain := newSplitQuery("test")
	table := &Table{keyColumn: "", timeColumn: "NA"}
	queries, err := parseThriftDomain(domain, table.keyColumn, table.timeColumn)
	assert.Nil(t, err)
	assert.Len(t, queries, 1)
}

func newSplitQuery(eventName string) *presto.PrestoThriftTupleDomain {
	return &presto.PrestoThriftTupleDomain{
		Domains: map[string]*presto.PrestoThriftDomain{
			"_col5": {
				ValueSet: &presto.PrestoThriftValueSet{
					RangeValueSet: &presto.PrestoThriftRangeValueSet{
						Ranges: []*presto.PrestoThriftRange{{
							Low: &presto.PrestoThriftMarker{
								Value: &presto.PrestoThriftBlock{
									VarcharData: &presto.PrestoThriftVarchar{
										Bytes: []byte(eventName),
										Sizes: []int32{int32(len(eventName))},
									},
								},
								Bound: presto.PrestoThriftBoundExactly,
							},
							High: &presto.PrestoThriftMarker{
								Value: &presto.PrestoThriftBlock{
									VarcharData: &presto.PrestoThriftVarchar{
										Bytes: []byte(eventName),
										Sizes: []int32{int32(len(eventName))},
									},
								},
								Bound: presto.PrestoThriftBoundExactly,
							},
						}},
					},
				},
			},
		},
	}
}
