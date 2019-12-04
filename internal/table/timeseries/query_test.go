// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
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
		assert.Equal(t, "{\"b\":\"QUJD\",\"u\":null}", string(id))

		out, err := decodeQuery(id)
		assert.NoError(t, err)
		assert.Equal(t, []byte("ABC"), out.Begin)
	})
}

func TestNewQuery(t *testing.T) {

}

func newSplitQuery(eventName string) *presto.PrestoThriftTupleDomain {
	return &presto.PrestoThriftTupleDomain{
		Domains: map[string]*presto.PrestoThriftDomain{
			"event": {
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
