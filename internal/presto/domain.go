// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"errors"
	"math"
	"time"

	expr "github.com/Knetic/govaluate"
)

var (
	errNoFilter      = errors.New("no filters were specified")
	errInvalidFilter = errors.New("constraint must have 3 parts: column, operator and value")
	errInvalidColumn = errors.New("constraint must match hash or sort columns with the appropriate type")
)

// NewDomain creates a new domain from a set of filters
func NewDomain(hashKey, sortKey string, filters ...string) (*PrestoThriftTupleDomain, error) {
	if len(filters) == 0 {
		return nil, errNoFilter
	}

	// TODO: replace with the filtering library
	domains := make(map[string]*PrestoThriftDomain, 2)
	for _, f := range filters {
		ex, err := expr.NewEvaluableExpression(f)
		if err != nil {
			return nil, err
		}

		tokens := ex.Tokens()
		if len(tokens) != 3 && len(tokens) != 7 {
			return nil, errInvalidFilter
		}

		if len(tokens) == 3 {
			// Check if the type of expression is valid
			columnToken, operatorToken, value := tokens[0], tokens[1], tokens[2]
			if columnToken.Kind != expr.VARIABLE || operatorToken.Kind != expr.COMPARATOR {
				return nil, errInvalidFilter
			}

			// Convert and compare
			column, operator := columnToken.Value.(string), operatorToken.Value.(string)
			switch {
			case column == hashKey && operator == "==" && value.Kind == expr.STRING:
				domains[hashKey] = equalsHash(value.Value.(string))
			case column == sortKey && operator == ">=" && value.Kind == expr.NUMERIC:
				domains[sortKey] = withinBoundedRangeSort(int64(value.Value.(float64)), int64(math.MaxInt64))
			default:
				return nil, errInvalidColumn
			}

		} else {
			// Check if the type of expression is valid
			columnToken, startOffset, endOffset := tokens[0], tokens[2], tokens[6]
			if columnToken.Kind != expr.VARIABLE || startOffset.Kind != expr.NUMERIC || endOffset.Kind != expr.NUMERIC {
				return nil, errInvalidFilter
			}

			// Convert and compare
			column, start, end := columnToken.Value.(string), startOffset.Value.(float64), endOffset.Value.(float64)
			switch {
			case column == sortKey:
				domains[sortKey] = withinBoundedRangeSort(int64(start), int64(end))
			default:
				return nil, errInvalidColumn
			}

		}

		/*for _, t := range ex.Tokens() {
			fmt.Printf("%+v %+v\n", t.Kind.String(), t.Value)
		}*/
	}

	return &PrestoThriftTupleDomain{
		Domains: domains,
	}, nil
}

// equalsHash creates a domain for the hash (equality)
func equalsHash(value string) *PrestoThriftDomain {
	return &PrestoThriftDomain{
		ValueSet: &PrestoThriftValueSet{
			RangeValueSet: &PrestoThriftRangeValueSet{
				Ranges: []*PrestoThriftRange{{
					Low: &PrestoThriftMarker{
						Value: &PrestoThriftBlock{
							VarcharData: &PrestoThriftVarchar{
								Bytes: []byte(value),
								Sizes: []int32{int32(len(value))},
							},
						},
						Bound: PrestoThriftBoundExactly,
					},
					High: &PrestoThriftMarker{
						Value: &PrestoThriftBlock{
							VarcharData: &PrestoThriftVarchar{
								Bytes: []byte(value),
								Sizes: []int32{int32(len(value))},
							},
						},
						Bound: PrestoThriftBoundExactly,
					},
				}},
			},
		},
	}
}

// withinBoundedRangeSort creates a domain for the sortkey bounds(ex: (= [) Lower inclusive and upper exclusive)
func withinBoundedRangeSort(start, end int64) *PrestoThriftDomain {
	return &PrestoThriftDomain{
		ValueSet: &PrestoThriftValueSet{
			RangeValueSet: &PrestoThriftRangeValueSet{
				Ranges: []*PrestoThriftRange{{
					Low: &PrestoThriftMarker{
						Value: &PrestoThriftBlock{
							BigintData: &PrestoThriftBigint{
								Nulls: []bool{false},
								Longs: []int64{start},
							},
						},
						Bound: PrestoThriftBoundExactly,
					},
					High: &PrestoThriftMarker{
						Value: &PrestoThriftBlock{
							BigintData: &PrestoThriftBigint{
								Nulls: []bool{false},
								Longs: []int64{end},
							},
						},
						Bound: PrestoThriftBoundExactly,
					},
				}},
			},
		},
	}
}

// ------------------------------------------------------------------------------------------------------------

// AsTimeRange converts thrift range as a time range
func (r *PrestoThriftRange) AsTimeRange() (time.Time, time.Time, bool) {

	// We must always have a low bound
	zero := time.Unix(0, 0)
	if r.Low == nil || r.Low.Value == nil || r.Low.Value.BigintData == nil {
		return zero, zero, false
	}

	switch {

	// Concrete interval [t0, t1]
	case r.Low.Bound == PrestoThriftBoundExactly &&
		r.High != nil && r.High.Bound == PrestoThriftBoundExactly && r.High.Value.BigintData != nil:
		return toTime(r.Low.Value.BigintData.Min()), toTime(r.High.Value.BigintData.Min()), true

	// Lower bound [t0, max]
	case r.Low.Bound == PrestoThriftBoundAbove:
		return toTime(r.Low.Value.BigintData.Min()), time.Unix(math.MaxInt64, 0), true

	// Upper bound [min, t0]
	case r.Low.Bound == PrestoThriftBoundBelow:
		return time.Unix(0, 0), toTime(r.Low.Value.BigintData.Min()), true

	}

	return zero, zero, false
}

// Converts time provided to a golang time
func toTime(t int64, ok bool) time.Time {
	if !ok {
		return time.Unix(t, 0)
	}

	watermark := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	switch {
	case t > watermark.UnixNano():
		return time.Unix(0, t)
	case t > watermark.UnixNano()/1000:
		return time.Unix(0, t*1000)
	case t > watermark.UnixNano()/1000000:
		return time.Unix(0, t*1000000)
	default:
		return time.Unix(t, 0)
	}
}
