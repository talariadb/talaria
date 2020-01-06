// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package typeof

import (
	"encoding/json"
	"sort"
)

// Schema represents a mapping between columns and their types
type Schema map[string]Type

// String returns the string representation of the schema
func (s Schema) String() string {
	columns := make([]columnType, 0, len(s))
	for k, v := range s {
		columns = append(columns, columnType{
			Column: k,
			Type:   v.SQL(),
		})
	}

	sort.SliceStable(columns, func(i, j int) bool {
		return columns[i].Column < columns[j].Column
	})

	b, _ := json.Marshal(columns)
	return string(b)
}

// Columns returns the columns in the schema
func (s Schema) Columns() []string {
	columns := make([]string, 0, len(s))
	for k := range s {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	return columns
}

// Compare the schema against a desired schema and returns mismatching columns/types.
func (s Schema) Compare(desired Schema) (Schema, bool) {
	var diff Schema
	for name, typ := range desired {
		if _, matchName := s[name]; !matchName || s[name] != typ {
			if diff == nil {
				diff = make(Schema, len(desired))
			}
			diff[name] = typ
		}
	}

	return diff, diff == nil
}

// Except returns a subset of the schema which does not contain the specified schema
func (s Schema) Except(other Schema) Schema {
	result := make(Schema, len(s)-len(other))
	for name, typ := range s {
		if _, ok := other[name]; !ok {
			result[name] = typ
		}
	}

	return result
}

// Union combines two schemas together. It returns a true if a union has mismatched types or not
// in case of a type mismatch.
func (s *Schema) Union(other Schema) bool {
	combined := *s
	for name, typ := range other {
		if _, exists := combined[name]; exists && combined[name] != typ {
			return false
		}

		combined[name] = typ
	}

	s = &combined
	return true
}

type columnType struct {
	Column string `json:"column"`
	Type   string `json:"type"`
}
