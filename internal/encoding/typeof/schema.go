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

type columnType struct {
	Column string `json:"column"`
	Type   string `json:"type"`
}
