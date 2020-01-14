// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"sort"
	"strings"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/scritchley/orc"
)

// SchemaFor generates a schema
func SchemaFor(schema typeof.Schema) (*orc.TypeDescription, error) {
	var sb strings.Builder
	sb.WriteString("struct<")

	// Ensure keys are sorted
	schemaKey := make([]string, 0, len(schema))
	for key := range schema {
		schemaKey = append(schemaKey, key)
	}
	sort.Strings(schemaKey)

	first := true
	for _, key := range schemaKey {
		typ := schema[key]
		if !first {
			sb.WriteByte(0x2c) // ,
		}
		first = false

		sb.WriteString(key)
		sb.WriteByte(0x3a) // :
		sb.WriteString(typ.Category().String())
	}

	sb.WriteByte(0x3e) // >
	return orc.ParseSchema(sb.String())
}
