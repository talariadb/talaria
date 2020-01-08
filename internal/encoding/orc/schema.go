// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"strings"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/scritchley/orc"
)

// SchemaFor generates a schema
func SchemaFor(schema typeof.Schema) (*orc.TypeDescription, error) {
	var sb strings.Builder
	sb.WriteString("struct<")

	first := true
	for name, typ := range schema {
		if !first {
			sb.WriteByte(0x2c) // ,
		}
		first = false

		sb.WriteString(name)
		sb.WriteByte(0x3a) // :
		sb.WriteString(typ.Category().String())
	}

	sb.WriteByte(0x3e) // >
	return orc.ParseSchema(sb.String())
}
