// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"testing"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
)

func TestSchemaFor(t *testing.T) {
	s := typeof.Schema{
		"a":        typeof.Int32,
		"b":        typeof.String,
		"c":        typeof.Int64,
		"d":        typeof.String,
		"/invalid": typeof.String,
		"123":      typeof.String,
	}

	out, err := SchemaFor(s)
	assert.NoError(t, err)
	assert.Equal(t, "struct<a:int,b:string,c:bigint,d:string>", out.String())
}
