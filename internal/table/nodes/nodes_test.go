// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package nodes_test

import (
	"testing"

	"github.com/grab/talaria/internal/table/nodes"
	"github.com/stretchr/testify/assert"
)

type noopMembership int

func (m noopMembership) Members() []string {
	return []string{"127.0.0.1"}
}

func TestNodes(t *testing.T) {
	table := nodes.New(new(noopMembership))
	assert.NotNil(t, table)
	assert.Equal(t, "nodes", table.Name())
	defer table.Close()

	// Get the schema
	schema, err := table.Schema()
	assert.NoError(t, err)
	assert.Len(t, schema, 5)

	// Get the splits
	splits, err := table.GetSplits([]string{}, nil, 10000)
	assert.NoError(t, err)
	assert.Len(t, splits, 1)
	assert.Equal(t, "127.0.0.1", splits[0].Addrs[0])

	// Get the rows
	page, err := table.GetRows(splits[0].Key, []string{"private", "uptime", "started", "public", "peers"}, 1*1024*1024)
	assert.NotNil(t, page)
	assert.NoError(t, err)
	assert.Len(t, page.Columns, 5)
	assert.NotEmpty(t, page.Columns[0].AsBlock().VarcharData.Bytes)
	assert.Contains(t, string(page.Columns[1].AsBlock().VarcharData.Bytes), "seconds")
	assert.Equal(t, 1, page.Columns[2].AsBlock().BigintData.Count())
	assert.Equal(t, `["127.0.0.1"]`, string(page.Columns[4].AsBlock().VarcharData.Bytes))
}

func TestNodes_NoColumn(t *testing.T) {
	table := nodes.New(new(noopMembership))
	assert.NotNil(t, table)
	assert.Equal(t, "nodes", table.Name())
	defer table.Close()

	// Get the rows
	page, err := table.GetRows(nil, []string{"xxx"}, 1*1024*1024)
	assert.NotNil(t, page)
	assert.NoError(t, err)
}
