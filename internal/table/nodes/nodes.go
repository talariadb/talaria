// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package nodes

import (
	"encoding/json"
	"net"
	"time"

	"github.com/emitter-io/address"
	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/table"
	"github.com/hako/durafmt"
)

// Assert the contract
var _ table.Table = new(Table)

// SplitKey is not required for this table
var splitKey = []byte{0x00}

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
	Addr() string
}

// Table represents a nodes table.
type Table struct {
	cluster   Membership // The membership list to use
	startedAt time.Time  // The time when the node was started
}

// New creates a new table implementation.
func New(cluster Membership) *Table {
	return &Table{
		cluster:   cluster,
		startedAt: time.Now(),
	}
}

// Close implements io.Closer interface.
func (t *Table) Close() error {
	return nil
}

// Name returns the name of the table.
func (t *Table) Name() string {
	return "nodes"
}

// Schema retrieves the metadata for the table
func (t *Table) Schema() (typeof.Schema, error) {
	return typeof.Schema{
		"address": typeof.String,
		"public":  typeof.String,
		"private": typeof.String,
		"started": typeof.Int64,
		"uptime":  typeof.String,
		"peers":   typeof.String,
	}, nil
}

// GetSplits retrieves the splits
func (t *Table) GetSplits(desiredColumns []string, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int) ([]table.Split, error) {

	// We need to generate as many splits as we have nodes in our cluster. Each split needs to contain the IP address of the
	// node containing that split, so Presto can reach it and request the data.
	splits := make([]table.Split, 0, 16)
	for _, m := range t.cluster.Members() {
		splits = append(splits, table.Split{
			Key:   splitKey,
			Addrs: []string{m},
		})
	}
	return splits, nil
}

// GetRows retrieves the data
func (t *Table) GetRows(splitID []byte, columns []string, maxBytes int64) (*table.PageResult, error) {
	result := &table.PageResult{
		Columns: make([]presto.Column, 0, len(columns)),
	}

	for _, c := range columns {
		schema, _ := t.Schema()
		if kind, hasType := schema[c]; hasType {
			column, err := t.getColumn(c, kind)
			if err != nil {
				return nil, err
			}

			result.Columns = append(result.Columns, column)
		}
	}
	return result, nil
}

// getColumn returns a coolumn info requested
func (t *Table) getColumn(columnName string, columnType typeof.Type) (presto.Column, error) {
	column := column.NewColumn(columnType)
	switch columnName {
	case "address":
		column.Append(t.cluster.Addr())
	case "public":
		column.Append(formatAddrs(address.GetPublic()))
	case "private":
		column.Append(formatAddrs(address.GetPrivate()))
	case "started":
		column.Append(t.startedAt.Unix())
	case "uptime":
		column.Append(durafmt.Parse(time.Now().Sub(t.startedAt)).String())
	case "peers":
		column.Append(encode(t.cluster.Members()))
	}
	return column, nil
}

// Formats the set of addresses
func formatAddrs(addrs []net.IPAddr, err error) string {
	if err != nil {
		return err.Error()
	}

	// Flatten to strings
	var arr []string
	for _, addr := range addrs {
		arr = append(arr, addr.String())
	}

	// Marshal as JSON
	return encode(arr)
}

// Encode encodes as JSON
func encode(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}

	return string(b)
}
