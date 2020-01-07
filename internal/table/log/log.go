// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

import (
	"io"
	"time"

	"github.com/grab/talaria/internal/monitor/logging"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/presto"
	"github.com/grab/talaria/internal/table/timeseries"
	"github.com/kelindar/binary/nocopy"
)

const (
	table = "log"
)

// Storage represents an underlying storage layer.
type Storage interface {
	io.Closer
	Append(key, value []byte, ttl time.Duration) error
	Range(seek, until []byte, f func(key, value []byte) bool) error
}

// Membership represents a contract required for recovering cluster information.
type Membership interface {
	Members() []string
	Addr() string
}

// Table represents a log table.
type Table struct {
	timeseries.Table
	cluster Membership
}

// New creates a new table implementation.
func New(log *config.Log, dataDir string, cluster Membership, monitor monitor.Client) *Table {
	cfg := &config.Storage{
		TTLInSec:   log.TTLInSec,
		TimeColumn: "time",
	}

	base := timeseries.New(table, cfg, dataDir, cluster, monitor)
	return &Table{
		Table:   *base,
		cluster: cluster,
	}
}

// Append converts message to a block and append to timeseries
func (t *Table) Append(msg string, level logging.Level) error {
	columns := t.toColumns(msg, level)
	block, err := makeBlock(columns)
	if err != nil {
		return err
	}

	return t.Table.Append(block)
}

func (t *Table) toColumns(msg string, level logging.Level) presto.NamedColumns {
	columns := make(presto.NamedColumns, 4)
	columns.Append("time", time.Now(), typeof.Timestamp)
	columns.Append("address", t.cluster.Addr(), typeof.String)
	columns.Append("level", string(level), typeof.String)
	columns.Append("message", msg, typeof.String)
	return columns
}

// makeBlocks creates a set of blocks from a set of named columns
func makeBlock(columns presto.NamedColumns) (block.Block, error) {
	b := block.Block{Key: nocopy.String("")}
	if err := b.WriteColumns(columns); err != nil {
		return block.Block{}, err
	}

	return b, nil
}
