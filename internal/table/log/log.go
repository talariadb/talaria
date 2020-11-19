// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

import (
	"fmt"
	"time"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/kelindar/talaria/internal/storage/writer"
	"github.com/kelindar/talaria/internal/table"
	"github.com/kelindar/talaria/internal/table/timeseries"
)

// Assert the contract
var _ table.Table = new(Table)

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
func New(cfg config.Func, cluster Membership, monitor monitor.Monitor) *Table {
	const name = "logs"

	// Open a dedicated logs storage
	store := disk.Open(cfg().Storage.Directory, name, monitor, cfg().Storage.Badger)

	// Create a noop streamer
	streams, _ := writer.ForStreaming(config.Streams{}, monitor, nil)

	base := timeseries.New(name, cluster, monitor, store, &config.Table{
		TTL:    24 * 3600, // 1 day
		SortBy: "time",
		HashBy: "",
		Schema: "",
	}, streams)
	return &Table{
		Table:   *base,
		cluster: cluster,
	}
}

// Errorf writes out an error message into the output logger.
func (t *Table) Errorf(f string, v ...interface{}) {
	t.Append(fmt.Sprintf("[error] "+f, v...), logging.LevelError)
}

// Warningf writes out a warning message into the output logger.
func (t *Table) Warningf(f string, v ...interface{}) {
	t.Append(fmt.Sprintf("[warning]: "+f, v...), logging.LevelWarning)
}

// Infof writes out a warning message into the output logger.
func (t *Table) Infof(f string, v ...interface{}) {
	t.Append(fmt.Sprintf(f, v...), logging.LevelInfo)
}

// Debugf writes out a warning message into the output logger.
func (t *Table) Debugf(f string, v ...interface{}) {
	t.Append(fmt.Sprintf(f, v...), logging.LevelDebug)
}

// Append converts message to a block and append to timeseries
func (t *Table) Append(msg string, level logging.Level) error {
	columns := t.toColumns(msg, level)
	block, err := block.FromColumns("", columns)
	if err != nil {
		return err
	}

	return t.Table.Append(block)
}

func (t *Table) toColumns(msg string, level logging.Level) column.Columns {
	columns := make(column.Columns, 4)
	columns.Append("time", time.Now(), typeof.Timestamp)
	columns.Append("address", t.cluster.Addr(), typeof.String)
	columns.Append("level", string(level), typeof.String)
	columns.Append("message", msg, typeof.String)
	return columns
}
