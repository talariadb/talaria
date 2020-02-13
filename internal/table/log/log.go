// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

import (
	"fmt"
	"time"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/block"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/grab/talaria/internal/table/timeseries"
)

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
	store := disk.Open(cfg().Storage.Directory, cfg().Tables.Log.Name, monitor)
	base := timeseries.New(cluster, monitor, store, timeseries.Config{
		Name:   cfg().Tables.Log.Name,
		TTL:    cfg().Tables.Log.TTL,
		SortBy: cfg().Tables.Log.SortBy,
		HashBy: "",
		Schema: func() *typeof.Schema { return nil },
	})
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
