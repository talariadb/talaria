// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package table

import (
	"fmt"

	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/grab/talaria/internal/table/log"
)

type table struct {
	Table *log.Table
}

// NewStandard returns a new logger that will only output log messages to Talaria log table
func NewTable(t *log.Table) logging.Logger {
	return &table{Table: t}
}

func (t *table) Errorf(f string, v ...interface{}) {
	t.Table.Append(fmt.Sprintf("[error] "+f, v...), logging.LevelError)
}

func (t *table) Warningf(f string, v ...interface{}) {
	t.Table.Append(fmt.Sprintf("[warning]: "+f, v...), logging.LevelWarning)
}

func (t *table) Infof(f string, v ...interface{}) {
	t.Table.Append(fmt.Sprintf(f, v...), logging.LevelInfo)
}

func (t *table) Debugf(f string, v ...interface{}) {
	t.Table.Append(fmt.Sprintf(f, v...), logging.LevelDebug)
}
