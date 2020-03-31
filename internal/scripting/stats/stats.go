// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package stats

import (
	"github.com/grab/talaria/internal/monitor"
	"github.com/kelindar/lua"
)

const ctxTag = "stats"

// New creates a new lua module exposing the stats
func New(monitor monitor.Monitor) lua.Module {
	l := &logger{monitor}
	m := &lua.NativeModule{
		Name:    "stats",
		Version: "1.0",
	}

	m.Register("count", l.Count)
	m.Register("histogram", l.Histogram)
	m.Register("gauge", l.Gauge)
	return m
}

type logger struct {
	monitor monitor.Monitor
}

// // Count increases or Decrease the value of something over time
func (l *logger) Count(key lua.String, amount lua.Number, tags lua.Strings) error {
	l.monitor.Count(ctxTag, string(key), int64(amount), []string(tags)...)
	return nil
}

// Histogram tracks the statistical distribution of a set of values
func (l *logger) Histogram(key lua.String, value lua.Number, tags lua.Strings) error {
	l.monitor.Histogram(ctxTag, string(key), float64(value), []string(tags)...)
	return nil
}

// Gauge measures a value over time
func (l *logger) Gauge(key lua.String, value lua.Number, tags lua.Strings) error {
	l.monitor.Gauge(ctxTag, string(key), float64(value), []string(tags)...)
	return nil
}
