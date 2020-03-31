// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package log

import (
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/kelindar/lua"
)

// New creates a new lua module exposing the logger
func New(monitor monitor.Monitor) lua.Module {
	l := &logger{monitor}
	m := &lua.NativeModule{
		Name:    "log",
		Version: "1.0",
	}

	m.Register("debug", l.Debug)
	m.Register("info", l.Info)
	m.Register("warning", l.Warning)
	m.Register("error", l.Error)
	return m
}

type logger struct {
	monitor monitor.Monitor
}

// Debug logs the message in debug mode.
func (l *logger) Debug(message lua.String) error {
	l.monitor.Debug(string(message))
	return nil
}

// Info logs the message in info mode.
func (l *logger) Info(message lua.String) error {
	l.monitor.Info(string(message))
	return nil
}

// Info logs the warning message.
func (l *logger) Warning(message lua.String) error {
	l.monitor.Warning(errors.New(string(message)))
	return nil
}

// Info logs the error message.
func (l *logger) Error(message lua.String) error {
	l.monitor.Error(errors.New(string(message)))
	return nil
}
