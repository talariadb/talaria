// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

import (
	"log"
	"os"
)

const prefix = ""

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

// NewStandard returns a new logger that will only output log messages to stdout or stderr
func NewStandard() Logger {
	return &stdLogger{
		stdout: log.New(os.Stdout, prefix, log.LstdFlags),
		stderr: log.New(os.Stderr, prefix, log.LstdFlags),
	}
}

// Implement the Logger interface
type stdLogger struct {
	stdout *log.Logger
	stderr *log.Logger
}

func (l *stdLogger) Errorf(f string, v ...interface{}) {
	l.stderr.Printf("[error] "+f, v...)
}

func (l *stdLogger) Warningf(f string, v ...interface{}) {
	l.stderr.Printf("[warning]: "+f, v...)
}

func (l *stdLogger) Infof(f string, v ...interface{}) {
	l.stdout.Printf(f, v...)
}

func (l *stdLogger) Debugf(f string, v ...interface{}) {
	l.stdout.Printf(f, v...)
}
