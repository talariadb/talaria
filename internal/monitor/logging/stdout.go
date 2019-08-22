// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

import (
	"fmt"
	"os"
)

// Logger defines required logging interface
type Logger interface {
	Debug(tag string, message string, args ...interface{})
	Info(tag string, message string, args ...interface{})
	Warn(tag string, message string, args ...interface{})
	Error(tag string, message string, args ...interface{})
	Fatal(tag string, message string, args ...interface{})
	Security(tag string, message string, args ...interface{})
}

// NewStdOut returns a new logger that will only output log messages to stdout
func NewStdOut() Logger {
	return &stdOutLogger{}
}

// Implement the Logger interface
type stdOutLogger struct{}

// Debug implements Logger interface
func (lr *stdOutLogger) Debug(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, "[Debug]["+tag+"] "+message+"\n", args...)
}

// Info implements Logger interface
func (lr *stdOutLogger) Info(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, "[Info]["+tag+"] "+message+"\n", args...)
}

// Warn implements Logger interface
func (lr *stdOutLogger) Warn(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, "[Warn]["+tag+"] "+message+"\n", args...)
}

// Error implements Logger interface
func (lr *stdOutLogger) Error(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[Error]["+tag+"] "+message+"\n", args...)
}

// Fatal implements Logger interface
func (lr *stdOutLogger) Fatal(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[Fatal]["+tag+"] "+message+"\n", args...)
}

// Security implements Logger interface
func (lr *stdOutLogger) Security(tag string, message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[Security]["+tag+"] "+message+"\n", args...)
}
