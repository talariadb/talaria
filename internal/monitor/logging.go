// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

const errTag = "error"

// Logger defines the interface for logger client
type Logger interface {
	// Debug ...
	Debug(tag, message string, args ...interface{})

	// Info ...
	Info(tag, message string, args ...interface{})

	// Warn ...
	Warn(tag, message string, args ...interface{})

	// Error ...
	Error(tag, message string, args ...interface{})

	// Fatal ...
	Fatal(tag, message string, args ...interface{})

	// Security ...
	Security(tag, message string, args ...interface{})

	// WarnWithStats ...
	WarnWithStats(tag, errType, message string, args ...interface{})

	// ErrorWithStats ...
	ErrorWithStats(tag, errType, message string, args ...interface{})

	// FatalWithStats ...
	FatalWithStats(tag, errType, message string, args ...interface{})

	// SecurityWithStats ...
	SecurityWithStats(tag, errType, message string, args ...interface{})
}

// Debug ...
func (c *clientImpl) Debug(tag, message string, args ...interface{}) {
	c.log.Debug(tag, message, args...)
}

// Info ...
func (c *clientImpl) Info(tag, message string, args ...interface{}) {
	c.log.Info(tag, message, args...)
}

// WarnWithStats ...
func (c *clientImpl) Warn(tag, message string, args ...interface{}) {
	c.log.Warn(tag, message, args...)
}

// Error ...
func (c *clientImpl) Error(tag, message string, args ...interface{}) {
	c.log.Error(tag, message, args...)
}

// Fatal ...
func (c *clientImpl) Fatal(tag, message string, args ...interface{}) {
	c.log.Fatal(tag, message, args...)
}

// Security ...
func (c *clientImpl) Security(tag, message string, args ...interface{}) {
	c.log.Security(tag, message, args)
}

// WarnWithStats ...
func (c *clientImpl) WarnWithStats(tag, errType, message string, args ...interface{}) {
	c.log.Warn(tag, message, args...)
	c.Count1(tag, errTag, "type:"+errType)
}

// ErrorWithStats ...
func (c *clientImpl) ErrorWithStats(tag, errType, message string, args ...interface{}) {
	c.log.Error(tag, message, args...)
	c.Count1(tag, errTag, "type:"+errType)
}

// FatalWithStats ...
func (c *clientImpl) FatalWithStats(tag, errType, message string, args ...interface{}) {
	c.log.Fatal(tag, message, args...)
	c.Count1(tag, errTag, "type:"+errType)
}

// SecurityWithStats ...
func (c *clientImpl) SecurityWithStats(tag, errType, message string, args ...interface{}) {
	c.log.Security(tag, message, args)
	c.Count1(tag, errTag, "type:"+errType)
}
