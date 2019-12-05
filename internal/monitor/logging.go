// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import "github.com/grab/talaria/internal/monitor/logging"

const errTag = "error"

// Logger defines the interface for logger client
type Logger interface {
	logging.Logger
	WarnWithStats(tag, errType, message string, args ...interface{})
	ErrorWithStats(tag, errType, message string, args ...interface{})
}

// WarnWithStats ...
func (c *clientImpl) WarnWithStats(tag, errType, message string, args ...interface{}) {
	c.Warningf(message, args...)
	c.Count1(tag, errTag, "type:"+errType)
}

// ErrorWithStats ...
func (c *clientImpl) ErrorWithStats(tag, errType, message string, args ...interface{}) {
	c.Errorf(message, args...)
	c.Count1(tag, errTag, "type:"+errType)
}
