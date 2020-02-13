// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

// NewNoop returns a logger that doesn't do anything.
func NewNoop() Logger {
	return new(noop)
}

type noop struct{}

func (l *noop) Errorf(f string, v ...interface{}) {}

func (l *noop) Warningf(f string, v ...interface{}) {}

func (l *noop) Infof(f string, v ...interface{}) {}

func (l *noop) Debugf(f string, v ...interface{}) {}
