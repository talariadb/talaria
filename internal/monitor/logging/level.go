// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package logging

type Level string

const (
	LevelError   Level = "error"
	LevelWarning Level = "warning"
	LevelInfo    Level = "info"
	LevelDebug   Level = "debug"
)
