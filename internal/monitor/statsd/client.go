// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package statsd

import (
	"strconv"

	"github.com/DataDog/datadog-go/statsd"
)

// New creates a new datadog statsd client
func New(host string, port int) Client {
	s, err := statsd.New(host + ":" + strconv.FormatInt(int64(port), 10))
	if err != nil {
		panic(err)
	}

	return s
}
