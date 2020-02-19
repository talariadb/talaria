// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package static

import (
	"reflect"

	"github.com/grab/talaria/internal/config"
)

// Configurer
type Configurer struct{}

//
func New() *Configurer {
	return &Configurer{}
}

// Configure to define static hard coded config values.
func (e *Configurer) Configure(c *config.Config) error {
	// initalize all the pointers in the struct
	t := reflect.TypeOf(config.Config{})
	v := reflect.New(t)
	reflect.ValueOf(c).Elem().Set(v.Elem())

	// Put static values in the config
	c.Readers.Presto = &config.Presto{Port: 8042}

	c.Writers.GRPC = &config.GRPC{Port: 8080}

	c.Tables.Timeseries = &config.Timeseries{
		Name:   "eventlog",
		TTL:    3600,
		SortBy: "tsi",
		HashBy: "event",
	}
	c.Tables.Log = &config.Log{
		Name:   "log",
		TTL:    24 * 3600, // 1 day
		SortBy: "time",
	}

	c.Statsd = &config.StatsD{
		Host: "localhost",
		Port: 8125,
	}

	return nil
}
