// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package static

import (
	"reflect"

	"github.com/kelindar/talaria/internal/config"
)

// Configurer represents a static configurer
type Configurer struct{}

// New creates a new static configurer
func New() *Configurer {
	return &Configurer{}
}

// Configure to define static hard coded config values.
func (e *Configurer) Configure(c *config.Config) error {
	// initalize all the pointers in the struct
	t := reflect.TypeOf(config.Config{})
	v := reflect.New(t)
	reflect.ValueOf(c).Elem().Set(v.Elem())

	// Put default values in the config
	c.AppName = "talaria"
	c.Readers.Presto = &config.Presto{Port: 8042}
	c.Writers.GRPC = &config.GRPC{Port: 8080}
	c.Tables = make(map[string]config.Table, 1)

	// Default statsD agent
	c.Statsd = &config.StatsD{
		Host: "localhost",
		Port: 8125,
	}

	return nil
}
