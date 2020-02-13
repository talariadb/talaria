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
	initializeStruct(t, v.Elem())
	reflect.ValueOf(c).Elem().Set(v.Elem())

	// Put static values in the config
	c.Readers.Presto.Port = 8042

	c.Writers.GRPC.Port = 8080

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

func initializeStruct(t reflect.Type, v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := t.Field(i)
		switch ft.Type.Kind() {
		case reflect.Struct:
			initializeStruct(ft.Type, f)
		case reflect.Ptr:
			fv := reflect.New(ft.Type.Elem())
			switch f.Type().Elem().Kind() {
			case reflect.Struct:
				initializeStruct(ft.Type.Elem(), fv.Elem())
				f.Set(fv)
			case reflect.Int64, reflect.Int32, reflect.Int, reflect.Float64, reflect.Float32, reflect.String, reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Bool:
				f.Set(fv)
			}
		default:
		}
	}
}
