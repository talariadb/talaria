// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"os"

	"github.com/ricochet2200/go-disk-usage/du"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor/logging"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor/statsd"
)

// Client ...
//go:generate mockery -name=Client -case underscore -inpkg
type Client interface {
	Tracker
	Stats
	Logger

	// DefaultTags ...
	DefaultTags() []string

	WithHost(hostname string) Client
}

// clientImpl monitors the system and hardware
type clientImpl struct {
	log   logging.Logger // The log client
	stats statsd.Client
	tags  []string
	du    *du.DiskUsage
}

// New ...
func New(log logging.Logger, stats statsd.Client, appname string) Client {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return &clientImpl{
		log:   log,
		stats: stats,
		du:    du.NewDiskUsage(wd),
		tags: []string{
			"app:" + appname,
		},
	}
}

// DefaultTags ...
func (c *clientImpl) DefaultTags() []string {
	return c.tags
}

// WithHost ...
func (c *clientImpl) WithHost(hostname string) Client {
	c.tags = append(c.tags, "host:"+hostname)
	return c
}
