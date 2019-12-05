// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"os"

	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/ricochet2200/go-disk-usage/du"
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
	logging.Logger
	stats StatsdClient
	tags  []string
	du    *du.DiskUsage
}

// New ...
func New(log logging.Logger, stats StatsdClient, appname string, env string) Client {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return &clientImpl{
		Logger: log,
		stats:  stats,
		du:     du.NewDiskUsage(wd),
		tags: []string{
			"app_name:" + appname,
			"env:" + env,
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
