// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package monitor

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/ricochet2200/go-disk-usage/du"
)

type StatsdClient interface {
	Timing(name string, value time.Duration, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
	Histogram(name string, value float64, tags []string, rate float64) error
	Count(name string, value int64, tags []string, rate float64) error
}

// Monitor represents a contract that a monitor should implement
type Monitor interface {
	Tracker

	// Stats
	Duration(contextTag, key string, start time.Time, tags ...string)
	Gauge(contextTag, key string, value float64, tags ...string)
	Histogram(contextTag, key string, value float64, tags ...string)
	Count1(contextTag, key string, tags ...string)
	Count(contextTag, key string, amount int64, tags ...string)

	// Logging
	Debug(f string, v ...interface{})
	Info(f string, v ...interface{})
	Warning(err error)
	Error(err error)
}

// clientImpl monitors the system and hardware
type clientImpl struct {
	logger logging.Logger
	stats  StatsdClient
	tags   []string
	du     *du.DiskUsage
}

// New ...
func New(log logging.Logger, stats StatsdClient, appname string, env string) Monitor {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return &clientImpl{
		logger: log,
		stats:  stats,
		du:     du.NewDiskUsage(wd),
		tags: []string{
			"app_name:" + appname,
			"env:" + env,
		},
	}
}

func (c *clientImpl) enrichTags(tags []string) []string {
	return append(tags, c.tags...)
}

// Duration is time tracking metrics
func (c *clientImpl) Duration(contextKey, key string, start time.Time, tags ...string) {
	c.stats.Timing(contextKey+"."+key, time.Now().Sub(start), c.enrichTags(tags), 1)
}

// Gauge measures a value over time
func (c *clientImpl) Gauge(contextTag, key string, value float64, tags ...string) {
	c.stats.Gauge(contextTag+"."+key, value, c.enrichTags(tags), 1)
}

// Histogram tracks the statistical distribution of a set of values
func (c *clientImpl) Histogram(contextTag, key string, value float64, tags ...string) {
	c.stats.Histogram(contextTag+"."+key, value, c.enrichTags(tags), 1)
}

// Count1 tracks the occurrence of something (this is equivalent to Count(key, 1, tags...)
func (c *clientImpl) Count1(contextTag, key string, tags ...string) {
	c.Count(contextTag, key, 1, tags...)
}

// Count increases or Decrease the value of something over time
func (c *clientImpl) Count(contextTag, key string, amount int64, tags ...string) {
	c.stats.Count(contextTag+"."+key, amount, c.enrichTags(tags), 1)
}

// Debug writes out a warning message into the output logger.
func (c *clientImpl) Debug(f string, v ...interface{}) {
	c.logger.Debugf(f, v)
}

// Info writes out a warning message into the output logger.
func (c *clientImpl) Info(f string, v ...interface{}) {
	c.logger.Infof(f, v)
}

// Warning writes out a warning message into the output logger.
func (c *clientImpl) Warning(err error) {
	if err == nil {
		return
	}

	if xerr, ok := err.(*errors.Error); ok {
		c.stats.Count("warning", 1, c.enrichTags([]string{
			"target:" + xerr.Target,
			"reason:" + xerr.Reason,
		}), 1)
	}

	c.logger.Errorf(err.Error())
}

// Error writes out an error message into the output logger.
func (c *clientImpl) Error(err error) {
	if err == nil {
		return
	}

	if xerr, ok := err.(*errors.Error); ok {
		c.stats.Count("error", 1, c.enrichTags([]string{
			"target:" + xerr.Target,
			"reason:" + xerr.Reason,
		}), 1)
	}

	c.logger.Errorf(err.Error())
}

// ------------------------------------------------------------------------------------------

// Debugf writes out a warning message into the output logger. This is to support Badger logger.
func (c *clientImpl) Debugf(f string, v ...interface{}) {
	c.logger.Debugf(f, v...)
}

// Info writes out a warning message into the output logger. This is to support Badger logger.
func (c *clientImpl) Infof(f string, v ...interface{}) {
	c.logger.Infof(f, v...)
}

// Warningf writes out a warning message into the output logger. This is to support Badger logger.
func (c *clientImpl) Warningf(f string, v ...interface{}) {
	c.logger.Warningf("%s: %s", caller(3), fmt.Sprintf(f, v...))
}

// Errorf writes out an error message into the output logger. This is to support Badger logger.
func (c *clientImpl) Errorf(f string, v ...interface{}) {
	c.logger.Errorf("%s: %s", caller(3), fmt.Sprintf(f, v...))
}

// Caller formats the caller
func caller(skip int) string {
	const watermark = "github.com/"
	_, fn, line, _ := runtime.Caller(skip)
	idx := strings.LastIndex(fn, watermark)
	if idx > 0 {
		idx += len(watermark)
	}
	if idx < 0 {
		idx = 0
	}

	return fmt.Sprintf("%s.%d", fn[idx:], line)
}
