// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3compact

import (
	"time"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/compact"
	"github.com/grab/talaria/internal/storage/flush"
	"github.com/grab/talaria/internal/storage/flush/writers"
)

const defaultInterval = 30 * time.Second

// New returns a compact store
func New(s3Config *config.S3Compact, monitor monitor.Monitor, store storage.Storage) *compact.Storage {
	s3Writer, err := writers.NewS3Writer(s3Config.Region, s3Config.Bucket, s3Config.Concurrency)
	if err != nil {
		panic(err)
	}

	nameFunc := func(row map[string]interface{}) (s string, e error) {
		return "", nil
	}

	if s3Config.NameFunc != "" {
		if fn, err := column.NewComputed("fileNameFunc", typeof.String, s3Config.NameFunc); err == nil {
			nameFunc = func(row map[string]interface{}) (s string, e error) {
				val, err := fn.Value(row)
				return val.(string), err
			}
		}
	}

	flusher := flush.New(monitor, s3Writer, nameFunc)
	return compact.New(store, flusher, flusher, monitor, defaultInterval)
}
