// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3compact

import (
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/compact"
	"github.com/grab/talaria/internal/storage/flush"
)

// New returns a compact store
func New(s3Config *config.S3Compact, monitor monitor.Monitor, store storage.Storage) *compact.Storage {

	// TODO: initialize the s3 writer after chunrong MR for s3 writer is merged.
	flusher := flush.New(monitor, nil, func(i map[string]interface{}) (s string, e error) {
		return "", nil
	})
	return compact.New(store, flusher, nil, monitor, 200*time.Millisecond)
}
