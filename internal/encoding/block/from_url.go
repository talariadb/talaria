// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package block

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/kelindar/loader"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// FromURLBy creates a block from a remote url which should be loaded. It repartitions the batch by a given partition key at the same time.
func FromURLBy(uri string, partitionBy string, filter *typeof.Schema, apply applyFunc) ([]Block, error) {
	var handler func([]byte, string, *typeof.Schema, applyFunc) ([]Block, error)
	switch strings.ToLower(filepath.Ext(uri)) {
	case ".orc":
		handler = FromOrcBy
	case ".csv":
		handler = FromCSVBy
	default:
		return nil, errors.Newf("block: unsupported file extension %s", filepath.Ext(uri))
	}

	l := loader.New()
	b, err := l.Load(context.Background(), uri)
	if err != nil {
		return nil, err
	}

	return handler(b, partitionBy, filter, apply)
}
