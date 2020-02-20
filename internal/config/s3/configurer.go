// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/grab/talaria/internal/config"
	"gopkg.in/yaml.v2"
)

// Configurer to fetch configuration from a s3 object
type Configurer struct {
	client *client
}

// New creates a new S3 configurer.
func New() *Configurer {
	c, _ := newClient(nil)
	return &Configurer{
		client: c,
	}
}

// Configure fetches a yaml config file from a s3 path and populate the config object
func (s *Configurer) Configure(c *config.Config) error {
	if c.URI == "" {
		return nil
	}

	u, err := url.Parse(c.URI)
	if err != nil {
		return err
	}

	// download the config
	b, err := s.client.Download(context.Background(), getBucket(u.Host), getPrefix(u.Path))
	if err != nil {
		return err
	}

	if yaml.Unmarshal(b, c) != nil {
		return err
	}

	// download the schema of the timeseries table by using the same bucket as the config and tablename_schema as the key
	if name := c.Tables.Timeseries.Name; name != "" {
		b, err := s.client.Download(context.Background(), getBucket(u.Host), getPrefix(path.Join(path.Dir(u.Path), name+"_schema.yaml")))
		if err != nil || len(b) == 0 {
			return nil // Schema not found, continue without it
		}

		if yaml.Unmarshal(b, &c.Tables.Timeseries.Schema) != nil {
			return err
		}
	}
	return nil
}

func getBucket(host string) string {
	return strings.Split(host, ".")[0]
}

func getPrefix(path string) string {
	return strings.TrimLeft(path, "/")
}
