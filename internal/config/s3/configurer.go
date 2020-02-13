// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"net/url"

	"github.com/grab/talaria/internal/config"
	"gopkg.in/yaml.v2"
)

// Configurer to fetch configuration from a s3 object
type Configurer struct {
	client *client
}

func New() *Configurer {
	c, _ := newClient(nil)
	return &Configurer{
		client: c,
	}
}

// Configure fetches a yaml config file from a s3 path and populate the config object
func (s *Configurer) Configure(c *config.Config) error {
	path := c.URI
	if path == "" {
		// s3 path missing. might be the user doesn't want to use s3 as configuration
		return nil
	}
	u, err := url.Parse(path)
	if err != nil {
		return err
	}

	// download the config
	b, err := s.client.Download(context.Background(), u.Host, u.Path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(b, c)
	if err != nil {
		return err
	}

	// download the schema of the timeseries table by using the same bucket as the config and tablename_schema as the key
	timeseriesName := c.Tables.Timeseries.Name
	if timeseriesName != "" {
		b, err = s.client.Download(context.Background(), u.Host, timeseriesName+"_schema")
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(b, &c.Tables.Timeseries.Schema)
		if err != nil {
			return err
		}
	}
	return nil
}
