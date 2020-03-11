// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor/logging"
	"github.com/kelindar/loader"
	"gopkg.in/yaml.v2"
)

type downloader interface {
	Load(ctx context.Context, uri string) ([]byte, error)
}

// Configurer to fetch configuration from a s3 object
type Configurer struct {
	client downloader
	log    logging.Logger
}

// New creates a new S3 configurer.
func New(log logging.Logger) *Configurer {
	return NewWith(loader.New(), log)
}

// SetLogger to set the logger after initialization
func (s *Configurer) SetLogger(lo logging.Logger) {
	s.log = lo
}

// NewWith creates a new S3 configurer.
func NewWith(dl downloader, log logging.Logger) *Configurer {
	return &Configurer{
		client: dl,
		log:    log,
	}
}

// Configure fetches a yaml config file from a s3 path and populate the config object
func (s *Configurer) Configure(c *config.Config) error {
	if c.URI == "" {
		return nil
	}

	// download the config
	b, err := s.client.Load(context.Background(), c.URI)
	if err != nil {
		s.log.Warningf("error in downloading config from s3. Load error %+v", err)
		return nil // Unable to load, skip
	}

	if yaml.Unmarshal(b, c) != nil {
		return err
	}

	// download the schema of the timeseries table by using the same bucket as the config and tablename_schema as the key
	name := c.Tables.Timeseries.Name
	if name == "" {
		s.log.Warningf("error in downloading event schema from s3. Schema name missing")
		return nil
	}

	// Parse the URL
	u, err := url.Parse(c.URI)
	if err != nil {
		return err
	}

	b, err = s.client.Load(context.Background(), fmt.Sprintf("s3://%v%v/%v_schema.yaml", u.Host, path.Dir(u.Path), name))

	if err != nil {
		s.log.Warningf("error in downloading event schema. Load error %+v", err)
		return nil // Schema not found, continue without it
	}

	if len(b) == 0 {
		return nil // Schema not found, continue without it
	}

	if yaml.Unmarshal(b, &c.Tables.Timeseries.Schema) != nil {
		return err
	}

	return nil
}
