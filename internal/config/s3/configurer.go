// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"context"
	"sync"

	"github.com/kelindar/loader"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"gopkg.in/yaml.v2"
)

type downloader interface {
	Load(ctx context.Context, uri string) ([]byte, error)
}

// Configurer to fetch configuration from a s3 object
type Configurer struct {
	sync.Mutex
	client downloader
	log    logging.Logger
}

// New creates a new S3 configurer.
func New(log logging.Logger) *Configurer {
	return NewWith(loader.New(), log)
}

// SetLogger to set the logger after initialization
func (s *Configurer) SetLogger(lo logging.Logger) {
	s.Lock()
	defer s.Unlock()
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
	s.Lock()
	defer s.Unlock()
	if c.URI == "" {
		return nil
	}

	// download the config
	b, err := s.client.Load(context.Background(), c.URI)
	if err != nil {
		s.log.Warningf("error in downloading config from s3. Load error %+v", err)
		return nil // Unable to load, skip
	}

	if err := yaml.Unmarshal(b, c); err != nil {
		return err
	}
	return nil
}
