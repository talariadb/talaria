// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/grab/async"
	"golang.org/x/net/context"
)

// store stores the config and reloads it after 5 seconds
type store struct {
	config       atomic.Value
	configurers  []Configurer
	loadInterval time.Duration // the repeat interval in seconds to load the config
}

func newStore(li time.Duration, co []Configurer) *store {
	s := &store{
		loadInterval: li,
		configurers:  co,
	}

	c, err := s.value()
	if err != nil {
		panic("unable to load config")
	}
	s.config.Store(c)
	return s
}

// sets a watch to reload the config after every loadInterval seconds
func (cs *store) watch(ctx context.Context) {
	async.Repeat(ctx, cs.loadInterval, cs.reload)
}

// reloads the config
func (cs *store) reload(ctx context.Context) (interface{}, error) {
	newConfig, err := cs.value()
	if err != nil {
		return nil, err
	}
	cs.config.Store(newConfig)
	return nil, nil
}

// value ...
func (cs *store) value() (*Config, error) {
	c := &Config{}
	// Iterate through all the loaders to fill this config object
	for _, p := range cs.configurers {
		err := p.Configure(c)
		if err != nil {
			log.Printf("%s : error in loadig config %s", p, err)
			return nil, err
		}
	}
	return c, nil
}
