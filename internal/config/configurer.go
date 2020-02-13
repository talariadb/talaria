// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

// Configurer is an interface for any component which will provide the config.
type Configurer interface {
	// Config reads the config in json and populate the config
	Configure(cfg *Config) error
}
