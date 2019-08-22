// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Config global
type Config struct {
	Port      int32          `json:"port"`
	Hostname  string         `json:"hostname"`
	DataDir   string         `json:"dataDir"`
	AwsRegion string         `json:"awsRegion"`
	Sqs       *SQSConfig     `json:"sqs"`
	Route     *RouteConfig   `json:"route"`
	Presto    *PrestoConfig  `json:"presto"`
	Storage   *StorageConfig `json:"storage"`
}

// SQSConfig represents the aws SQS configuration
type SQSConfig struct {
	Endpoint          string `json:"endpoint"`
	Retry             int    `json:"retry"`
	WaitTimeout       int64  `json:"waitTimeout"`
	VisibilityTimeout *int64 `json:"visibilityTimeout"` // in seconds
}

// RouteConfig represents the Route53 configuration
type RouteConfig struct {
	Domain string `json:"domain"`
	ZoneID string `json:"zoneID"`
}

// PrestoConfig represents the Presto configuration
type PrestoConfig struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// StorageConfig represents the storage configuration
type StorageConfig struct {
	TTLInSec int64 `json:"ttlInSec"`
}

// Load ...
func Load(envVar string) *Config {
	cfg := &Config{}
	LoadJSONEnvPathOrPanic(envVar, cfg)
	return cfg
}

// LoadJSONEnvPathOrPanic calls LoadJSONEnvPath but panics on error
func LoadJSONEnvPathOrPanic(envVar string, config interface{}) {
	if err := LoadJSONEnvPath(envVar, config); err != nil {
		panic(fmt.Errorf("failed to load config file with error %s", err))
	}
}

// LoadJSONEnvPath gets your config from the json file provided by env var,
// and fills your struct with the option
func LoadJSONEnvPath(envVar string, config interface{}) error {
	if config == nil {
		return errors.New("Config object is empty")
	}

	filename := os.Getenv(envVar)
	if filename == "" {
		return fmt.Errorf("Env var is empty: %s", envVar)
	}
	log.Printf("loading config from envVar %s, file = %s", envVar, filename)
	return LoadJSONFile(filename, config)
}

// LoadJSONFile gets your config from the json file,
// and fills your struct with the option
func LoadJSONFile(filename string, config interface{}) error {
	if config == nil {
		return errors.New("Config object is empty.")
	}

	// Load the configuration
	bytes, err := loadFromFile(filename)
	if err != nil {
		return err
	}
	json.Unmarshal(bytes, config)
	return nil
}

// loads a file from OS File
func loadFromFile(uri string) ([]byte, error) {
	log.Printf("%s : loading config from OS File %s", "config", uri)
	return ioutil.ReadFile(uri)
}
