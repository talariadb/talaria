// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

import (
	"context"
	"time"

	"github.com/grab/talaria/internal/encoding/typeof"
)

// Config global
type Config struct {
	URI      string     `json:"uri" yaml:"uri" env:"URI"`
	Env      string     `json:"env" yaml:"env" env:"ENV"` // The environment (eg: prd, stg)
	Domain   string     `json:"domain" yaml:"domain" env:"DOMAIN"`
	Readers  Readers    `json:"readers" yaml:"readers" env:"READERS"`
	Writers  Writers    `json:"writers" yaml:"writers" env:"WRITERS"`
	Storage  Storage    `json:"storage" yaml:"storage" env:"STORAGE"`
	Tables   Tables     `json:"tables" yaml:"tables" env:"TABLES"`
	Statsd   *StatsD    `json:"statsd,omitempty" yaml:"statsd" env:"STATSD"`
	Computed []Computed `json:"computed" yaml:"computed" env:"COMPUTED"`
}

// Tables is a list of table configs
type Tables struct {
	Timeseries *Timeseries `json:"timeseries,omitempty" yaml:"timeseries"  env:"TIMESERIES"`
	Log        *Log        `json:"log,omitempty" yaml:"log" env:"LOG"`
	Nodes      *Nodes      `json:"nodes,omitempty" yaml:"nodes" env:"NODES"`
}

// Timeseries is the config for the timeseries table
type Timeseries struct {
	Name   string         `json:"name" yaml:"name" env:"NAME"`                 // The name of the table
	TTL    int64          `json:"ttl,omitempty" yaml:"ttl" env:"TTL"`          // The ttl (in seconds) for the storage, defaults to 1 hour.
	HashBy string         `json:"hashBy,omitempty" yaml:"hashBy" env:"HASHBY"` // The column to use as key (metric), defaults to 'event'.
	SortBy string         `json:"sortBy,omitempty" yaml:"sortBy" env:"SORTBY"` // The column to use as time, defaults to 'tsi'.
	Schema *typeof.Schema `json:"schema" yaml:"schema" env:"SCHEMA"`           // The schema of the table
}

// Log is the config for log table
type Log struct {
	Name   string `json:"name" yaml:"name" env:"NAME"`                 // The name of the table
	TTL    int64  `json:"ttl,omitempty" yaml:"ttl" env:"TTL"`          // The ttl (in seconds) for the storage, defaults to 1 hour.
	SortBy string `json:"sortBy,omitempty" yaml:"sortBy" env:"SORTBY"` // The column to use as time, defaults to 'time'.
}

// Nodes
type Nodes struct {
	Name string `json:"name" yaml:"name" env:"NAME"` // The name of the table
}

// S3Compact stores the config for s3 compaction
type S3Compact struct {
	Region      string `json:"region" yaml:"region" env:"REGION"`                // The region of AWS bucket
	Bucket      string `json:"bucket" yaml:"bucket" env:"BUCKET"`                // The name of AWS bucket
	Concurrency int    `json:"concurrency" yaml:"concurrency" env:"CONCURRENCY"` // The S3 upload concurrency
	NameFunc    string `json:"nameFunc" yaml:"nameFunc" env:"NAMEFUNC"`          // The lua script to compute file name given a row
}

// Storage is the location to write the data
type Storage struct {
	Directory string     `json:"dir" yaml:"dir" env:"DIR"`
	S3Compact *S3Compact `json:"s3Compact" yaml:"s3Compact" env:"S3COMPACT"`
}

// Readers are ways to read the data
type Readers struct {
	Presto *Presto `json:"presto" yaml:"presto" env:"PRESTO"`
}

// Writers are sources to write data
type Writers struct {
	GRPC  *GRPC  `json:"grpc,omitempty" yaml:"grpc" env:"GRPC"`    // The GRPC ingress
	S3SQS *S3SQS `json:"s3sqs,omitempty" yaml:"s3sqs" env:"S3SQS"` // The S3SQS ingress
}

// GRPC represents the configuration for gRPC ingress
type GRPC struct {
	Port int32 `json:"port" yaml:"port" env:"PORT"` // The port for the gRPC listener (default: 8080)
}

// S3SQS represents the aws S3 SQS configuration
type S3SQS struct {
	Region            string `json:"region" yaml:"region" env:"REGION"`
	Queue             string `json:"queue" yaml:"queue" env:"QUEUE"`
	WaitTimeout       int64  `json:"waitTimeout,omitempty" yaml:"waitTimeout" env:"WAITTIMEOUT"`                   // in seconds
	VisibilityTimeout *int64 `json:"visibilityTimeout,omitempty" yaml:"visibilityTimeout" env:"VISIBILITYTIMEOUT"` // in seconds
	Retries           int    `json:"retries" yaml:"retries" env:"RETRIES"`
}

// Presto represents the Presto configuration
type Presto struct {
	Port   int32  `json:"port" yaml:"port" env:"PORT"`
	Schema string `json:"schema" yaml:"schema" env:"SCHEMA"`
}

// StatsD represents the configuration for statsD client
type StatsD struct {
	Host string `json:"host" yaml:"host" env:"HOST"`
	Port int64  `json:"port" port:"port" env:"PORT"`
}

// Computed represents a computed column
type Computed struct {
	Name string      `json:"name"`
	Type typeof.Type `json:"type"`
	Func string      `json:"func"`
}

type Func func() *Config

// Load iterates through all the providers and fills the config object.
// Order of providers is important as the the last provider can override the previous one
// It sets watch on the config for hot reload of the config
func Load(ctx context.Context, d time.Duration, configurers ...Configurer) Func {
	cs := newStore(d, configurers)
	cs.watch(ctx)
	return func() *Config {
		return cs.config.Load().(*Config)
	}
}
