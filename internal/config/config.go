// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

import (
	"context"
	"time"

	"github.com/kelindar/talaria/internal/encoding/typeof"
)

// Configurer is an interface for any component which will provide the config.
type Configurer interface {
	Configure(*Config) error
}

// BadgerDefault represents a default way of configuring badger
type BadgerDefault string

// Various Badger options
const (
	BadgerStorage       BadgerDefault = "storage"
	BadgerIngestion     BadgerDefault = "ingestion"
	BadgerDefaultOption BadgerDefault = "default"
)

// Config global
type Config struct {
	URI      string     `json:"uri" yaml:"uri" env:"URI"`
	Env      string     `json:"env" yaml:"env" env:"ENV"`             // The environment (eg: prd, stg)
	AppName  string     `json:"appName" yaml:"appName" env:"APPNAME"` // app name used for monitoring
	Domain   string     `json:"domain" yaml:"domain" env:"DOMAIN"`
	Readers  Readers    `json:"readers" yaml:"readers" env:"READERS"`
	Writers  Writers    `json:"writers" yaml:"writers" env:"WRITERS"`
	Storage  Storage    `json:"storage" yaml:"storage" env:"STORAGE"`
	Tables   Tables     `json:"tables" yaml:"tables"`
	Statsd   *StatsD    `json:"statsd,omitempty" yaml:"statsd" env:"STATSD"`
	Computed []Computed `json:"computed" yaml:"computed" env:"COMPUTED"`
	K8s      *K8s       `json:"k8s,omitempty" yaml:"k8s" env:"K8S"`
}

type K8s struct {
	ProbePort int32 `json:"probePort" yaml:"probePort" env:"PROBEPORT"` // The port which is used for liveness and readiness probes (default: 8080)
}

// Tables is a list of table configs
type Tables map[string]Table

// Table is the config for the timeseries table
type Table struct {
	TTL     int64       `json:"ttl,omitempty" yaml:"ttl" env:"TTL"`          // The ttl (in seconds) for the storage, defaults to 1 hour.
	HashBy  string      `json:"hashBy,omitempty" yaml:"hashBy" env:"HASHBY"` // The column to use as key (metric), defaults to 'event'.
	SortBy  string      `json:"sortBy,omitempty" yaml:"sortBy" env:"SORTBY"` // The column to use as time, defaults to 'tsi'.
	Schema  string      `json:"schema" yaml:"schema" env:"SCHEMA"`           // The schema of the table
	Compact *Compaction `json:"compact" yaml:"compact" env:"COMPACT"`        // The compaction configuration for the table
	Streams Streams     `json:"streams" yaml:"streams" env:"STREAMS"`        // The streams to stream data to for data in this table
}

// Storage is the location to write the data
type Storage struct {
	Badger
	Directory string `json:"dir" yaml:"dir" env:"DIR"`
}

// Badger configures badger K-V store that we use underlying.
// This will help to tune the options to optimize for various uses cases like bigger files, point query or range queries.
type Badger struct {
	SyncWrites          *bool         `json:"syncWrites" yaml:"syncWrites" env:"SYNCWRITES"`                            // Whether to sync writes to disk before ack. defaults to true
	ValueLogMaxEntries  *uint32       `json:"valueLogMaxEntries" yaml:"valueLogMaxEntries" env:"VALUELOGMAXENTRIES"`    // Maximum number of entries a value log file can hold approximately. defaults to 5000
	MaxTableSize        *int64        `json:"maxTableSize" yaml:"maxTableSize" env:"MAXTABLESIZE"`                      // Maximum size in bytes for each LSM table or file.
	LevelOneSize        *int64        `json:"levelOneSize" yaml:"levelOneSize" env:"LEVELONESIZE"`                      // Maximum total size in bytes for Level 1. defaults to 1 Million
	LevelSizeMultiplier *int          `json:"levelSizeMultiplier" yaml:"levelSizeMultiplier" env:"LEVELSIZEMULTIPLIER"` // The ratio between the maximum sizes of contiguous levels in the LSM. defaults to 10
	MaxLevels           *int          `json:"maxLevels" yaml:"maxLevels" env:"MAXLEVELS"`                               // Maximum number of levels of compaction allowed in the LSM. defaults to 7
	Default             BadgerDefault `json:"default" yaml:"default" env:"DEFAULT"`                                     // default badger option to optimize for storage, ingestion or default that badger provides
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
	VisibilityTimeout int64  `json:"visibilityTimeout,omitempty" yaml:"visibilityTimeout" env:"VISIBILITYTIMEOUT"` // in seconds
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

// Compaction represents a configuration for compaction sinks
type Compaction struct {
	Sinks    `yaml:",inline"`
	NameFunc string `json:"nameFunc" yaml:"nameFunc" env:"NAMEFUNC"` // The lua script to compute file name given a row
	Interval int    `json:"interval" yaml:"interval" env:"INTERVAL"` // The compaction interval, in seconds
}

// Streams are lists of sinks to be streamed to
type Streams []Sinks

// Sinks represents a configuration for writer sinks
type Sinks struct {
	S3       *S3Sink       `json:"s3" yaml:"s3"`              // The S3 writer configuration
	Azure    *AzureSink    `json:"azure" yaml:"azure"`        // The Azure writer configuration
	BigQuery *BigQuerySink `json:"bigquery" yaml:"bigquery" ` // The Big Query writer configuration
	GCS      *GCSSink      `json:"gcs" yaml:"gcs" `           // The Google Cloud Storage writer configuration
	File     *FileSink     `json:"file" yaml:"file" `         // The local file system writer configuration
	Talaria  *TalariaSink  `json:"talaria" yaml:"talaria" `   // The Talaria writer configuration
	PubSub   *PubSubSink   `json:"pubsub" yaml:"pubsub" `     // The Google Pub/Sub writer configuration
}

// S3Sink represents a sink for AWS S3 and compatible stores.
type S3Sink struct {
	Region      string `json:"region" yaml:"region" env:"REGION"`                // The region of AWS bucket
	Bucket      string `json:"bucket" yaml:"bucket" env:"BUCKET"`                // The name of AWS bucket
	Prefix      string `json:"prefix" yaml:"prefix" env:"PREFIX"`                // The prefix to add
	Endpoint    string `json:"endpoint" yaml:"endpoint" env:"ENDPOINT"`          // The custom endpoint to use
	SSE         string `json:"sse" yaml:"sse" env:"SSE"`                         // The server side encryption to use
	AccessKey   string `json:"accessKey" yaml:"accessKey" env:"ACCESSKEY"`       // The optional static access key
	SecretKey   string `json:"secretKey" yaml:"secretKey" env:"SECRETKEY"`       // The optional static secret key
	Concurrency int    `json:"concurrency" yaml:"concurrency" env:"CONCURRENCY"` // The S3 upload concurrency
}

// AzureSink reprents a sink to Azure
type AzureSink struct {
	Container string `json:"container" yaml:"container" env:"CONTAINER"` // The container name
	Prefix    string `json:"prefix" yaml:"prefix" env:"PREFIX"`          // The prefix to add
}

// BigQuerySink reprents a sink to Google Big Query
type BigQuerySink struct {
	Project string `json:"project" yaml:"project" env:"PROJECT"` // The project ID
	Dataset string `json:"dataset" yaml:"dataset" env:"DATASET"` // The dataset ID
	Table   string `json:"table" yaml:"table" env:"TABLE"`       // The table ID
}

// GCSSink represents a sink to Google Cloud Storage
type GCSSink struct {
	Bucket string `json:"bucket" yaml:"bucket" env:"BUCKET"` // The name of the bucket
	Prefix string `json:"prefix" yaml:"prefix" env:"PREFIX"` // The prefix to add
}

// FileSink represents a sink to the local file system
type FileSink struct {
	Directory string `json:"dir" yaml:"dir" env:"DIR"`
}

// PubSubSink represents a stream to Google Pub/Sub
type PubSubSink struct {
	Project string `json:"project" yaml:"project" env:"PROJECT"`
	Topic   string `json:"topic" yaml:"topic" env:"TOPIC"`
	Filter  string `json:"filter" yaml:"filter" env:"FILTER"`
	Encoder string `json:"encoder" yaml:"encoder" env:"ENCODER"`
}

// TalariaSink represents a sink to an instance of Talaria
type TalariaSink struct {
	Endpoint              string         `json:"endpoint" yaml:"endpoint" env:"ENDPOINT"`                    // The second Talaria endpoint
	CircuitTimeout        *time.Duration `json:"timeout" yaml:"timeout" env:"TIMEOUT"`                       // The timeout (in seconds) for requests to the second Talaria
	MaxConcurrent         *int           `json:"concurrency" yaml:"concurrency" env:"CONCURRENCY"`           // The number of concurrent requests permissible
	ErrorPercentThreshold *int           `json:"errorThreshold" yaml:"errorThreshold" env:"ERROR_THRESHOLD"` // The percentage of failed requests tolerated
}

// Func represents a config function
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
