// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	logTag = "config"
)

// Config global
type Config struct {
	Hostname  string   `json:"hostname"`
	GRPC      *GRPC    `json:"grpc"`
	DataDir   string   `json:"dataDir"`
	AwsRegion string   `json:"awsRegion"`
	Env       string   `json:"env"`
	Domain    string   `json:"domain"`
	Sqs       *SQS     `json:"sqs"`
	Presto    *Presto  `json:"presto"`
	Storage   *Storage `json:"storage"`
	Statsd    *StatsD  `json:"statsd"`
}

// GRPC represents the configuration for gRPC server
type GRPC struct {
	Port int32 `json:"port"` // The port for the gRPC listener (default: 8080)
}

// SQS represents the aws SQS configuration
type SQS struct {
	QueueURL          string `json:"endpoint"`
	WaitTimeout       int64  `json:"waitTimeout"`       // in seconds
	VisibilityTimeout *int64 `json:"visibilityTimeout"` // in seconds
	MaxRetries        int    `json:"maxRetries"`
}

// Presto represents the Presto configuration
type Presto struct {
	Port   int32  `json:"port"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// Storage represents the storage configuration
type Storage struct {
	TTLInSec   int64  `json:"ttlInSec"`   // The ttl for the storage, defaults to 1 hour.
	KeyColumn  string `json:"keyColumn"`  // The column to use as key (metric), defaults to 'event'.
	TimeColumn string `json:"timeColumn"` // The column to use as time, defaults to 'tsi'.
}

// StatsD represents the configuration for statsD client
type StatsD struct {
	Host string `json:"host"`
	Port int64  `json:"port"`
}

// Load loads the configuration
func Load(envVar string) *Config {
	cfg := &Config{
		Presto: &Presto{
			Port: 8042,
		},
		Storage: &Storage{
			TTLInSec:   3600,
			KeyColumn:  "event",
			TimeColumn: "tsi",
		},
		GRPC: &GRPC{
			Port: 8080,
		},
	}

	// Load the configuration
	if err := loadJSONEnvPath(envVar, cfg); err != nil {
		panic(fmt.Errorf("failed to load config file with error %s", err))
	}

	return cfg
}

// LoadJSONEnvPath gets your config from the json file provided by env var,
// and fills your struct with the option
func loadJSONEnvPath(envVar string, config interface{}) error {
	if config == nil {
		return errors.New("configuration is empty")
	}

	filename := os.Getenv(envVar)
	if filename == "" {
		return fmt.Errorf("Env var is empty: %s", envVar)
	}
	log.Printf("loading config from envVar %s, file = %s", envVar, filename)
	return loadJSONFile(filename, config)
}

// Loader represents a configuration loader delegate
type loader func(string) ([]byte, error)

// LoadJSONFile gets your config from the json file,
// and fills your struct with the option
func loadJSONFile(filename string, config interface{}) error {
	if config == nil {
		return errors.New("configuration is empty")
	}

	// Default to loading from file, for safety
	loadConfig := loader(loadFromFile)

	// If the filename provided is actually an HTTP or HTTPS uri, let's load from there
	// In future, we can add ucm://
	if strings.HasPrefix(filename, "http://") || strings.HasPrefix(filename, "https://") {
		loadConfig = loader(loadFromHTTP)
	}

	// If the URL points to S3, use S3 SDK to load the configuration from
	if strings.HasPrefix(filename, "s3://") {
		loadConfig = loader(loadFromS3)
	}

	// Load the configuration
	bytes, err := loadConfig(filename)
	if err != nil {
		return err
	}
	json.Unmarshal(bytes, config)
	return nil
}

// loads a file from HTTP
func loadFromHTTP(uri string) ([]byte, error) {
	resp, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Write the body to file
	log.Printf("%s : loading config from HTTP %s", logTag, uri)
	return ioutil.ReadAll(resp.Body)
}

// loads a file from OS File
func loadFromFile(uri string) ([]byte, error) {
	log.Printf("%s : loading config from OS File %s", logTag, uri)
	return ioutil.ReadFile(uri)
}

// loads a file from S3
func loadFromS3(uri string) ([]byte, error) {
	log.Printf("%s : loading config from S3 %s", logTag, uri)

	// Parse the URL
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Create the session
	conf := aws.NewConfig()
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	// Download the file
	w := &aws.WriteAtBuffer{}
	c := s3manager.NewDownloader(sess)
	_, err = c.Download(w, &s3.GetObjectInput{
		Bucket: aws.String(u.Host),
		Key:    aws.String(u.Path),
	})
	if err != nil {
		return nil, err
	}

	// Successfully downloaded the configuration
	return w.Bytes(), nil
}
