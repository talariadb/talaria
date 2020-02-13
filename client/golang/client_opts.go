// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package client

import (
	"time"

	"github.com/myteksi/hystrix-go/hystrix"
	"google.golang.org/grpc/credentials"
)

// Option is a functional parameter used to configure the client.
type Option func(client *Client)

// netconf defines connection pool configuration for a gRPC service
type netconf struct {
	CircuitOptions map[string]hystrix.CommandConfig
	Address        string
	DialTimeout    time.Duration
	Credentials    credentials.TransportCredentials
	NonBlocking    bool // once set to true, the client will be returned before connection gets ready
}

// WithNetwork specifies the configuration for a connection.
func WithNetwork(dialTimeout time.Duration) Option {
	return func(client *Client) {
		client.netconf.DialTimeout = dialTimeout
	}
}

// WithCircuit specifies the configuration for the circuit breaker.
func WithCircuit(timeout time.Duration, maxConcurrent, errorThresholdPercent int) Option {
	return func(client *Client) {
		if client.netconf.CircuitOptions == nil {
			client.netconf.CircuitOptions = make(map[string]hystrix.CommandConfig, 1)
		}

		client.netconf.CircuitOptions[commandName] = hystrix.CommandConfig{
			Timeout:               int(timeout / time.Millisecond),
			MaxConcurrentRequests: maxConcurrent,
			ErrorPercentThreshold: errorThresholdPercent,
		}
	}
}

// WithCredential specfies the configuration for the gRPC credentials based on TLS
// with this one being set, connection will be created in secure manner
func WithCredential(credentials credentials.TransportCredentials) Option {
	return func(client *Client) {
		client.netconf.Credentials = credentials
	}
}

func WithNonBlock() Option {
	return func(client *Client) {
		client.netconf.NonBlocking = true
	}
}
