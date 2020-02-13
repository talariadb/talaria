// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/grab/talaria/proto"
	"github.com/myteksi/hystrix-go/hystrix"
	"google.golang.org/grpc"
)

const (
	commandName        = "talaria"
	defaultDialTimeout = 5 * time.Second
)

var (
	// ErrUnableToConnect is error when client is unable connect to rpc server
	ErrUnableToConnect = errors.New("unable to connect Talaria server")
)

// Client represents a client for Talaria.
type Client struct {
	netconf netconf          // The network pool configuration.
	ingress pb.IngressClient // The underlying service.
	conn    *grpc.ClientConn // The underlying connection
}

// Dial creates a new client and connect to Talaria grpc server.
func Dial(address string, options ...Option) (*Client, error) {
	c := &Client{
		netconf: netconf{
			CircuitOptions: map[string]hystrix.CommandConfig{
				commandName: {
					Timeout:               int(defaultDialTimeout / time.Millisecond),
					MaxConcurrentRequests: hystrix.DefaultMaxConcurrent,
					ErrorPercentThreshold: hystrix.DefaultErrorPercentThreshold,
				},
			},
			Address:     address,
			DialTimeout: defaultDialTimeout,
		},
	}

	// Apply the options to overwrite the defaults
	for _, option := range options {
		option(c)
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	hystrix.Configure(c.netconf.CircuitOptions)
	return c, nil
}

func (c *Client) connect() error {
	var conn *grpc.ClientConn
	timeoutCtx, _ := context.WithTimeout(context.Background(), c.netconf.DialTimeout)
	dialOptions := []grpc.DialOption{}
	if !c.netconf.NonBlocking {
		dialOptions = append(dialOptions, grpc.WithBlock())
	}

	if c.isConnectionInsecure() {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(c.netconf.Credentials))
	}

	conn, err := grpc.DialContext(timeoutCtx, c.netconf.Address, dialOptions...)
	if err != nil {
		fmt.Println("err", err)
		return ErrUnableToConnect
	}
	c.ingress = pb.NewIngressClient(conn)
	return nil
}

func (c *Client) isConnectionInsecure() bool {
	return c.netconf.Credentials == nil
}

// IngestBatch sends a batch of events to Talaria server.
func (c *Client) IngestBatch(ctx context.Context, batch pb.Batch) error {
	req := &pb.IngestRequest{
		Data: &pb.IngestRequest_Batch{
			Batch: &batch,
		},
	}

	err := hystrix.Do(commandName, func() error {
		_, err := c.ingress.Ingest(ctx, req)
		return err
	}, nil)
	return err
}

// Close connection
func (c *Client) Close() error {
	return c.conn.Close()
}
