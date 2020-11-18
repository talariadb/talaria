// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package client

import (
	"context"
	"errors"
	"time"

	pb "github.com/kelindar/talaria/proto"
	"github.com/myteksi/hystrix-go/hystrix"
	"github.com/sercand/kuberesolver/v3"
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

func init() {
	kuberesolver.RegisterInCluster()
}

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
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.netconf.DialTimeout)
	defer cancel()

	var dialOptions []grpc.DialOption
	if c.netconf.LoadBalancer != "" {
		dialOptions = append(dialOptions, grpc.WithBalancerName(c.netconf.LoadBalancer))
	}

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
		return ErrUnableToConnect
	}
	c.ingress = pb.NewIngressClient(conn)
	return nil
}

func (c *Client) isConnectionInsecure() bool {
	return c.netconf.Credentials == nil
}

// IngestBatch sends a batch of events to Talaria server.
func (c *Client) IngestBatch(ctx context.Context, batch []Event) error {
	encoded := newEncoder().Encode(batch)
	req := &pb.IngestRequest{
		Data: &pb.IngestRequest_Batch{
			Batch: encoded,
		},
	}

	return hystrix.Do(commandName, func() error {
		_, err := c.ingress.Ingest(ctx, req)
		return err
	}, nil)
}

// IngestURL sends a request to Talaria to ingest a file from a specific URL.
func (c *Client) IngestURL(ctx context.Context, url string) error {
	return hystrix.Do(commandName, func() error {
		_, err := c.ingress.Ingest(ctx, &pb.IngestRequest{
			Data: &pb.IngestRequest_Url{
				Url: url,
			},
		})
		return err
	}, nil)
}

// IngestCSV sends a set of comma-separated file to Talaria to ingest.
func (c *Client) IngestCSV(ctx context.Context, data []byte) error {
	return hystrix.Do(commandName, func() error {
		_, err := c.ingress.Ingest(ctx, &pb.IngestRequest{
			Data: &pb.IngestRequest_Csv{
				Csv: data,
			},
		})
		return err
	}, nil)
}

// IngestORC sends an ORC-encoded file to Talaria to ingest.
func (c *Client) IngestORC(ctx context.Context, data []byte) error {
	return hystrix.Do(commandName, func() error {
		_, err := c.ingress.Ingest(ctx, &pb.IngestRequest{
			Data: &pb.IngestRequest_Orc{
				Orc: data,
			},
		})
		return err
	}, nil)
}

// Close connection
func (c *Client) Close() error {
	return c.conn.Close()
}
