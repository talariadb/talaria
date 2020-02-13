// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package cluster

import (
	"net"
	"strconv"
	"strings"

	"github.com/emitter-io/address"
	"github.com/hashicorp/memberlist"
)

const (
	ctxTag = "cluster"
)

// Membership represents a contract which returns a list of IP addresses.
type Membership interface {
	Members() []string
	Addr() string
}

// Cluster represents a cluster management/discovery mechanism using gossip.
type Cluster struct {
	list *memberlist.Memberlist
	addr string
}

// New creates a new gossip cluster.
func New(port int) *Cluster {
	cfg := memberlist.DefaultWANConfig()
	cfg.BindPort = port
	cfg.AdvertisePort = port
	cfg.AdvertiseAddr = getAddress()
	list, err := memberlist.Create(cfg)
	if err != nil {
		panic("failed to create gossip memberlist: " + err.Error())
	}

	return &Cluster{
		list: list,
		addr: net.JoinHostPort(cfg.AdvertiseAddr, strconv.FormatInt(int64(cfg.AdvertisePort), 10)),
	}
}

// Members returns the current set of nodes available.
func (c *Cluster) Members() (nodes []string) {
	for _, m := range c.list.Members() {
		nodes = append(nodes, m.Addr.String())
	}
	return
}

// Addr returns the advertised address
func (c *Cluster) Addr() string {
	return c.addr
}

// Join attempts to join a cluster.
func (c *Cluster) Join(addresses ...string) error {
	_, err := c.list.Join(addresses)
	return err
}

// JoinHostname attempts to join all of the nodes behind a hostname
func (c *Cluster) JoinHostname(name string) error {

	// Get the addresses by performing a DNS lookup, this should not fail
	addr, err := net.LookupHost(name)
	if err != nil {
		return err
	}

	return c.Join(addr...)
}

// Close closes the gossip
func (c *Cluster) Close() error {
	return c.list.Shutdown()
}

// GetAddress returns our own IP Address.
func getAddress() string {
	interfaces, err := address.GetPrivate()
	if err != nil {
		panic(err)
	}

	// Prefer 10.x.x.x
	private := interfaces[0].IP.String()
	for _, addr := range interfaces {
		if strings.HasPrefix(addr.IP.String(), "10.") {
			private = addr.IP.String()
		}
	}
	return private
}
