// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package cluster

import (
	"context"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/emitter-io/address"
	"github.com/hashicorp/memberlist"
)

const (
	ctxTag = "cluster"
)

// Membership represents a contract which returns a list of IP addresses.
type Membership interface {
	Members() []string
}

// Cluster represents a cluster management/discovery mechanism using gossip.
type Cluster struct {
	list *memberlist.Memberlist
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
	}
}

// Members returns the current set of nodes available.
func (c *Cluster) Members() (nodes []string) {
	for _, m := range c.list.Members() {
		nodes = append(nodes, m.Addr.String())
	}
	return
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

// JoinAndSync attempts to join all of the nodes behind a hostname
func (c *Cluster) JoinAndSync(ctx context.Context, r53 *Route53, domainName, zoneID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:

			// Re-join by hostname
			if err := c.JoinHostname(domainName); err != nil {
				r53.monitor.ErrorWithStats(ctxTag, "join_hostname", "[join] "+err.Error())
			}

			// Get the target and update our A record
			if err := r53.Upsert(domainName, zoneID, c.Members(), 3000); err != nil {
				r53.monitor.ErrorWithStats(ctxTag, "upsert_members", "[r53] "+err.Error())
			}

			// Sleep for some random time
			time.Sleep(time.Duration(300+rand.Intn(300)) * time.Second)
		}
	}
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
