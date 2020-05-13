package net

import (
	"errors"
	"net"
	"strings"

	"github.com/emitter-io/address"
	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/monitor"
)

const ctxTag = "net"

// New creates a new lua module exposing the stats
func New(monitor monitor.Monitor) lua.Module {
	l := &libnet{monitor}
	m := &lua.NativeModule{
		Name:    "net",
		Version: "1.0",
	}

	m.Register("getmac", l.GetMac)
	m.Register("addr", l.Addr)
	return m
}

type libnet struct {
	monitor monitor.Monitor
}

// GetMac returns the mac address
func (l *libnet) GetMac() (lua.String, error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, ifa := range ifas {
		if addr := ifa.HardwareAddr.String(); addr != "" {
			return lua.String(addr), nil
		}
	}
	return "", errors.New("no valid mac address found")
}

// Addr returns the address of a specific host
func (l *libnet) Addr(host lua.String) (lua.String, error) {
	if !strings.Contains(string(host), ":") {
		host = host + ":80"
	}

	addr, err := address.Parse(string(host), 0)
	if err != nil {
		return "", err
	}

	return lua.String(addr.IP.String()), nil
}
