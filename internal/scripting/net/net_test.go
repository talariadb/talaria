package net

import (
	"context"
	"testing"

	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
)

func Test_GetMac(t *testing.T) {
	m := New(monitor.NewNoop())
	assert.NotNil(t, m)

	// Create a new script
	s, err := lua.FromString("test", `
	local net = require("net")

	function main() 
		local addr = net.getmac()
		return addr
	end`, m)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	addr, err := s.Run(context.Background())
	assert.NoError(t, err)
	assert.Contains(t, []rune(addr.(lua.String)), rune(':'))
}

func Test_Addr(t *testing.T) {
	m := New(monitor.NewNoop())
	assert.NotNil(t, m)

	// Create a new script
	s, err := lua.FromString("test", `
	local net = require("net")

	function main() 
		return net.addr("localhost")
	end`, m)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	addr, err := s.Run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1", string(addr.(lua.String)))
}
