// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/kelindar/loader"
	loaderpkg "github.com/kelindar/loader"
	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	mlog "github.com/kelindar/talaria/internal/scripting/log"
	mnet "github.com/kelindar/talaria/internal/scripting/net"
	mstats "github.com/kelindar/talaria/internal/scripting/stats"
)

const (
	LuaLoaderTyp    = "lua"
	PluginLoaderTyp = "plugin"
)

// Computed represents a computed column
type Computed interface {
	Name() string
	Type() typeof.Type
	Value(map[string]interface{}) (interface{}, error)
}

type Loader struct {
	loader *loader.Loader // The loader to use to load and watch for code updates
}

type Handler interface {
	Load(columnName string, uriOrCode string) (Computed, error)
	String() string
}

type PluginHandler struct {
	Loader
	main         mainFunc
	functionName string
}
type LuaHandler struct {
	modules []lua.Module // The modules for the scripting environment
	Loader
}

func (h *PluginHandler) Load(columnName, uriOrCode string) (Computed, error) {
	log.Println("LoadGoPlugin: ", columnName, uriOrCode)
	// try to download it
	if err := h.watch(uriOrCode, h.updateGoPlugin); err != nil {
		return nil, err
	}

	return newGoFunc(columnName, h.main), nil
}

func (h *PluginHandler) String() string { return "plugin" }

type HandlerLoader struct {
	hs []Handler
}

func NewHandlerLoader(handlers ...Handler) *HandlerLoader {
	s := HandlerLoader{}
	for _, h := range handlers {
		s.hs = append(s.hs, h)
	}
	return &s
}

func (l *HandlerLoader) LoadHandler(uri string, monitor monitor.Monitor) Handler {
	h := "plugin"
	// h := parseHandler(uri)
	for _, d := range l.hs {
		if d.String() == h {
			return d
		}
	}

	return nil
}
func NewPluginHandler(functionName string) *PluginHandler {
	return &PluginHandler{
		Loader:       Loader{loaderpkg.New()},
		functionName: functionName,
	}
}

// NewComputed creates a new script from a string
func NewComputed(columnName, functionName string, outpuTyp typeof.Type, uriOrCode string, monitor monitor.Monitor) (Computed, error) {
	switch uriOrCode {
	case "make://identifier":
		return newIdentifier(columnName), nil
	case "make://timestamp":
		return newTimestamp(columnName), nil
	}

	pluginHandler := NewPluginHandler(functionName)
	luaHandler := NewLuaHandler([]lua.Module{
		mlog.New(monitor),
		mstats.New(monitor),
		mnet.New(monitor),
	})
	NewHandlerLoader(pluginHandler, luaHandler)
	return nil, nil

	// if strings.HasSuffix(uriOrCode, ".so") {
	// 	l := script.NewPluginHandler(functionName)
	// 	return l.LoadGoPlugin(columnName, uriOrCode)
	// }

	// loader := script.NewLuaHandler([]lua.Module{
	// 	mlog.New(monitor),
	// 	mstats.New(monitor),
	// 	mnet.New(monitor),
	// })
	// s, err := loader.LoadLua(columnName, uriOrCode)
	// if err != nil {
	// 	return nil, err
	// }

	// return &scripted{
	// 	code: s,
	// 	typ:  outpuTyp,
	// }, nil
}

// ------------------------------------------------------------------------------------------------------------

// scripted represents a computed column computed through a lua script
type scripted struct {
	code *lua.Script // The script associated with the column
	typ  typeof.Type // The type of the column
}

// Name returns the name of the column
func (c *scripted) Name() string {
	return c.code.Name()
}

// Type returns the type of the column
func (c *scripted) Type() typeof.Type {
	return c.typ
}

// Value computes the column value for the row
func (c *scripted) Value(row map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Run the script
	out, err := c.code.Run(ctx, row)
	if err != nil {
		return nil, err
	}

	// If there's no new row generated, return nil
	if out.Type() == lua.TypeNil {
		return nil, nil
	}

	switch c.typ {
	case typeof.Bool:
		if v, ok := out.(lua.Bool); ok {
			return bool(v), nil
		}
	case typeof.Int32:
		if v, ok := out.(lua.Number); ok {
			return int32(v), nil
		}
	case typeof.Int64, typeof.Timestamp:
		if v, ok := out.(lua.Number); ok {
			return int64(v), nil
		}
	case typeof.Float64:
		if v, ok := out.(lua.Number); ok {
			return float64(v), nil
		}
	case typeof.String, typeof.JSON:
		if v, ok := out.(lua.String); ok {
			return string(v), nil
		}
	}

	// Type mismatch
	return nil, fmt.Errorf("script expects %s type but got %T", c.typ.String(), out)
}

// ------------------------------------------------------------------------------------------------------------

// identifier represents a computed column that generates an event ID
type identifier struct {
	seq  uint32 // Sequence counter
	rnd  uint32 // Random component
	name string // Name of the column
}

// newIdentifier creates a new ID generator column
func newIdentifier(name string) *identifier {
	b := make([]byte, 4)
	rand.Read(b)
	uniq := binary.BigEndian.Uint32(b)

	return &identifier{
		seq:  0,
		rnd:  uniq,
		name: name,
	}
}

// Name returns the name of the column
func (c *identifier) Name() string {
	return c.name
}

// Type returns the type of the column
func (c *identifier) Type() typeof.Type {
	return typeof.String
}

// Value computes the column value for the row
func (c *identifier) Value(row map[string]interface{}) (interface{}, error) {
	id := make([]byte, 16)
	binary.BigEndian.PutUint64(id[0:8], uint64(time.Now().UTC().UnixNano()))
	binary.BigEndian.PutUint32(id[8:12], atomic.AddUint32(&c.seq, 1))
	binary.BigEndian.PutUint32(id[12:16], c.rnd)
	return hex.EncodeToString(id), nil
}

// ------------------------------------------------------------------------------------------------------------

// Timestamp represents a timestamp computed column
type timestamp struct {
	name string // Name of the column
}

// newIdentifier creates a new ID generator column
func newTimestamp(name string) *timestamp {
	return &timestamp{
		name: name,
	}
}

// Name returns the name of the column
func (c *timestamp) Name() string {
	return c.name
}

// Type returns the type of the column
func (c *timestamp) Type() typeof.Type {
	return typeof.Timestamp
}

// Value computes the column value for the row
func (c *timestamp) Value(row map[string]interface{}) (interface{}, error) {
	return time.Now().UTC().Unix(), nil
}
