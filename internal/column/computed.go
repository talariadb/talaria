// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/loader"
	"github.com/kelindar/lua"
)

// Loader is the default loader to use for loading computed columns.
var Loader = loader.New()

// Default empty script
const emptyScript = `function main(row) 
	return null
end`

// NewComputed creates a new script from a string
func NewComputed(name string, typ typeof.Type, uriOrCode string, loader *script.Loader) (*Computed, error) {
	s, err := loader.Load(name, uriOrCode)
	if err != nil {
		return nil, err
	}

	return &Computed{
		code: s,
		typ:  typ,
	}, nil
}

// Computed represents a computed column
type Computed struct {
	code *lua.Script // The script associated with the column
	typ  typeof.Type // The type of the column
}

// Name returns the name of the column
func (c *Computed) Name() string {
	return c.code.Name()
}

// Type returns the type of the column
func (c *Computed) Type() typeof.Type {
	return c.typ
}

// Value computes the column value for the row
func (c *Computed) Value(row map[string]interface{}) (interface{}, error) {
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
