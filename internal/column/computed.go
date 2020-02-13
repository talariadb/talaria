// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/kelindar/loader"
	"github.com/kelindar/lua"
)

// NewComputed creates a new script from a string
func NewComputed(name string, typ typeof.Type, code string) (*Computed, error) {

	// If the string is actually a URL, try to download it
	var err error
	if uri, err := url.Parse(code); err == nil {
		code, err = download(uri.String())
		if err != nil {
			return nil, err
		}
	}

	// Create a script from the code
	s, err := lua.FromString(name, code)
	if err != nil {
		return nil, err
	}

	return &Computed{
		code: s,
		typ:  typ,
	}, nil
}

func download(uri string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	loader := loader.New()

	b, err := loader.Load(ctx, uri)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ------------------------------------------------------------------------------------------

// Computed represents a computed column
type Computed struct {
	code *lua.Script
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
