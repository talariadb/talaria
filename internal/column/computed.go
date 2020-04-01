// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/grab/async"
	"github.com/grab/talaria/internal/encoding/typeof"
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
func NewComputed(name string, typ typeof.Type, uriOrCode string, modules ...lua.Module) (*Computed, error) {

	// Create an empty script, we'll update it right away
	s, err := lua.FromString(name, emptyScript, modules...)
	if err != nil {
		return nil, err
	}

	// If the string is actually a URL, try to download it
	if err := watch(uriOrCode, s.Update); err != nil {
		return nil, err
	}

	return &Computed{
		code: s,
		typ:  typ,
	}, nil
}

func watch(uriOrCode string, onUpdate func(io.Reader) error) error {
	if _, err := url.Parse(uriOrCode); err != nil {
		return onUpdate(strings.NewReader(uriOrCode)) // Assume it's the actual lua code
	}

	// Start watching on the URL
	updates := Loader.Watch(context.Background(), uriOrCode, 5*time.Minute)
	u := <-updates
	if u.Err != nil {
		return u.Err
	}

	// Read the updates asynchronously
	async.Invoke(context.Background(), func(ctx context.Context) (interface{}, error) {
		for u := range updates {
			if u.Err == nil {
				_ = onUpdate(bytes.NewReader(u.Data))
			}
		}
		return nil, nil
	})

	// Perform a first update
	return onUpdate(bytes.NewReader(u.Data))
}

// ------------------------------------------------------------------------------------------

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
