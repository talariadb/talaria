package script

import (
	"context"
	"fmt"
	"log"
	"time"

	loaderpkg "github.com/kelindar/loader"
	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/encoding/typeof"
)

type LuaLoader struct {
	modules []lua.Module // The modules for the scripting environment
	Loader
	code *lua.Script // The script associated with the column
	typ  typeof.Type // The type of the column
}

// NewLuaLoader creates a new loader that can be used to load scripts
func NewLuaLoader(luaModules []lua.Module, typ typeof.Type) *LuaLoader {
	return &LuaLoader{
		modules: luaModules,
		Loader:  Loader{loaderpkg.New()},
		typ:     typ,
	}
}
func (l *LuaLoader) String() string { return luaType }

// Load creates a new script from code or URL and starts a watching if needed
func (l *LuaLoader) Load(uriOrCode string) (Handler, error) {
	log.Println("LoadLua: ", uriOrCode)

	// Default empty script
	const emptyScript = `function main(row)
		return null
	end`

	// Create an empty script, we'll update it right away
	s, err := lua.FromString("luaScript", emptyScript, l.modules...)
	if err != nil {
		return nil, err
	}

	// If the string is actually a URL, try to download it
	if err := l.watch(uriOrCode, s.Update); err != nil {
		return nil, err
	}

	return &LuaLoader{
		code: s,
		typ:  l.typ,
	}, nil
}

func (l *LuaLoader) Value(row map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Run the script
	out, err := l.code.Run(ctx, row)
	if err != nil {
		return nil, err
	}

	// If there's no new row generated, return nil
	if out.Type() == lua.TypeNil {
		return nil, nil
	}

	switch l.typ {
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
	return nil, fmt.Errorf("script expects %s type but got %T", l.typ.String(), out)
}
