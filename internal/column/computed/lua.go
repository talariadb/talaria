package column

import (
	"github.com/kelindar/talaria/internal/encoding/typeof"
	script "github.com/kelindar/talaria/internal/scripting"
)

type loadComputed struct {
	name   string // Name of the column
	loader script.Handler
	typ    typeof.Type
}

func (l *loadComputed) Name() string {
	return l.Name()
}
func (l *loadComputed) Type() typeof.Type {
	return l.typ
}
func (l *loadComputed) Value(row map[string]interface{}) (interface{}, error) {
	return l.loader.Value(row)
}

// // scripted represents a computed column computed through a lua script
// type scripted struct {
// 	name string      // Name of the column
// 	code *lua.Script // The script associated with the column
// 	typ  typeof.Type // The type of the column
// }

// // Name returns the name of the column
// func (c *scripted) Name() string {
// 	return c.code.Name()
// }

// // Type returns the type of the column
// func (c *scripted) Type() typeof.Type {
// 	return c.typ
// }

// // Value computes the column value for the row
// func (c *scripted) Value(row map[string]interface{}) (interface{}, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	// Run the script
// 	out, err := c.code.Run(ctx, row)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// If there's no new row generated, return nil
// 	if out.Type() == lua.TypeNil {
// 		return nil, nil
// 	}

// 	switch c.typ {
// 	case typeof.Bool:
// 		if v, ok := out.(lua.Bool); ok {
// 			return bool(v), nil
// 		}
// 	case typeof.Int32:
// 		if v, ok := out.(lua.Number); ok {
// 			return int32(v), nil
// 		}
// 	case typeof.Int64, typeof.Timestamp:
// 		if v, ok := out.(lua.Number); ok {
// 			return int64(v), nil
// 		}
// 	case typeof.Float64:
// 		if v, ok := out.(lua.Number); ok {
// 			return float64(v), nil
// 		}
// 	case typeof.String, typeof.JSON:
// 		if v, ok := out.(lua.String); ok {
// 			return string(v), nil
// 		}
// 	}

// 	// Type mismatch
// 	return nil, fmt.Errorf("script expects %s type but got %T", c.typ.String(), out)
// }
