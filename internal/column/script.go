package column

import (
	"fmt"

	"github.com/kelindar/talaria/internal/encoding/typeof"
)

type mainFunc = func(map[string]interface{}) (interface{}, error)

// goFunc represents a go computed column
type goFunc struct {
	name string      // Name of the column
	typ  typeof.Type // The type of the column
	main mainFunc
}

// newGoFunc ..
func newGoFunc(name string, function mainFunc) *goFunc {
	return &goFunc{
		name: name,
		main: function,
	}
}

// Name returns the name of the column
func (c *goFunc) Name() string {
	return c.name
}

// Type returns the type of the column
func (c *goFunc) Type() typeof.Type {
	return typeof.String
}

// Value computes the column value for the row
func (c *goFunc) Value(row map[string]interface{}) (interface{}, error) {
	fmt.Println("before is ", row)
	res, err := c.main(row)
	fmt.Println("after, data is ", res, err)
	return c.main(row)
}
