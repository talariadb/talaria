package column

// // goFunc represents a go computed column
// type goFunc struct {
// 	name string      // Name of the column
// 	typ  typeof.Type // The type of the column
// 	main script.MainFunc
// }

// // newGoFunc ..
// func newGoFunc(name string, function script.MainFunc) goFunc {
// 	return goFunc{
// 		name: name,
// 		main: function,
// 	}
// }

// // Name returns the name of the column
// func (c *goFunc) Name() string {
// 	return c.name
// }

// // Type returns the type of the column
// func (c *goFunc) Type() typeof.Type {
// 	return typeof.String
// }

// // Value computes the column value for the row
// func (c *goFunc) Value(row map[string]interface{}) (interface{}, error) {
// 	fmt.Println("before is ", row)
// 	res, err := c.main(row)
// 	fmt.Println("after, data is ", res, err)
// 	return c.main(row)
// }
