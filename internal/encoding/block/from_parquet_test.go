package block

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

const testFileForParquet = "../../../test/test2.parquet"

func TestFromParquet_Nested(t *testing.T) {
	o, err := ioutil.ReadFile(testFileForParquet)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(nil)
	b, err := FromParquetBy(o, "foo", nil, apply)
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(b))
}