package block

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFileForParquet = "../../../test/test2.parquet"
const testFileForParquetWithMissingAttributes = "../../../test/testfilewithmissingatts.parquet"

func TestFromParquet_Nested(t *testing.T) {
	RunTest(t, testFileForParquet)
}

func TestFromParquet_MissingAttributes(t *testing.T) {
	RunTest(t, testFileForParquetWithMissingAttributes)
}

func RunTest(t *testing.T, testFileName string) {
	o, err := ioutil.ReadFile(testFileName)
	assert.NotEmpty(t, o)
	assert.NoError(t, err)

	apply := Transform(nil)
	b, err := FromParquetBy(o, "foo", nil, apply)
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(b))
}