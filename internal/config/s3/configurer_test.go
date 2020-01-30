package s3

import (
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	// initialize config
	c := &config.Config{}
	st := static.New()
	err := st.Configure(c)
	assert.Nil(t, err)
	c.URI = "s3://dev-ap-southeast-1-go-app-configs.s3-ap-southeast-1.amazonaws.com/conf-server-conf-stg.json"
	c.Tables.Timeseries.Name = "abc"

	var down downloadMock
	down = func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error) {
		return int64(0), nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	s3C := Configurer{
		client: cl,
	}

	err = s3C.Configure(c)
	fmt.Printf("%+v\n", c.Tables.Timeseries)
	assert.Nil(t, err)

}
