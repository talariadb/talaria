package s3

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type downloadMock func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error)

func (d downloadMock) DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error) {
	return d(ctx, w, input, options...)
}

func TestClient(t *testing.T) {
	var down downloadMock
	down = func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error) {
		return int64(0), nil
	}
	cl, err := newClient(down)
	assert.Nil(t, err)
	assert.NotNil(t, cl)

	_, err = cl.Download(context.Background(), "an", "cd")
	assert.Nil(t, err)
}
