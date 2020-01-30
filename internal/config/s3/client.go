package s3

import (
	"context"
	"io"
	"runtime"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grab/talaria/internal/monitor/errors"
)

type downloader interface {
	DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error)
}

// Client interface to interact with S3
type Client interface {
	Download(ctx context.Context, bucket, key string) ([]byte, error)
}

type client struct {
	downloader downloader
}

// newClient a new S3 Client.
func newClient(dl downloader) (*client, error) {
	if dl != nil {
		return &client{
			downloader: dl,
		}, nil
	}

	// Create the session
	conf := aws.NewConfig()
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, errors.Internal("unable to create a session", err)
	}

	return &client{
		downloader: s3manager.NewDownloader(sess, func(d *s3manager.Downloader) { d.Concurrency = runtime.NumCPU() }),
	}, nil
}

// Download a specific key from the bucket
func (s *client) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	w := new(aws.WriteAtBuffer)
	n, err := s.downloader.DownloadWithContext(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return w.Bytes()[:n], nil
}
