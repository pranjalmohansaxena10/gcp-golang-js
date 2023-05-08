package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"cloud.google.com/go/storage"
)

type gcsClient struct {
	logger log.Logger
	bucket *storage.BucketHandle
}

type GCSBucketParams struct {
	Bucket         string
	ServiceAccount string
	Logger         log.Logger
}

func newGCSClient(ctx context.Context, params GCSBucketParams) (Storage, error) {
	if params.Bucket == "" {
		return nil, errors.New("missing google cloud storage bucket name")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't create GCS storage client since: %+v", err)
	}
	return gcsClient{
		logger: params.Logger,
		bucket: client.Bucket(params.Bucket),
	}, nil
}

// Download gets the content of given object in GCS and returns []byte
func (g gcsClient) Download(ctx context.Context, options *DownloadOptions) ([]byte, error) {
	if options == nil {
		return nil, errors.New("missing download options")
	}
	key := options.Folder + "/" + options.Key
	if strings.HasPrefix(options.Key, options.Folder) {
		key = options.Key
	}

	g.logger.Printf("Downloading file: %+v from GCS Bucket...", key)

	reader, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("error downloading file: %+v from GCS since: %+v", key, err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// Uploads the given data to GCS Bucket
func (g gcsClient) Upload(ctx context.Context, options *UploadOptions, r io.Reader) error {
	if options == nil {
		return errors.New("missing upload options")
	}
	key := options.Folder + "/" + options.Key
	g.logger.Printf("Uploading file: %+v to GCS Bucket...", key)
	gcsWriter := g.bucket.Object(key).NewWriter(ctx)

	if _, err := io.Copy(gcsWriter, r); err != nil {
		return err
	}
	return gcsWriter.Close()
}
