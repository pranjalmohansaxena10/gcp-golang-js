package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
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

const DirDelim = "/"

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
	key := options.Folder + DirDelim + options.Key
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
	key := options.Folder + DirDelim + options.Key
	g.logger.Printf("Uploading file: %+v to GCS Bucket...", key)
	gcsWriter := g.bucket.Object(key).NewWriter(ctx)

	if _, err := io.Copy(gcsWriter, r); err != nil {
		return err
	}
	return gcsWriter.Close()
}

// Exists check whether given object is present in GCS bucket or not
func (g gcsClient) Exists(ctx context.Context, options *ListOptions) (bool, error) {
	if options == nil {
		return false, errors.New("missing list options")
	}
	key := options.Folder + DirDelim + options.Key
	g.logger.Printf("Checking whether file: %+v exists in GCS Bucket...", key)
	if _, err := g.bucket.Object(key).Attrs(ctx); err == nil {
		return true, nil
	} else if err != storage.ErrObjectNotExist {
		return false, err
	}
	return false, nil
}

func (g gcsClient) ListKeys(ctx context.Context, options *ListOptions) (keys []string, err error) {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	dir := options.Folder + DirDelim + options.Prefix
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	g.logger.Printf("Iterating for prefix: %+v in GCS Bucket...", dir)
	// If recursive iteration is enabled we should pass an empty delimiter.
	delimiter := DirDelim
	if options.Recursive {
		delimiter = ""
	}

	it := g.bucket.Objects(ctx, &storage.Query{
		Prefix:    dir,
		Delimiter: delimiter,
	})
	for {
		select {
		case <-ctx.Done():
			return keys, ctx.Err()
		default:
		}
		attrs, err := it.Next()
		if err == iterator.Done {
			return keys, nil
		}
		if err != nil {
			return keys, err
		}
		keys = append(keys, attrs.Prefix+attrs.Name)
	}
}
