package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

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
const preSignURLExpiryDuration = 4 * time.Hour

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
	if options == nil {
		return keys, errors.New("missing list options")
	}

	prefix := options.Folder + DirDelim + options.Prefix
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, DirDelim) + DirDelim
	}

	g.logger.Printf("Iterating for prefix: %+v in GCS Bucket...", prefix)
	// If recursive iteration is enabled we should pass an empty delimiter.
	delimiter := DirDelim
	if options.Recursive {
		delimiter = ""
	}

	it := g.bucket.Objects(ctx, &storage.Query{
		Prefix:    prefix,
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

func (g gcsClient) GetTempTokenForDownload(options *DownloadOptions) (tempToken string, err error) {
	if options == nil {
		return tempToken, errors.New("missing download options")
	}
	key := options.Folder + DirDelim + options.Key
	if strings.HasPrefix(options.Key, options.Folder) {
		key = options.Key
	}

	g.logger.Printf("Getting temp token for file: %+v from GCS Bucket...", key)

	tempToken, err = g.bucket.SignedURL(key, &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(preSignURLExpiryDuration),
		Scheme:  storage.SigningSchemeV4,
	})
	if err != nil {
		return "", fmt.Errorf("error getting temp token for file: %+v from GCS since: %+v", key, err)
	}

	return tempToken, err
}

func (g gcsClient) DownloadFromCdn(ctx context.Context, options *DownloadOptions) (output []byte, err error) {
	sourcePath, err := g.GetTempTokenForDownload(options)
	if err != nil {
		return []byte{}, err
	}

	g.logger.Printf("Downloading data from Google Cloud Storage CDN...")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourcePath, http.NoBody)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	output, err = io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return output, fmt.Errorf("non-20x status code %d", res.StatusCode)
	}

	return
}

func (g gcsClient) IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrObjectNotExist)
}

func (g gcsClient) Delete(ctx context.Context, options *DeleteOptions) error {
	if options == nil {
		return errors.New("missing delete options")
	}
	key := options.Folder + DirDelim + options.Key
	g.logger.Printf("Deleting key: %+v from GCS Bucket...", key)

	return g.bucket.Object(key).Delete(ctx)
}