package storage

import (
	"context"
	"io"
	"log"
	"strings"
)

type DownloadOptions struct {
	Folder string
	Key    string
}

type UploadOptions struct {
	Folder   string // Bucket or Container Name
	Key      string
	FileType string
}

type Storage interface {
	// Download downloads the file from the storage
	Download(ctx context.Context, options *DownloadOptions) ([]byte, error)
	// Upload the contents of the reader as an object into the bucket.
	Upload(ctx context.Context, options *UploadOptions, r io.Reader) error
	// // Exists checks if the given object exists.
	// Exists(ctx context.Context, opts *ListOptions) (bool, error)
	// // ListKeys list all the keys for given options
	// ListKeys(ctx context.Context, options *ListOptions) ([]string, error)
	// // GetTempTokenForDownload returns the signed token to download files.
	// GetTempTokenForDownload(options *DownloadOptions) (string, error)
	// // DownloadFromCdn download objects via CDN
	// DownloadFromCdn(ctx context.Context, options *DownloadOptions) (output []byte, err error)
	// // Delete deletes the object for given options
	// Delete(ctx context.Context, options *DeleteOptions) error
	// // IsNotFoundErr returns true if blob/object is not found
	// IsNotFoundErr(err error) bool
}

// NewStorageClient returns new storage client
func NewStorageClient(ctx context.Context, cloudProvider string, bucketName string) (Storage, error) {

	switch strings.ToLower(cloudProvider) {
	case "gcs":
		return newGCSClient(ctx, GCSBucketParams{
			Bucket:         bucketName,
			ServiceAccount: "",
			Logger:         *log.Default(),
		})
	default:
		return newGCSClient(ctx, GCSBucketParams{
			Bucket:         bucketName,
			ServiceAccount: "",
			Logger:         *log.Default(),
		})
	}
}
