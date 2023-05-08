package main

import (
	"bytes"
	"context"
	"log"
	"pranjalmohansaxena10/gcp-golang-js/storage"
)

func main() {
	ctx := context.Background()
	logger := *log.Default()
	client, err := storage.NewStorageClient(ctx, "gcs", "dev-poc", logger)
	if err != nil {
		logger.Printf("Couldn't create storage client: %+v", err)
		return
	}

	folder := "firstDir"
	key := "testData4.txt"
	prefix := "secondDir"

	err = client.Upload(ctx, &storage.UploadOptions{
		Folder:   folder,
		Key:      key,
		FileType: "",
	}, bytes.NewReader([]byte("test data to validate upload")))
	if err != nil {
		logger.Printf("Couldn't upload file to cloud storage: %+v", err)
		return
	}
	logger.Printf("Uploading data is successful")

	exists, err := client.Exists(ctx, &storage.ListOptions{
		Folder: folder,
		Key:    key,
		Prefix: "",
	})

	if err != nil {
		logger.Printf("Couldn't check whether given file exists in cloud storage: %+v", err)
		return
	}
	logger.Printf("Given file: %+v exists in cloud storage: %+v", folder+key, exists)

	data, err := client.Download(ctx, &storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't download file from cloud storage: %+v", err)
		return
	}
	logger.Printf("Downloading data is successful")
	logger.Printf("Data: %+v", string(data))

	keys, err := client.ListKeys(ctx, &storage.ListOptions{
		Folder:    folder,
		Prefix:    prefix,
		Recursive: true,
	})
	if err != nil {
		logger.Printf("Couldn't get keys for cloud storage: %+v", err)
		return
	}
	logger.Printf("Keys: %+v", keys)

	tempToken, err := client.GetTempTokenForDownload(&storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't get tempToken from cloud storage: %+v", err)
		return
	}
	logger.Printf("TempToken: %+v", tempToken)

	cdnData, err := client.DownloadFromCdn(ctx, &storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't download data from CDN: %+v", err)
		return
	}
	logger.Printf("Data from CDN: %+v", string(cdnData))

	err = client.Delete(ctx, &storage.DeleteOptions{
		Folder:   folder + "/secondDir",
		Key:      key,
	})
	if err != nil {
		logger.Printf("Couldn't delete data from GCS Bucket since: %+v", err)
		return
	}
	logger.Print("Deleted data successfully")
}
