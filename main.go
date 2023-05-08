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
	}

	folder := "firstDir/secondDir"
	key := "testData1.txt"

	err = client.Upload(ctx, &storage.UploadOptions{
		Folder:   folder,
		Key:      key,
		FileType: "",
	}, bytes.NewReader([]byte("test data to validate upload")))
	if err != nil {
		logger.Printf("Couldn't upload file to cloud storage: %+v", err)
	}
	logger.Printf("Uploading data is successful")

	exists, err := client.Exists(ctx, &storage.ListOptions{
		Folder:    folder,
		Key:       key,
		Prefix:    "",
		Recursive: false,
	})

	if err != nil {
		logger.Printf("Couldn't check whether given file exists in cloud storage: %+v", err)	
	}
	if exists {
		logger.Printf("Given file: %+v exists in cloud storage: %+v", folder + key, exists)	
	}

	data, err := client.Download(ctx, &storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't download file from cloud storage: %+v", err)
	}
	logger.Printf("Downloading data is successful")
	logger.Printf("Data: %+v", string(data))
}
