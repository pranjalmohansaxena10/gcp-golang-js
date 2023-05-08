package main

import (
	"bytes"
	"context"
	"fmt"
	"pranjalmohansaxena10/gcp-golang-js/storage"
)

func main() {
	ctx := context.Background()
	client, err := storage.NewStorageClient(ctx, "gcs", "dev-poc")
	if err != nil {
		fmt.Printf("Couldn't create storage client: %+v\n", err)
	}

	folder := "firstDir/secondDir"
	key := "testData1.txt"
	err = client.Upload(ctx, &storage.UploadOptions{
		Folder:   folder,
		Key:      key,
		FileType: "",
	}, bytes.NewReader([]byte("test data to validate upload")))

	if err != nil {
		fmt.Printf("Couldn't upload file to cloud storage: %+v\n", err)
	}

	data, err := client.Download(ctx, &storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		fmt.Printf("Couldn't download file from cloud storage: %+v\n", err)
	}

	fmt.Printf("Data: %+v\n", string(data))
}
