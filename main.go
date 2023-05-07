package main

import (
	"context"
	"fmt"
	"pranjalmohansaxena10/gcp-golang-js/storage"
)

func main() {
	ctx := context.Background()
	client, err := storage.NewStorageClient(ctx, "gcs", "dev-poc")
	if err != nil {
		fmt.Printf("Couldn't crate storage client: %+v\n", err)
	}
	data, err := client.Download(ctx, &storage.DownloadOptions{
		Folder: "",
		Key:    "testFile.txt",
	})
	if err != nil {
		fmt.Printf("Couldn't download file from cloud storage: %+v\n", err)
	}

	fmt.Printf("Data: %+v\n", string(data))
}
