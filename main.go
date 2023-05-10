package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"pranjalmohansaxena10/gcp-golang-js/queue"
	"pranjalmohansaxena10/gcp-golang-js/secrets"
	"pranjalmohansaxena10/gcp-golang-js/storage"
	"time"
)

func main() {

	ctx := context.Background()
	folder := "firstDir"
	key := "testData4.txt"
	prefix := "secondDir"
	bucket := "dev-poc"
	GCSBucketInteractions(ctx, bucket, folder, key, prefix)

	projectID := "hyperexecute-dev"
	GSMInterations(ctx, projectID)

	subscriptionID := "validate-pubsub-interactions"
	topic := "dev-poc-pubsub"
	batchSize := 5
	PubsubInteractions(ctx, projectID, subscriptionID, topic, batchSize)
}

func GCSBucketInteractions(ctx context.Context, bucket, folder, key, prefix string) {
	logger := *log.Default()

	//----------Storage Client Creation--------------
	client, err := storage.NewStorageClient(ctx, "gcs", bucket, logger)
	if err != nil {
		logger.Printf("Couldn't create storage client: %+v", err)
		return
	}

	//----------Upload Functionality--------------
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

	//----------Exists Functionality--------------
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

	//----------Download Functionality--------------
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

	//----------ListKeys Functionality--------------
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

	//----------TempToken Download Functionality--------------
	tempToken, err := client.GetTempTokenForDownload(&storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't get tempToken from cloud storage: %+v", err)
		return
	}
	logger.Printf("TempToken: %+v", tempToken)

	//----------Download data from CDN Functionality--------------
	cdnData, err := client.DownloadFromCdn(ctx, &storage.DownloadOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't download data from CDN: %+v", err)
		return
	}
	logger.Printf("Data from CDN: %+v", string(cdnData))

	//----------Delete Functionality--------------
	err = client.Delete(ctx, &storage.DeleteOptions{
		Folder: folder,
		Key:    key,
	})
	if err != nil {
		logger.Printf("Couldn't delete data from GCS Bucket since: %+v", err)
		return
	}
	logger.Print("Deleted data successfully")
}

func GSMInterations(ctx context.Context, projectID string) {
	logger := *log.Default()

	//----------Secret Client Creation--------------
	client, err := secrets.NewSecretsClient(ctx, "gcs", projectID, logger)
	if err != nil {
		logger.Printf("Couldn't create secret client: %+v", err)
		return
	}

	payload := map[string]string{}
	payload["username"] = "pranjalmohansaxena10asas"
	payload["secretKey"] = "secondSecret"
	payload["secretValue"] = "dhfkjadhflkjq"

	// ----------SetSecret--------------
	err = client.SetSecret(payload)
	if err != nil {
		logger.Printf("couldn't set secret data since: %+v", err)
		return
	}

	//----------SetSecrets--------------
	err = client.SetSecrets(&secrets.SecretsPayloadReq{
		OrgID: "",
		User:  "pranjalmohansaxena10",
		Secrets: []secrets.Secret{{
			Key:   "firstSecret",
			Value: "3128301",
		}, {
			Key:   "secondSecret",
			Value: "dhfkjadhflkjq",
		}},
	})
	if err != nil {
		logger.Printf("couldn't set secret data since: %+v", err)
		return
	}

	//----------Secret Get-----------------
	secretData, err := client.GetSecret(payload)
	if err != nil {
		logger.Printf("couldn't get secret data since: %+v", err)
		return
	}

	logger.Printf("Secret Data: %+v", secretData)

	//----------Secret Delete-----------------
	err = client.DeleteSecret(payload)
	if err != nil {
		logger.Printf("couldn't delete secret data since: %+v", err)
		return
	}
}

func PubsubInteractions(ctx context.Context, projectID, subscriptionID, topic string, batchSize int) {
	logger := *log.Default()
	//----------Queue Client Creation--------------
	client, err := queue.NewQueueInstance("GCP", projectID, subscriptionID, topic, logger, ctx)
	if err != nil {
		logger.Printf("couldn't create queue instance: %+v", err)
		return
	}

	//----------Sending data--------------
	for i := 1; i <= batchSize; i++ {
		err = client.Send([]byte(fmt.Sprintf("%+v ", i) + time.Now().String()))
		if err != nil {
			logger.Printf("couldn't send message to queue: %+v", err)
			return
		}
	}

	//----------Receiving data--------------
	receivedMessage, err := client.Receive()
	if err != nil {
		logger.Printf("couldn't receive message from queue: %+v", err)
		return
	}
	logger.Printf("Received message as: %+v", string(receivedMessage.Msg))

	//----------Receiving data in batches--------------
	receivedMessages, err := client.ReceiveMessagesInBatch(batchSize)
	if err != nil {
		logger.Printf("couldn't receive message from queue: %+v", err)
		return
	}
	logger.Printf("Got data of batch as: %+v", len(receivedMessages))
	for idx := 0; idx < len(receivedMessages); idx++ {
		logger.Printf("Received message as: %+v", string(receivedMessages[idx].Msg))
	}
}
