package queue

import (
	"context"
	"log"
)

type ReceivedMessage struct {
	Msg           []byte
	ReceiptHandle string
}

const (
	GCP string = "GCP"
)

type Queue interface {
	Receive() (receivedMessage ReceivedMessage, err error)
	Send(message []byte) error
	// Delete(message ReceivedMessage) error
	// Close() error
	ReceiveMessagesInBatch(numberOfMessages int) (receivedMessages []ReceivedMessage, err error)
	// DeleteMessagesInBatch(messages []ReceivedMessage) error
	// SendWithPartition(message []byte, partitionKey string) error
	// GetQueueMessageCount() (int, error)
}

type QueueInstance struct {
	logger   log.Logger
	ctx      context.Context
	queueObj interface{}
}

func NewQueueInstance(manager, projectID, subscriptionID, topic string, logger log.Logger, ctx context.Context) (Queue, error) {
	return NewPubSubQueue(ctx, logger, projectID, subscriptionID, topic)
}
