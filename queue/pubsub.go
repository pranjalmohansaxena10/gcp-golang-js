package queue

import (
	"context"
	"errors"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

const ReadTimeout = 500 * time.Millisecond
const MaxErrorCount = 3
const GRACEFUL_SHUTDOWN_INITIATED = false

type PubSubQueue struct {
	QueueInstance
}

type queueObj struct {
	topic          pubsub.Topic
	subClient      pubsub.Subscription
	projectID      string
	subscriptionID string
}

func (sq PubSubQueue) Close() error {
	return nil
}

func (sq PubSubQueue) GetQueueInstance() queueObj {
	return sq.queueObj.(queueObj)
}

func NewPubSubQueue(ctx context.Context, logger log.Logger, projectID, subscriptionID, topicName string) (*PubSubQueue, error) {
	pubsubQueue := PubSubQueue{}
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client since: %+v", err)
		return nil, err
	}

	topic := client.Topic(topicName)
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatalf("failed to check pubsub topic exists since: %+v", err)
		return nil, err
	}
	if !topicExists {
		return nil, errors.New("given pubsub topic doesn't exists")
	}

	var subscription *pubsub.Subscription
	subscription = client.Subscription(subscriptionID)
	subscriptionExists, err := subscription.Exists(ctx)
	if err != nil {
		log.Fatalf("failed to check pubsub subscription exists since: %+v", err)
		return nil, err
	}
	if !subscriptionExists {
		log.Printf("given subscription id %+v doesn't exists. Creating new one...", subscriptionID)
		subscription, err = client.CreateSubscription(context.Background(), subscriptionID,
			pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			log.Fatalf("failed to create pubsub subscription since: %+v", err)
			return nil, err
		}
	}
	log.Printf("given subscription id %+v exists. Using same subscription", subscriptionID)

	pubsubQueue.queueObj = queueObj{
		topic:          *topic,
		subClient:      *subscription,
		projectID:      projectID,
		subscriptionID: subscriptionID,
	}
	pubsubQueue.logger = logger
	pubsubQueue.ctx = ctx
	return &pubsubQueue, nil
}

func (p PubSubQueue) Receive() (receivedMessage ReceivedMessage, err error) {
	queue := p.GetQueueInstance()
	errorCount := 0
	for {
		p.logger.Printf("receiving 1 message...")
		if GRACEFUL_SHUTDOWN_INITIATED {
			return ReceivedMessage{}, errors.New("graceful shutdown initiated")
		}

		msgResult := pubsub.Message{}
		childContext, cancel := context.WithCancel(p.ctx)
		defer cancel()
		err := queue.subClient.Receive(childContext, func(ctx context.Context, m *pubsub.Message) {
			p.logger.Printf("1 message received")
			msgResult = *m
			m.Ack()
			cancel()
		})

		if err != nil {
			errorCount += 1
			if errorCount < MaxErrorCount {
				time.Sleep(ReadTimeout)
				continue
			}
			return ReceivedMessage{}, err
		}

		if len(msgResult.Data) == 0 {
			time.Sleep(ReadTimeout)
			continue
		}

		receivedMessage.Msg = msgResult.Data
		break
	}
	return receivedMessage, nil
}

func (p PubSubQueue) Send(message []byte) error {
	if message == nil {
		return errors.New("invalid message given to publish")
	}
	queue := p.GetQueueInstance()
	result := queue.topic.Publish(p.ctx, &pubsub.Message{
		Data: message,
	})
	messageID, err := result.Get(p.ctx)
	if err != nil {
		p.logger.Printf("Error in publishing message: %+v", err)
		return err
	}
	p.logger.Printf("Given message successfully published to topic %+v with messageID as %+v", queue.topic.String(), messageID)
	return nil
}
func (p PubSubQueue) ReceiveMessagesInBatch(numberOfMessages int) (receivedMessages []ReceivedMessage, err error) {
	queue := p.GetQueueInstance()
	errorCount := 0
	messagesChan := make(chan []byte, numberOfMessages)
	childContext, cancel := context.WithCancel(p.ctx)
	defer cancel()
	go func() {
		for message := range messagesChan {
			receivedMessages = append(receivedMessages, ReceivedMessage{
				Msg: message,
			})
			p.logger.Printf("len: %+v and numberOfMessages: %+v", len(receivedMessages), numberOfMessages)
			if len(receivedMessages) >= numberOfMessages {
				cancel()
				return
			}
		}
	}()
	for {
		p.logger.Printf("receiving messages with batch size: %+v...", numberOfMessages)
		if GRACEFUL_SHUTDOWN_INITIATED {
			return nil, errors.New("graceful shutdown initiated")
		}
		queue.subClient.ReceiveSettings.MaxOutstandingMessages = numberOfMessages
		queue.subClient.ReceiveSettings.Synchronous = true

		p.logger.Printf("settings : %+v", queue.subClient.ReceiveSettings)
		err := queue.subClient.Receive(childContext, func(ctx context.Context, m *pubsub.Message) {
			messagesChan <- m.Data
			m.Ack()
		})

		if err != nil {
			errorCount += 1
			if errorCount < MaxErrorCount {
				time.Sleep(ReadTimeout)
				continue
			}
			return nil, err
		}
		break
	}

	return receivedMessages, nil
}
