package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:29092"
	topic         = "chats"
	groupID       = "chats-cg-2"
)

func ReceiveMessages2() {
	fmt.Println("Consumer is reading messages")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func main() {
	ReceiveMessages2()
}
