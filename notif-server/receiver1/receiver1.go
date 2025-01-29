package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:29092"
	topic         = "notifs"
	groupID       = "notifs-grp-2"
)

func ReceiveMessages() {
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
		fmt.Printf("message at partition %v: %s = %s\n",
			msg.Partition, string(msg.Key), string(msg.Value))
	}

	if err := reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func main() {
	ReceiveMessages()
}
