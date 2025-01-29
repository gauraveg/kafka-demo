package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:29092"
	topic         = "chats"
)

type SendMsg struct {
	Message string `json:"message"`
}

func ProduceMessage(msgs []string) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.RoundRobin{},
	}

	fmt.Println("Producer is writing messages")

	for _, msg := range msgs {
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(msg),
			},
		)

		log.Printf("Produced: %s", msg)

		if err != nil {
			log.Printf("failed to write messages: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func main() {
	route := chi.NewRouter()
	route.Post("/send", func(w http.ResponseWriter, r *http.Request) {
		var sendMsg SendMsg
		err := json.NewDecoder(r.Body).Decode(&sendMsg)
		if err != nil {
			log.Println(err)
			return
		}

		msgs := strings.Split(sendMsg.Message, ",")

		ProduceMessage(msgs)

		w.Write([]byte("sent"))
		w.WriteHeader(http.StatusOK)
	})

	log.Fatal(http.ListenAndServe(":8080", route))
}
