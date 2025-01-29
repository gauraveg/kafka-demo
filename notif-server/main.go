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
	topic         = "notifs"
)

type SendNote struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func ProduceMessage(sendMsg SendNote, msgs []string) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}

	fmt.Println("Producer is writing messages")

	var err error
	for _, msg := range msgs {
		if sendMsg.Type == "dev" {
			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte("dev"),
					Value: []byte(msg),
				},
			)
		} else if sendMsg.Type == "user" {
			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte("user"),
					Value: []byte(msg),
				},
			)
		} else if sendMsg.Type == "manager" {
			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte("manager"),
					Value: []byte(msg),
				},
			)
		}

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
		var sendMsg SendNote
		err := json.NewDecoder(r.Body).Decode(&sendMsg)
		if err != nil {
			log.Println(err)
			return
		}

		msgs := strings.Split(sendMsg.Message, ",")

		ProduceMessage(sendMsg, msgs)

		w.Write([]byte("sent"))
		w.WriteHeader(http.StatusOK)
	})

	log.Fatal(http.ListenAndServe(":8090", route))
}
