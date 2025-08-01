package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal("Failed to connect to NATS:", err)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal("Failed to create JetStream context:", err)
	}

	// Create USERS_UPDATE stream
	updateStreamConfig := &nats.StreamConfig{
		Name:     "USERS_UPDATE",
		Subjects: []string{"users.push.*"},
		Storage:  nats.FileStorage,
	}

	updateStream, err := js.AddStream(updateStreamConfig)
	if err != nil {
		log.Printf("Failed to create USERS_UPDATE stream (may already exist): %v", err)
	} else {
		fmt.Printf("Created USERS_UPDATE stream: %+v\n", updateStream)
	}

	// Create USERS_BROADCAST stream
	broadcastStreamConfig := &nats.StreamConfig{
		Name:     "USERS_BROADCAST",
		Subjects: []string{"users.pull.*"},
		Storage:  nats.FileStorage,
	}

	broadcastStream, err := js.AddStream(broadcastStreamConfig)
	if err != nil {
		log.Printf("Failed to create USERS_BROADCAST stream (may already exist): %v", err)
	} else {
		fmt.Printf("Created USERS_BROADCAST stream: %+v\n", broadcastStream)
	}

	fmt.Println("✅ NATS JetStream streams setup complete!")
}
