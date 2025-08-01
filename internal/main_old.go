package main

import (
	"cognyx/psychic-robot/persistence/db"
	"cognyx/psychic-robot/persistence/repository"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()
	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}
	// Add or update stream with wildcard subject

	// Initialize database connection
	dbURL := "postgres://postgres:postgres@localhost:5432/poc?sslmode=disable"
	conn, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer conn.Close(context.Background())

	// Initialize repositories
	queries := db.New(conn)
	_ = repository.NewUserRepository(queries)

	// Subscribe to all users.push.* messages
	_, err = js.Subscribe("users.push.*", func(msg *nats.Msg) {
		fmt.Printf("Received [%s]: %s\n", msg.Subject, string(msg.Data))
		msg.Ack()

		var user UserVersionDTO

		json.Unmarshal(msg.Data, &user)

		js.Publish("users.pull."+strconv.FormatInt(user.UserID, 10), msg.Data)

	}, nats.Durable("my-consumer"), nats.ManualAck())
	if err != nil {
		log.Fatal(err)
	}

	// Wait to receive
	select {}
}

type UserVersionDTO struct {
	ID        int64     `json:"id"`         // Version ID
	UserID    int64     `json:"user_id"`    // Reference to user
	Version   int32     `json:"version"`    // Version number
	UserData  db.User   `json:"user_data"`  // Complete user data snapshot
	Action    string    `json:"action"`     // create, update
	CreatedAt time.Time `json:"created_at"` // When this version was created
	UpdatedAt time.Time `json:"updated_at"` // When this version was created
	Deleted   bool      `json:"_deleted"`   // Whether this version is deleted
}
