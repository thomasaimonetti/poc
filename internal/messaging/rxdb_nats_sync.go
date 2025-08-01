package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"cognyx/psychic-robot/persistence/db"
	"cognyx/psychic-robot/persistence/repository"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	natsgo "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// RxDBDocument represents a document structure used by RxDB NATS replication
type RxDBDocument struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Status    string    `json:"status"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Deleted   bool      `json:"_deleted"`
}

// RxDBNATSSync handles synchronization between RxDB NATS replication and PostgreSQL
type RxDBNATSSync struct {
	userRepo    repository.UserRepository
	versionRepo repository.VersionRepository
	subscriber  *nats.Subscriber
	publisher   *nats.Publisher
	logger      watermill.LoggerAdapter
}

// NewRxDBNATSSync creates a new RxDB NATS synchronization service
func NewRxDBNATSSync(
	natsURL string,
	userRepo repository.UserRepository,
	versionRepo repository.VersionRepository,
	logger watermill.LoggerAdapter,
) (*RxDBNATSSync, error) {
	// Create NATS connection
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	log.Printf("Connected to NATS")

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Ensure the stream exists for RxDB replication
	streamName := "users-stream"
	_, err = js.StreamInfo(streamName)
	if err != nil {
		// Create the stream if it doesn't exist
		_, err = js.AddStream(&natsgo.StreamConfig{
			Name:     streamName,
			Subjects: []string{"users.push.>"},
			Storage:  natsgo.FileStorage,
		})
		if err != nil {
			fmt.Println("failed to create JetStream stream: %w", err)
			return nil, fmt.Errorf("failed to create JetStream stream: %w", err)
		}
		fmt.Println("Created JetStream stream for RxDB replication: %s", streamName)
	}

	// Create Watermill publisher and subscriber
	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL: natsURL,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS publisher: %w", err)
	}

	subscriber, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL: natsURL,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS subscriber: %w", err)
	}

	return &RxDBNATSSync{
		userRepo:    userRepo,
		versionRepo: versionRepo,
		subscriber:  subscriber,
		publisher:   publisher,
		logger:      logger,
	}, nil
}

// Start begins listening for RxDB NATS replication messages
func (s *RxDBNATSSync) Start(ctx context.Context) error {
	s.logger.Info("Starting RxDB NATS synchronization", watermill.LogFields{})

	// Subscribe to RxDB replication subjects
	// RxDB uses subject pattern: {subjectPrefix}.{documentId}
	messages, err := s.subscriber.Subscribe(ctx, "users.>")
	if err != nil {
		return fmt.Errorf("failed to subscribe to users subjects: %w", err)
	}

	// Process messages in a goroutine
	go func() {
		for {
			select {
			case msg := <-messages:
				if err := s.handleRxDBMessage(ctx, msg); err != nil {
					s.logger.Error("Failed to handle RxDB message", err, watermill.LogFields{
						"subject": msg.Metadata.Get("subject"),
					})
				}
				msg.Ack()
			case <-ctx.Done():
				s.logger.Info("RxDB NATS sync context cancelled", watermill.LogFields{})
				return
			}
		}
	}()

	return nil
}

// handleRxDBMessage processes incoming RxDB replication messages
func (s *RxDBNATSSync) handleRxDBMessage(ctx context.Context, msg *message.Message) error {
	subject := msg.Metadata.Get("subject")
	s.logger.Info("Received RxDB message", watermill.LogFields{
		"subject": subject,
		"payload": string(msg.Payload),
	})

	// Parse the RxDB document
	var rxdbDoc RxDBDocument
	if err := json.Unmarshal(msg.Payload, &rxdbDoc); err != nil {
		return fmt.Errorf("failed to unmarshal RxDB document: %w", err)
	}

	// Convert RxDB document ID to database ID if it's numeric
	// Otherwise, we'll need to track the mapping or use string IDs
	var dbUserID int64
	if numericID, err := strconv.ParseInt(rxdbDoc.ID, 10, 64); err == nil {
		dbUserID = numericID
	} else {
		// For UUID-based IDs, we need to find the user by email or maintain a mapping
		// For this implementation, we'll create new users or find by email
		existingUser, err := s.findUserByEmail(ctx, rxdbDoc.Email)
		if err == nil && existingUser != nil {
			dbUserID = existingUser.ID
		}
	}

	if rxdbDoc.Deleted {
		// Handle deletion
		return s.handleUserDeletion(ctx, dbUserID, rxdbDoc)
	} else if dbUserID > 0 {
		// Handle update
		return s.handleUserUpdate(ctx, dbUserID, rxdbDoc)
	} else {
		// Handle creation
		return s.handleUserCreation(ctx, rxdbDoc)
	}
}

// findUserByEmail searches for a user by email
func (s *RxDBNATSSync) findUserByEmail(ctx context.Context, email string) (*db.User, error) {
	// This would require adding a method to the user repository
	// For now, we'll skip this and let creation happen
	return nil, fmt.Errorf("user not found")
}

// handleUserCreation creates a new user in PostgreSQL from RxDB data
func (s *RxDBNATSSync) handleUserCreation(ctx context.Context, rxdbDoc RxDBDocument) error {
	s.logger.Info("Creating user from RxDB sync", watermill.LogFields{
		"email":   rxdbDoc.Email,
		"rxdb_id": rxdbDoc.ID,
	})

	// Create user in PostgreSQL
	createParams := db.CreateUserParams{
		Email:  rxdbDoc.Email,
		Status: rxdbDoc.Status,
	}
	if rxdbDoc.Role != "" {
		createParams.Role.String = rxdbDoc.Role
		createParams.Role.Valid = true
	}

	user, err := s.userRepo.Create(ctx, createParams)
	if err != nil {
		return fmt.Errorf("failed to create user in database: %w", err)
	}

	// Create version record
	userDataJSON, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user data: %w", err)
	}

	versionParams := db.CreateVersionParams{
		ObjectType: "user",
		ObjectID:   user.ID,
		Json:       userDataJSON,
		Version:    1,
		Action:     "create",
		Actor:      "rxdb-sync",
	}

	_, err = s.versionRepo.Create(ctx, versionParams)
	if err != nil {
		s.logger.Error("Failed to create version record", err, watermill.LogFields{
			"user_id": user.ID,
		})
		// Don't fail the user creation if version creation fails
	}

	s.logger.Info("User created successfully from RxDB sync", watermill.LogFields{
		"user_id": user.ID,
		"email":   user.Email,
		"rxdb_id": rxdbDoc.ID,
	})

	// Optionally publish back to RxDB subjects with the database ID
	// This would help maintain ID mapping between RxDB and PostgreSQL
	return s.publishUserToRxDB(ctx, user, "create")
}

// handleUserUpdate updates an existing user in PostgreSQL from RxDB data
func (s *RxDBNATSSync) handleUserUpdate(ctx context.Context, userID int64, rxdbDoc RxDBDocument) error {
	s.logger.Info("Updating user from RxDB sync", watermill.LogFields{
		"user_id": userID,
		"email":   rxdbDoc.Email,
	})

	// Get existing user (though we don't use it in this implementation)
	_, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get existing user: %w", err)
	}

	// Update user in PostgreSQL
	updateParams := db.UpdateUserParams{
		ID:     userID,
		Email:  rxdbDoc.Email,
		Status: rxdbDoc.Status,
	}
	if rxdbDoc.Role != "" {
		updateParams.Role.String = rxdbDoc.Role
		updateParams.Role.Valid = true
	}

	updatedUser, err := s.userRepo.Update(ctx, updateParams)
	if err != nil {
		return fmt.Errorf("failed to update user in database: %w", err)
	}

	// Get next version number
	existingVersions, err := s.versionRepo.ListByObject(ctx, "user", userID)
	if err != nil {
		return fmt.Errorf("failed to get existing versions: %w", err)
	}
	nextVersion := int32(len(existingVersions) + 1)

	// Create new version record
	userDataJSON, err := json.Marshal(updatedUser)
	if err != nil {
		return fmt.Errorf("failed to marshal user data: %w", err)
	}

	versionParams := db.CreateVersionParams{
		ObjectType: "user",
		ObjectID:   userID,
		Json:       userDataJSON,
		Version:    nextVersion,
		Action:     "update",
		Actor:      "rxdb-sync",
	}

	_, err = s.versionRepo.Create(ctx, versionParams)
	if err != nil {
		s.logger.Error("Failed to create version record", err, watermill.LogFields{
			"user_id": userID,
		})
	}

	s.logger.Info("User updated successfully from RxDB sync", watermill.LogFields{
		"user_id": userID,
		"email":   updatedUser.Email,
		"version": nextVersion,
	})

	return s.publishUserToRxDB(ctx, updatedUser, "update")
}

// handleUserDeletion handles soft deletion of a user from RxDB data
func (s *RxDBNATSSync) handleUserDeletion(ctx context.Context, userID int64, rxdbDoc RxDBDocument) error {
	if userID == 0 {
		// Can't delete a user we don't have in the database
		s.logger.Info("Skipping deletion of unknown user", watermill.LogFields{
			"rxdb_id": rxdbDoc.ID,
		})
		return nil
	}

	s.logger.Info("Deleting user from RxDB sync", watermill.LogFields{
		"user_id": userID,
		"email":   rxdbDoc.Email,
	})

	// For this implementation, we'll just log the deletion
	// In a real system, you might want to implement soft deletion in PostgreSQL
	// or handle this differently based on your business requirements

	// Get next version number
	existingVersions, err := s.versionRepo.ListByObject(ctx, "user", userID)
	if err != nil {
		return fmt.Errorf("failed to get existing versions: %w", err)
	}
	nextVersion := int32(len(existingVersions) + 1)

	// Create deletion version record
	deletionData := map[string]interface{}{
		"id":         userID,
		"email":      rxdbDoc.Email,
		"_deleted":   true,
		"deleted_at": time.Now().Format(time.RFC3339),
	}

	userDataJSON, err := json.Marshal(deletionData)
	if err != nil {
		return fmt.Errorf("failed to marshal deletion data: %w", err)
	}

	versionParams := db.CreateVersionParams{
		ObjectType: "user",
		ObjectID:   userID,
		Json:       userDataJSON,
		Version:    nextVersion,
		Action:     "delete",
		Actor:      "rxdb-sync",
	}

	_, err = s.versionRepo.Create(ctx, versionParams)
	if err != nil {
		s.logger.Error("Failed to create deletion version record", err, watermill.LogFields{
			"user_id": userID,
		})
	}

	s.logger.Info("User deletion processed from RxDB sync", watermill.LogFields{
		"user_id": userID,
		"version": nextVersion,
	})

	return nil
}

// publishUserToRxDB publishes user data back to RxDB subjects
func (s *RxDBNATSSync) publishUserToRxDB(ctx context.Context, user db.User, action string) error {
	// Convert database user to RxDB format
	rxdbDoc := RxDBDocument{
		ID:        fmt.Sprintf("%d", user.ID), // Convert database ID to string for RxDB
		Email:     user.Email,
		Status:    user.Status,
		Role:      user.Role.String,
		CreatedAt: user.CreatedAt,
		UpdatedAt: time.Now(),
		Deleted:   false,
	}

	docJSON, err := json.Marshal(rxdbDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal RxDB document: %w", err)
	}

	// Publish to RxDB subject pattern
	subject := fmt.Sprintf("users.%s", user.ID)
	msg := message.NewMessage(fmt.Sprintf("%d", user.ID), docJSON)
	msg.Metadata.Set("subject", subject)

	err = s.publisher.Publish(subject, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to RxDB subject: %w", err)
	}

	s.logger.Info("Published user data to RxDB", watermill.LogFields{
		"subject": subject,
		"action":  action,
		"user_id": user.ID,
	})

	return nil
}

// Stop shuts down the RxDB NATS synchronization
func (s *RxDBNATSSync) Stop() error {
	s.logger.Info("Stopping RxDB NATS synchronization", watermill.LogFields{})

	if s.subscriber != nil {
		if err := s.subscriber.Close(); err != nil {
			s.logger.Error("Failed to close subscriber", err, watermill.LogFields{})
		}
	}

	if s.publisher != nil {
		if err := s.publisher.Close(); err != nil {
			s.logger.Error("Failed to close publisher", err, watermill.LogFields{})
		}
	}

	return nil
}
