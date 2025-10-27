package control

import "time"

// Published represents a notification from a generator about messages it has published
type Published struct {
	// GeneratorID is the unique identifier of the generator
	GeneratorID string `json:"generator_id"`

	// Timestamp is when the messages were published
	Timestamp time.Time `json:"timestamp"`

	// StartID is the first message ID in the range
	StartID int64 `json:"start_id"`

	// EndID is the last message ID in the range (inclusive)
	EndID int64 `json:"end_id"`
}

// MessageRange tracks a range of message IDs from a generator
type MessageRange struct {
	GeneratorID string
	StartID     int64
	EndID       int64
	Timestamp   time.Time
}
