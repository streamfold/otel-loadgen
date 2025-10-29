package control

import "time"

// Published represents a notification from a generator about messages it has published
type Published struct {
	// GeneratorID is the unique identifier of the generator
	GeneratorID string `json:"generator_id"`

	// Timestamp is when the messages were published
	Timestamp time.Time `json:"timestamp"`

	// StartID is the first message ID in the range
	StartID uint64 `json:"start_id"`

	// RangeLen is the length of the ID range
	RangeLen uint `json:"range_len"`
}

// MessageRange tracks a range of message IDs from a generator
type MessageRange struct {
	GeneratorID string
	StartID     uint64
	RangeLen uint
	Timestamp   time.Time
}
