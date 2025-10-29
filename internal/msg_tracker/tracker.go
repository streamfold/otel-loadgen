package msg_tracker

import (
	"sync"
	"time"
)

// MessageRange represents a range of message IDs with a bitmask for tracking acknowledgments
type MessageRange struct {
	StartID        uint64
	RangeLen       uint
	Timestamp      time.Time
	AckedCount     uint    // Number of unique messages acked
	DuplicateCount uint    // Number of duplicate acks received
	bitmap         []uint64 // Each uint64 holds 64 bits
}

// NewMessageRange creates a new message range
func NewMessageRange(startID uint64, rangeLen uint) *MessageRange {
	if rangeLen == 0 {
		panic("range length must be > 0")
	}

	bitmapSize := (rangeLen + 63) / 64 // Round up to nearest 64

	return &MessageRange{
		StartID:  startID,
		RangeLen: rangeLen,
		bitmap:   make([]uint64, bitmapSize),
	}
}

// Ack marks a message ID as acknowledged
// Returns true if the message was in range, false otherwise
func (mr *MessageRange) Ack(msgID uint64) bool {
	if !mr.Contains(msgID) {
		return false
	}
	
	offset := msgID - mr.StartID
	idx := offset / 64
	bit := offset % 64

	// Check if already acked
	wasAlreadyAcked := (mr.bitmap[idx] & (1 << bit)) != 0

	// Set the bit
	mr.bitmap[idx] |= (1 << bit)

	// Update counters
	if wasAlreadyAcked {
		mr.DuplicateCount++
	} else {
		mr.AckedCount++
	}

	return true
}

// IsAcked checks if a message ID has been acknowledged
func (mr *MessageRange) IsAcked(msgID uint64) bool {
	if !mr.Contains(msgID) {
		return false
	}

	offset := msgID - mr.StartID
	idx := offset / 64
	bit := offset % 64

	return (mr.bitmap[idx] & (1 << bit)) != 0
}

// Contains checks if the range contains the given message ID
func (mr *MessageRange) Contains(msgID uint64) bool {
	return msgID >= mr.StartID && msgID < mr.StartID + uint64(mr.RangeLen)
}

// TotalMessages returns the total number of messages in the range
func (mr *MessageRange) TotalMessages() uint {
	return mr.RangeLen
}

// UnackedCount returns the number of messages that have not been acknowledged
func (mr *MessageRange) UnackedCount() uint {
	return mr.TotalMessages() - mr.AckedCount
}

// generatorTracker holds all ranges for a specific generator ID
type generatorTracker struct {
	mu     sync.RWMutex
	ranges map[uint64]*MessageRange // Key is startID, we assume ranges are unique
}

func newGeneratorTracker() *generatorTracker {
	return &generatorTracker{
		ranges: make(map[uint64]*MessageRange),
	}
}

// findRange finds the range containing the given message ID
func (gt *generatorTracker) findRange(startRangeID uint64, rangeLen uint) *MessageRange {
	return gt.ranges[startRangeID]
}

// addRange adds or returns existing range
func (gt *generatorTracker) addRange(startID uint64, rangeLen uint) *MessageRange {
	if r, exists := gt.ranges[startID]; exists {
		return r
	}

	r := NewMessageRange(startID, rangeLen)
	gt.ranges[startID] = r
	return r
}

// addRangeWithTimestamp adds a new range with timestamp, or updates timestamp if range exists
func (gt *generatorTracker) addRangeWithTimestamp(startID uint64, rangeLen uint, timestamp time.Time) *MessageRange {
	if r, exists := gt.ranges[startID]; exists {
		r.Timestamp = timestamp
		return r
	}

	r := NewMessageRange(startID, rangeLen)
	r.Timestamp = timestamp
	gt.ranges[startID] = r
	return r
}

// unackedOlderThan returns the total number of unacked messages in ranges older than the given timestamp
func (gt *generatorTracker) unackedOlderThan(timestamp time.Time) uint {
	var total uint
	for _, r := range gt.ranges {
		// Only count ranges with a timestamp before the given timestamp
		if !r.Timestamp.IsZero() && r.Timestamp.Before(timestamp) {
			total += r.UnackedCount()
		}
	}
	return total
}

// Tracker is the main message tracking service
type Tracker struct {
	mu         sync.RWMutex
	generators map[string]*generatorTracker
}

// NewTracker creates a new message tracker
func NewTracker() *Tracker {
	return &Tracker{
		generators: make(map[string]*generatorTracker),
	}
}

// Ack acknowledges a message ID within a specific range for a generator
func (t *Tracker) Ack(generatorID string, startRangeID uint64, rangeLen uint, msgID uint64) bool {
	// Fast path: read lock to check if generator exists
	t.mu.RLock()
	gt, exists := t.generators[generatorID]
	t.mu.RUnlock()

	if !exists {
		// Need to create generator tracker
		t.mu.Lock()
		// Double-check after acquiring write lock
		gt, exists = t.generators[generatorID]
		if !exists {
			gt = newGeneratorTracker()
			t.generators[generatorID] = gt
		}
		t.mu.Unlock()
	}

	// Lock the generator tracker
	gt.mu.Lock()
	defer gt.mu.Unlock()

	// Find or create the range
	r := gt.findRange(startRangeID, rangeLen)
	if r == nil {
		r = gt.addRange(startRangeID, rangeLen)
	}

	// Ack the message
	return r.Ack(msgID)
}

// AddRange adds a message range for a generator without acking any messages
// The timestamp is recorded for the range. If the range already exists, the timestamp is updated.
func (t *Tracker) AddRange(generatorID string, startRangeID uint64, rangeLen uint, timestamp time.Time) {
	// Fast path: read lock to check if generator exists
	t.mu.RLock()
	gt, exists := t.generators[generatorID]
	t.mu.RUnlock()

	if !exists {
		// Need to create generator tracker
		t.mu.Lock()
		// Double-check after acquiring write lock
		gt, exists = t.generators[generatorID]
		if !exists {
			gt = newGeneratorTracker()
			t.generators[generatorID] = gt
		}
		t.mu.Unlock()
	}

	// Lock the generator tracker
	gt.mu.Lock()
	defer gt.mu.Unlock()

	// Add the range with timestamp (or update timestamp if it exists)
	gt.addRangeWithTimestamp(startRangeID, rangeLen, timestamp)
}

// IsAcked checks if a message ID has been acknowledged
func (t *Tracker) IsAcked(generatorID string, startRangeID uint64, rangeLen uint, msgID uint64) bool {
	t.mu.RLock()
	gt, exists := t.generators[generatorID]
	t.mu.RUnlock()

	if !exists {
		return false
	}

	gt.mu.RLock()
	defer gt.mu.RUnlock()

	r := gt.findRange(startRangeID, rangeLen)
	if r == nil {
		return false
	}

	return r.IsAcked(msgID)
}

// UnackedOlderThan returns a map of generator ID to the total number of unacked messages
// for ranges that have a timestamp before the given timestamp.
// Only includes generators that have unacked messages older than the timestamp.
func (t *Tracker) UnackedOlderThan(timestamp time.Time) map[string]uint {
	result := make(map[string]uint)

	t.mu.RLock()
	defer t.mu.RUnlock()

	for generatorID, gt := range t.generators {
		gt.mu.RLock()
		unacked := gt.unackedOlderThan(timestamp)
		gt.mu.RUnlock()

		if unacked > 0 {
			result[generatorID] = unacked
		}
	}

	return result
}
