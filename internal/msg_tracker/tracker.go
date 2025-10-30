package msg_tracker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// MessageRange represents a range of message IDs with a bitmask for tracking acknowledgments
type MessageRange struct {
	sync.RWMutex
	StartID        uint64
	RangeLen       uint
	Timestamp      time.Time
	AckedCount     uint     // Number of unique messages acked
	DuplicateCount uint     // Number of duplicate acks received
	bitmap         []uint64 // Each uint64 holds 64 bits
}

// Per-generator report
type GeneratorReport struct {
	Unacked          uint
	TotalAcked       uint
	TotalDuped       uint
	OldestUnackedAge time.Time
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

type AckedResult struct {
	// Maybe should be an enum?
	Dup   bool
	Acked bool
}

// Ack marks a message ID as acknowledged
// Returns true if the message was in range, false otherwise

func (mr *MessageRange) Ack(msgID uint64) (AckedResult, bool) {
	var result AckedResult

	mr.Lock()
	defer mr.Unlock()

	if !mr.contains(msgID) {
		fmt.Printf("does not contain msgID: %d\n", msgID)
		return result, false
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
		result.Dup = true
	} else {
		mr.AckedCount++
		result.Acked = true
	}

	return result, true
}

// IsAcked checks if a message ID has been acknowledged
func (mr *MessageRange) IsAcked(msgID uint64) bool {
	mr.RLock()
	defer mr.RUnlock()

	if !mr.contains(msgID) {
		return false
	}

	offset := msgID - mr.StartID
	idx := offset / 64
	bit := offset % 64

	return (mr.bitmap[idx] & (1 << bit)) != 0
}

// Contains checks if the range contains the given message ID
func (mr *MessageRange) contains(msgID uint64) bool {
	return msgID >= mr.StartID && msgID < mr.StartID+uint64(mr.RangeLen)
}

// TotalMessages returns the total number of messages in the range
func (mr *MessageRange) TotalMessages() uint {
	// This field is static
	return mr.RangeLen
}

func (mr *MessageRange) TotalAckedCount() uint {
	mr.RLock()
	defer mr.RUnlock()

	return mr.AckedCount
}

// UnackedCount returns the number of messages that have not been acknowledged
func (mr *MessageRange) UnackedCount() uint {
	mr.RLock()
	defer mr.RUnlock()

	return mr.TotalMessages() - mr.AckedCount
}

func (mr *MessageRange) GetTimestamp() time.Time {
	mr.RLock()
	defer mr.RUnlock()

	return mr.Timestamp
}

func (mr *MessageRange) UpdateTimestamp(timestamp time.Time) {
	mr.Lock()
	defer mr.Unlock()

	mr.Timestamp = timestamp
}

func (mr *MessageRange) OlderThan(timestamp time.Time) bool {
	mr.RLock()
	defer mr.RUnlock()

	return !mr.Timestamp.IsZero() && mr.Timestamp.Before(timestamp)
}

// generatorTracker holds all ranges for a specific generator ID
type generatorTracker struct {
	mu         sync.RWMutex
	totalAcked atomic.Uint64
	totalDuped atomic.Uint64
	ranges     map[uint64]*MessageRange // Key is startID, we assume ranges are unique
}

func newGeneratorTracker() *generatorTracker {
	return &generatorTracker{
		ranges: make(map[uint64]*MessageRange),
	}
}

// findRange finds the range containing the given message ID
func (gt *generatorTracker) findRange(startRangeID uint64) *MessageRange {
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
		r.UpdateTimestamp(timestamp)
		r.Timestamp = timestamp
		return r
	}

	r := NewMessageRange(startID, rangeLen)
	r.Timestamp = timestamp
	gt.ranges[startID] = r
	return r
}

// unackedOlderThan returns the total number of unacked messages in ranges older than the given timestamp and the oldest timestamp
func (gt *generatorTracker) unackedOlderThan(timestamp time.Time) (uint, time.Time) {
	var total uint
	var oldestTime time.Time
	for _, r := range gt.ranges {
		// Only count ranges with a timestamp before the given timestamp
		if r.OlderThan(timestamp) {
			unacked := r.UnackedCount()
			if unacked > 0 {
				ts := r.GetTimestamp()
				if oldestTime.IsZero() || ts.Before(oldestTime) {
					oldestTime = r.GetTimestamp()
				}
			}

			total += unacked
		}
	}
	return total, oldestTime
}

func (gt *generatorTracker) ackedCount() uint {
	var total uint
	for _, r := range gt.ranges {
		total += r.TotalAckedCount()
	}
	return total
}

// Tracker is the main message tracking service
type Tracker struct {
	mu         sync.RWMutex
	log *zap.Logger
	generators map[string]*generatorTracker
}

// NewTracker creates a new message tracker
func NewTracker(log *zap.Logger) *Tracker {
	return &Tracker{
		log: log,
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
	gt.mu.RLock()

	// Find or create the range
	r, exists := gt.ranges[startRangeID]
	if !exists {
		// Upgrade to write lock
		gt.mu.RUnlock()

		gt.mu.Lock()
		r = gt.addRange(startRangeID, rangeLen)
		gt.mu.Unlock()
	} else {
		gt.mu.RUnlock()
	}

	// Ack the message
	result, success := r.Ack(msgID)
	if success {
		if result.Dup {
			gt.totalDuped.Add(1)
		} else if result.Acked {
			gt.totalAcked.Add(1)
		}
	}

	return success
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

// UpdateRange is called when the load generator exists before sending the entire range of messages. In this
// case we need to know that the range is truncated, otherwise messages would be marked as unacked
func (t *Tracker) UpdateRange(generatorID string, startRangeID uint64, rangeLen uint) {
	t.mu.RLock()
	gt, exists := t.generators[generatorID]
	t.mu.RUnlock()

	if !exists {
		t.log.Warn("attempt to update a range for unknown generator ID")
		return
	}
	
	gt.mu.RLock()
	r, exists := gt.ranges[startRangeID]
	gt.mu.RUnlock()
	
	if !exists {
		t.log.Warn("attempt to update a range that does not exist")
		return
	}
	
	r.Lock()
	defer r.Unlock()
	r.RangeLen = rangeLen
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

	r, exists := gt.ranges[startRangeID]
	if !exists {
		return false
	}

	return r.IsAcked(msgID)
}

func (t *Tracker) AckedCount() map[string]uint {
	result := make(map[string]uint)

	t.mu.RLock()
	defer t.mu.RUnlock()

	for generatorID, gt := range t.generators {
		gt.mu.RLock()
		acked := gt.ackedCount()
		gt.mu.RUnlock()

		if acked > 0 {
			result[generatorID] = acked
		}
	}

	return result
}

// UnackedOlderThan returns a map of generator ID to the total number of unacked messages
// for ranges that have a timestamp before the given timestamp.
// Only includes generators that have unacked messages older than the timestamp.
func (t *Tracker) GeneratorReport(timestamp time.Time) map[string]GeneratorReport {
	result := make(map[string]GeneratorReport)

	t.mu.RLock()
	defer t.mu.RUnlock()

	for generatorID, gt := range t.generators {
		gt.mu.RLock()
		unacked, oldestTime := gt.unackedOlderThan(timestamp)
		gt.mu.RUnlock()

		result[generatorID] = GeneratorReport{
			Unacked:          unacked,
			TotalAcked:       uint(gt.totalAcked.Load()),
			TotalDuped:       uint(gt.totalDuped.Load()),
			OldestUnackedAge: oldestTime,
		}
	}

	return result
}
