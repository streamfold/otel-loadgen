package msg_tracker

import (
	"sync"
	"testing"
	"time"
)

func TestMessageRange_NewAndAck(t *testing.T) {
	mr := NewMessageRange(100, 200)

	if mr.StartID != 100 {
		t.Errorf("Expected StartID 100, got %d", mr.StartID)
	}
	if mr.EndID != 200 {
		t.Errorf("Expected EndID 200, got %d", mr.EndID)
	}

	// Ack a message in range
	if !mr.Ack(150) {
		t.Error("Expected Ack(150) to return true")
	}

	if !mr.IsAcked(150) {
		t.Error("Expected message 150 to be acked")
	}

	// Ack message at boundaries
	if !mr.Ack(100) {
		t.Error("Expected Ack(100) to return true")
	}
	if !mr.Ack(200) {
		t.Error("Expected Ack(200) to return true")
	}

	if !mr.IsAcked(100) {
		t.Error("Expected message 100 to be acked")
	}
	if !mr.IsAcked(200) {
		t.Error("Expected message 200 to be acked")
	}
}

func TestMessageRange_OutOfRange(t *testing.T) {
	mr := NewMessageRange(100, 200)

	// Ack outside range
	if mr.Ack(99) {
		t.Error("Expected Ack(99) to return false")
	}
	if mr.Ack(201) {
		t.Error("Expected Ack(201) to return false")
	}

	if mr.IsAcked(99) {
		t.Error("Expected message 99 to not be acked")
	}
	if mr.IsAcked(201) {
		t.Error("Expected message 201 to not be acked")
	}
}

func TestMessageRange_BitmaskStorage(t *testing.T) {
	// Test that bitmask correctly stores multiple acks
	mr := NewMessageRange(0, 127)

	// Ack multiple messages across different uint64 blocks
	mr.Ack(0)
	mr.Ack(1)
	mr.Ack(63)
	mr.Ack(64)
	mr.Ack(65)
	mr.Ack(127)

	if !mr.IsAcked(0) || !mr.IsAcked(1) || !mr.IsAcked(63) {
		t.Error("Expected messages in first block to be acked")
	}
	if !mr.IsAcked(64) || !mr.IsAcked(65) || !mr.IsAcked(127) {
		t.Error("Expected messages in second block to be acked")
	}

	// Check unacked messages
	if mr.IsAcked(2) || mr.IsAcked(62) || mr.IsAcked(66) || mr.IsAcked(126) {
		t.Error("Expected unacked messages to return false")
	}
}

func TestTracker_BasicAck(t *testing.T) {
	tracker := NewTracker()

	// Ack a message (should create generator and range automatically)
	if !tracker.Ack("gen1", 0, 100, 50) {
		t.Error("Expected Ack to return true")
	}

	// Verify it's acked
	if !tracker.IsAcked("gen1", 0, 100, 50) {
		t.Error("Expected message 50 to be acked")
	}

	// Verify other messages aren't acked
	if tracker.IsAcked("gen1", 0, 100, 51) {
		t.Error("Expected message 51 to not be acked")
	}
}

func TestTracker_MultipleGenerators(t *testing.T) {
	tracker := NewTracker()

	// Ack messages for different generators
	tracker.Ack("gen1", 0, 100, 50)
	tracker.Ack("gen2", 0, 100, 50)
	tracker.Ack("gen1", 0, 100, 75)

	// Verify isolation between generators
	if !tracker.IsAcked("gen1", 0, 100, 50) {
		t.Error("Expected gen1 message 50 to be acked")
	}
	if !tracker.IsAcked("gen1", 0, 100, 75) {
		t.Error("Expected gen1 message 75 to be acked")
	}
	if !tracker.IsAcked("gen2", 0, 100, 50) {
		t.Error("Expected gen2 message 50 to be acked")
	}
	if tracker.IsAcked("gen2", 0, 100, 75) {
		t.Error("Expected gen2 message 75 to not be acked")
	}
}

func TestTracker_MultipleRanges(t *testing.T) {
	tracker := NewTracker()

	// Ack messages in different ranges for same generator
	tracker.Ack("gen1", 0, 100, 50)
	tracker.Ack("gen1", 101, 200, 150)
	tracker.Ack("gen1", 0, 100, 75)

	// Verify both ranges work correctly
	if !tracker.IsAcked("gen1", 0, 100, 50) {
		t.Error("Expected message 50 in range 0-100 to be acked")
	}
	if !tracker.IsAcked("gen1", 0, 100, 75) {
		t.Error("Expected message 75 in range 0-100 to be acked")
	}
	if !tracker.IsAcked("gen1", 101, 200, 150) {
		t.Error("Expected message 150 in range 101-200 to be acked")
	}

	// Verify ranges are isolated
	if tracker.IsAcked("gen1", 101, 200, 50) {
		t.Error("Expected message 50 in range 101-200 to not be acked")
	}
}

func TestTracker_AddRange(t *testing.T) {
	tracker := NewTracker()

	// Add a range without acking
	ts := time.Now()
	tracker.AddRange("gen1", 0, 100, ts)

	// Verify range exists but no messages are acked
	if tracker.IsAcked("gen1", 0, 100, 50) {
		t.Error("Expected no messages to be acked after AddRange")
	}

	// Now ack a message in the range
	tracker.Ack("gen1", 0, 100, 50)

	if !tracker.IsAcked("gen1", 0, 100, 50) {
		t.Error("Expected message 50 to be acked")
	}
}

func TestTracker_Concurrency(t *testing.T) {
	tracker := NewTracker()

	// Test concurrent access
	var wg sync.WaitGroup
	numGoroutines := 100
	numMessages := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(genID int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				tracker.Ack("gen1", 0, 10000, int64(genID*numMessages+j))
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were acked
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numMessages; j++ {
			msgID := int64(i*numMessages + j)
			if !tracker.IsAcked("gen1", 0, 10000, msgID) {
				t.Errorf("Expected message %d to be acked", msgID)
			}
		}
	}
}

func TestTracker_ConcurrentGenerators(t *testing.T) {
	tracker := NewTracker()

	var wg sync.WaitGroup
	numGenerators := 50
	numMessages := 100

	// Concurrently ack messages across different generators
	for i := 0; i < numGenerators; i++ {
		wg.Add(1)
		go func(genNum int) {
			defer wg.Done()
			genID := string(rune('A' + genNum))
			for j := 0; j < numMessages; j++ {
				tracker.Ack(genID, 0, 1000, int64(j))
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages across all generators
	for i := 0; i < numGenerators; i++ {
		genID := string(rune('A' + i))
		for j := 0; j < numMessages; j++ {
			if !tracker.IsAcked(genID, 0, 1000, int64(j)) {
				t.Errorf("Expected message %d for generator %s to be acked", j, genID)
			}
		}
	}
}

func TestTracker_LargeRange(t *testing.T) {
	tracker := NewTracker()

	// Test with a large range
	startID := int64(0)
	endID := int64(1000000)

	tracker.AddRange("gen1", startID, endID, time.Now())

	// Ack some messages
	tracker.Ack("gen1", startID, endID, 0)
	tracker.Ack("gen1", startID, endID, 500000)
	tracker.Ack("gen1", startID, endID, 1000000)

	// Verify
	if !tracker.IsAcked("gen1", startID, endID, 0) {
		t.Error("Expected message 0 to be acked")
	}
	if !tracker.IsAcked("gen1", startID, endID, 500000) {
		t.Error("Expected message 500000 to be acked")
	}
	if !tracker.IsAcked("gen1", startID, endID, 1000000) {
		t.Error("Expected message 1000000 to be acked")
	}
	if tracker.IsAcked("gen1", startID, endID, 500001) {
		t.Error("Expected message 500001 to not be acked")
	}
}

func TestTracker_AddRangeTimestamp(t *testing.T) {
	tracker := NewTracker()

	// Add a range with initial timestamp
	ts1 := time.Now()
	tracker.AddRange("gen1", 0, 100, ts1)

	// Verify range exists
	tracker.mu.RLock()
	gt := tracker.generators["gen1"]
	tracker.mu.RUnlock()

	if gt == nil {
		t.Fatal("Expected generator to exist")
	}

	gt.mu.RLock()
	r := gt.findRange(0, 100)
	gt.mu.RUnlock()

	if r == nil {
		t.Fatal("Expected range to exist")
	}

	if !r.Timestamp.Equal(ts1) {
		t.Errorf("Expected timestamp %v, got %v", ts1, r.Timestamp)
	}

	// Update the same range with a new timestamp
	time.Sleep(10 * time.Millisecond) // Ensure different timestamp
	ts2 := time.Now()
	tracker.AddRange("gen1", 0, 100, ts2)

	// Verify timestamp was updated
	gt.mu.RLock()
	r = gt.findRange(0, 100)
	gt.mu.RUnlock()

	if r == nil {
		t.Fatal("Expected range to still exist")
	}

	if !r.Timestamp.Equal(ts2) {
		t.Errorf("Expected timestamp to be updated to %v, got %v", ts2, r.Timestamp)
	}

	if r.Timestamp.Equal(ts1) {
		t.Error("Expected timestamp to have changed from original")
	}
}

func TestMessageRange_AckCounters(t *testing.T) {
	mr := NewMessageRange(0, 100)

	// Initially counters should be zero
	if mr.AckedCount != 0 {
		t.Errorf("Expected AckedCount to be 0, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 0 {
		t.Errorf("Expected DuplicateCount to be 0, got %d", mr.DuplicateCount)
	}

	// Ack first message
	mr.Ack(50)
	if mr.AckedCount != 1 {
		t.Errorf("Expected AckedCount to be 1, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 0 {
		t.Errorf("Expected DuplicateCount to be 0, got %d", mr.DuplicateCount)
	}

	// Ack same message again (duplicate)
	mr.Ack(50)
	if mr.AckedCount != 1 {
		t.Errorf("Expected AckedCount to still be 1, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 1 {
		t.Errorf("Expected DuplicateCount to be 1, got %d", mr.DuplicateCount)
	}

	// Ack another new message
	mr.Ack(75)
	if mr.AckedCount != 2 {
		t.Errorf("Expected AckedCount to be 2, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 1 {
		t.Errorf("Expected DuplicateCount to still be 1, got %d", mr.DuplicateCount)
	}

	// Ack first message again
	mr.Ack(50)
	if mr.AckedCount != 2 {
		t.Errorf("Expected AckedCount to still be 2, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 2 {
		t.Errorf("Expected DuplicateCount to be 2, got %d", mr.DuplicateCount)
	}

	// Ack second message again
	mr.Ack(75)
	if mr.AckedCount != 2 {
		t.Errorf("Expected AckedCount to still be 2, got %d", mr.AckedCount)
	}
	if mr.DuplicateCount != 3 {
		t.Errorf("Expected DuplicateCount to be 3, got %d", mr.DuplicateCount)
	}
}

func TestMessageRange_UnackedCount(t *testing.T) {
	mr := NewMessageRange(0, 100)

	// Total messages: 101 (0-100 inclusive)
	if mr.TotalMessages() != 101 {
		t.Errorf("Expected TotalMessages to be 101, got %d", mr.TotalMessages())
	}

	// Initially all unacked
	if mr.UnackedCount() != 101 {
		t.Errorf("Expected UnackedCount to be 101, got %d", mr.UnackedCount())
	}

	// Ack one message
	mr.Ack(50)
	if mr.UnackedCount() != 100 {
		t.Errorf("Expected UnackedCount to be 100, got %d", mr.UnackedCount())
	}

	// Ack duplicate doesn't change unacked count
	mr.Ack(50)
	if mr.UnackedCount() != 100 {
		t.Errorf("Expected UnackedCount to still be 100, got %d", mr.UnackedCount())
	}

	// Ack 10 more messages
	for i := 0; i < 10; i++ {
		mr.Ack(int64(i))
	}
	if mr.UnackedCount() != 90 {
		t.Errorf("Expected UnackedCount to be 90, got %d", mr.UnackedCount())
	}

	// Ack all messages
	for i := 0; i <= 100; i++ {
		mr.Ack(int64(i))
	}
	if mr.UnackedCount() != 0 {
		t.Errorf("Expected UnackedCount to be 0, got %d", mr.UnackedCount())
	}
}

func TestTracker_UnackedOlderThan(t *testing.T) {
	tracker := NewTracker()

	// Create some ranges with different timestamps
	baseTime := time.Now()
	oldTime := baseTime.Add(-1 * time.Hour)
	recentTime := baseTime.Add(-10 * time.Minute)

	// Add ranges with old timestamp for gen1
	tracker.AddRange("gen1", 0, 99, oldTime)    // 100 messages
	tracker.AddRange("gen1", 100, 199, oldTime) // 100 messages

	// Add range with recent timestamp for gen1
	tracker.AddRange("gen1", 200, 299, recentTime) // 100 messages

	// Add range with old timestamp for gen2
	tracker.AddRange("gen2", 0, 49, oldTime) // 50 messages

	// Ack some messages in old ranges
	tracker.Ack("gen1", 0, 99, 10)
	tracker.Ack("gen1", 0, 99, 20)
	tracker.Ack("gen1", 100, 199, 100)

	// Ack some in recent range
	tracker.Ack("gen1", 200, 299, 250)

	// Query unacked older than recent time (should include old ranges only)
	unacked := tracker.UnackedOlderThan(recentTime)

	// gen1 should have: (100 - 2) + (100 - 1) = 98 + 99 = 197 unacked in old ranges
	if unacked["gen1"] != 197 {
		t.Errorf("Expected gen1 to have 197 unacked, got %d", unacked["gen1"])
	}

	// gen2 should have: 50 - 0 = 50 unacked
	if unacked["gen2"] != 50 {
		t.Errorf("Expected gen2 to have 50 unacked, got %d", unacked["gen2"])
	}

	// Query with a very old timestamp (should return nothing)
	veryOld := baseTime.Add(-2 * time.Hour)
	unacked = tracker.UnackedOlderThan(veryOld)
	if len(unacked) != 0 {
		t.Errorf("Expected no unacked messages older than very old timestamp, got %v", unacked)
	}

	// Ack all messages in gen2's range
	for i := int64(0); i <= 49; i++ {
		tracker.Ack("gen2", 0, 49, i)
	}

	// Query again - gen2 should not appear
	unacked = tracker.UnackedOlderThan(recentTime)
	if _, exists := unacked["gen2"]; exists {
		t.Error("Expected gen2 to not appear when all messages are acked")
	}
	if unacked["gen1"] != 197 {
		t.Errorf("Expected gen1 to still have 197 unacked, got %d", unacked["gen1"])
	}
}

func TestTracker_UnackedOlderThan_MultipleRanges(t *testing.T) {
	tracker := NewTracker()

	baseTime := time.Now()
	old1 := baseTime.Add(-2 * time.Hour)
	old2 := baseTime.Add(-1 * time.Hour)
	recent := baseTime.Add(-10 * time.Minute)

	// Add multiple ranges with different timestamps
	tracker.AddRange("gen1", 0, 9, old1)     // 10 messages
	tracker.AddRange("gen1", 10, 19, old2)   // 10 messages
	tracker.AddRange("gen1", 20, 29, recent) // 10 messages

	// Ack 5 messages in first range
	for i := int64(0); i < 5; i++ {
		tracker.Ack("gen1", 0, 9, i)
	}

	// Ack 3 messages in second range
	for i := int64(10); i < 13; i++ {
		tracker.Ack("gen1", 10, 19, i)
	}

	// Ack 2 messages in third range
	tracker.Ack("gen1", 20, 29, 20)
	tracker.Ack("gen1", 20, 29, 21)

	// Query with timestamp between old2 and recent
	queryTime := baseTime.Add(-30 * time.Minute)
	unacked := tracker.UnackedOlderThan(queryTime)

	// Should include first two ranges: (10 - 5) + (10 - 3) = 5 + 7 = 12
	if unacked["gen1"] != 12 {
		t.Errorf("Expected gen1 to have 12 unacked in old ranges, got %d", unacked["gen1"])
	}

	// Query with timestamp after old1 but before old2
	queryTime2 := baseTime.Add(-90 * time.Minute)
	unacked2 := tracker.UnackedOlderThan(queryTime2)

	// Should only include first range: 10 - 5 = 5
	if unacked2["gen1"] != 5 {
		t.Errorf("Expected gen1 to have 5 unacked, got %d", unacked2["gen1"])
	}
}
