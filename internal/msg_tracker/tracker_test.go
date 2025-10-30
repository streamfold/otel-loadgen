package msg_tracker

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestMessageRange_NewAndAck(t *testing.T) {
	mr := NewMessageRange(100, 101) // 101 messages: 100-200 inclusive

	if mr.StartID != 100 {
		t.Errorf("Expected StartID 100, got %d", mr.StartID)
	}
	if mr.RangeLen != 101 {
		t.Errorf("Expected RangeLen 101, got %d", mr.RangeLen)
	}

	// Ack a message in range
	result, success := mr.Ack(150)
	if !success {
		t.Error("Expected Ack(150) to return success=true")
	}
	if !result.Acked {
		t.Error("Expected Ack(150) to return Acked=true")
	}

	if !mr.IsAcked(150) {
		t.Error("Expected message 150 to be acked")
	}

	// Ack message at boundaries
	result, success = mr.Ack(100)
	if !success || !result.Acked {
		t.Error("Expected Ack(100) to succeed")
	}
	result, success = mr.Ack(200)
	if !success || !result.Acked {
		t.Error("Expected Ack(200) to succeed")
	}

	if !mr.IsAcked(100) {
		t.Error("Expected message 100 to be acked")
	}
	if !mr.IsAcked(200) {
		t.Error("Expected message 200 to be acked")
	}
}

func TestMessageRange_OutOfRange(t *testing.T) {
	mr := NewMessageRange(100, 101) // 101 messages: 100-200 inclusive

	// Ack outside range
	_, success := mr.Ack(99)
	if success {
		t.Error("Expected Ack(99) to return success=false")
	}
	_, success = mr.Ack(201)
	if success {
		t.Error("Expected Ack(201) to return success=false")
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
	mr := NewMessageRange(0, 128) // 128 messages: 0-127 inclusive

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
	tracker := NewTracker(zap.NewNop())

	// Ack a message (should create generator and range automatically)
	if !tracker.Ack("gen1", 0, 101, 50) {
		t.Error("Expected Ack to return true")
	}

	// Verify it's acked
	if !tracker.IsAcked("gen1", 0, 101, 50) {
		t.Error("Expected message 50 to be acked")
	}

	// Verify other messages aren't acked
	if tracker.IsAcked("gen1", 0, 101, 51) {
		t.Error("Expected message 51 to not be acked")
	}
}

func TestTracker_MultipleGenerators(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Ack messages for different generators
	tracker.Ack("gen1", 0, 101, 50)
	tracker.Ack("gen2", 0, 101, 50)
	tracker.Ack("gen1", 0, 101, 75)

	// Verify isolation between generators
	if !tracker.IsAcked("gen1", 0, 101, 50) {
		t.Error("Expected gen1 message 50 to be acked")
	}
	if !tracker.IsAcked("gen1", 0, 101, 75) {
		t.Error("Expected gen1 message 75 to be acked")
	}
	if !tracker.IsAcked("gen2", 0, 101, 50) {
		t.Error("Expected gen2 message 50 to be acked")
	}
	if tracker.IsAcked("gen2", 0, 101, 75) {
		t.Error("Expected gen2 message 75 to not be acked")
	}
}

func TestTracker_MultipleRanges(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Ack messages in different ranges for same generator
	tracker.Ack("gen1", 0, 101, 50)
	tracker.Ack("gen1", 101, 100, 150)
	tracker.Ack("gen1", 0, 101, 75)

	// Verify both ranges work correctly
	if !tracker.IsAcked("gen1", 0, 101, 50) {
		t.Error("Expected message 50 in range 0-100 to be acked")
	}
	if !tracker.IsAcked("gen1", 0, 101, 75) {
		t.Error("Expected message 75 in range 0-100 to be acked")
	}
	if !tracker.IsAcked("gen1", 101, 100, 150) {
		t.Error("Expected message 150 in range 101-200 to be acked")
	}

	// Verify ranges are isolated
	if tracker.IsAcked("gen1", 101, 100, 50) {
		t.Error("Expected message 50 in range 101-200 to not be acked")
	}
}

func TestTracker_AddRange(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Add a range without acking
	ts := time.Now()
	tracker.AddRange("gen1", 0, 101, ts)

	// Verify range exists but no messages are acked
	if tracker.IsAcked("gen1", 0, 101, 50) {
		t.Error("Expected no messages to be acked after AddRange")
	}

	// Now ack a message in the range
	tracker.Ack("gen1", 0, 101, 50)

	if !tracker.IsAcked("gen1", 0, 101, 50) {
		t.Error("Expected message 50 to be acked")
	}
}

func TestTracker_Concurrency(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Test concurrent access
	var wg sync.WaitGroup
	numGoroutines := 100
	numMessages := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(genID int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				tracker.Ack("gen1", 0, 10001, uint64(genID*numMessages+j))
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were acked
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numMessages; j++ {
			msgID := uint64(i*numMessages + j)
			if !tracker.IsAcked("gen1", 0, 10001, msgID) {
				t.Errorf("Expected message %d to be acked", msgID)
			}
		}
	}
}

func TestTracker_ConcurrentGenerators(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

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
				tracker.Ack(genID, 0, 1001, uint64(j))
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages across all generators
	for i := 0; i < numGenerators; i++ {
		genID := string(rune('A' + i))
		for j := 0; j < numMessages; j++ {
			if !tracker.IsAcked(genID, 0, 1001, uint64(j)) {
				t.Errorf("Expected message %d for generator %s to be acked", j, genID)
			}
		}
	}
}

func TestTracker_LargeRange(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Test with a large range
	startID := uint64(0)
	rangeLen := uint(1000001)

	tracker.AddRange("gen1", startID, rangeLen, time.Now())

	// Ack some messages
	tracker.Ack("gen1", startID, rangeLen, 0)
	tracker.Ack("gen1", startID, rangeLen, 500000)
	tracker.Ack("gen1", startID, rangeLen, 1000000)

	// Verify
	if !tracker.IsAcked("gen1", startID, rangeLen, 0) {
		t.Error("Expected message 0 to be acked")
	}
	if !tracker.IsAcked("gen1", startID, rangeLen, 500000) {
		t.Error("Expected message 500000 to be acked")
	}
	if !tracker.IsAcked("gen1", startID, rangeLen, 1000000) {
		t.Error("Expected message 1000000 to be acked")
	}
	if tracker.IsAcked("gen1", startID, rangeLen, 500001) {
		t.Error("Expected message 500001 to not be acked")
	}
}

func TestMessageRange_AckCounters(t *testing.T) {
	mr := NewMessageRange(0, 101)

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
	mr := NewMessageRange(0, 101)

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
		mr.Ack(uint64(i))
	}
	if mr.UnackedCount() != 90 {
		t.Errorf("Expected UnackedCount to be 90, got %d", mr.UnackedCount())
	}

	// Ack all messages
	for i := 0; i <= 100; i++ {
		mr.Ack(uint64(i))
	}
	if mr.UnackedCount() != 0 {
		t.Errorf("Expected UnackedCount to be 0, got %d", mr.UnackedCount())
	}
}

func TestTracker_UnackedOlderThan(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Create some ranges with different timestamps
	baseTime := time.Now()
	oldTime := baseTime.Add(-1 * time.Hour)
	recentTime := baseTime.Add(-10 * time.Minute)

	// Add ranges with old timestamp for gen1
	tracker.AddRange("gen1", 0, 100, oldTime)   // 100 messages
	tracker.AddRange("gen1", 100, 100, oldTime) // 100 messages

	// Add range with recent timestamp for gen1
	tracker.AddRange("gen1", 200, 100, recentTime) // 100 messages

	// Add range with old timestamp for gen2
	tracker.AddRange("gen2", 0, 50, oldTime) // 50 messages

	// Ack some messages in old ranges
	tracker.Ack("gen1", 0, 100, 10)
	tracker.Ack("gen1", 0, 100, 20)
	tracker.Ack("gen1", 100, 100, 100)

	// Ack some in recent range
	tracker.Ack("gen1", 200, 100, 250)

	// Query unacked older than recent time (should include old ranges only)
	reports := tracker.GeneratorReport(recentTime)

	// gen1 should have: (100 - 2) + (100 - 1) = 98 + 99 = 197 unacked in old ranges
	if reports["gen1"].Unacked != 197 {
		t.Errorf("Expected gen1 to have 197 unacked, got %d", reports["gen1"].Unacked)
	}

	// gen2 should have: 50 - 0 = 50 unacked
	if reports["gen2"].Unacked != 50 {
		t.Errorf("Expected gen2 to have 50 unacked, got %d", reports["gen2"].Unacked)
	}

	// Query with a very old timestamp (should return nothing with unacked)
	veryOld := baseTime.Add(-2 * time.Hour)
	reports = tracker.GeneratorReport(veryOld)
	hasUnacked := false
	for _, report := range reports {
		if report.Unacked > 0 {
			hasUnacked = true
			break
		}
	}
	if hasUnacked {
		t.Errorf("Expected no unacked messages older than very old timestamp, got %v", reports)
	}

	// Ack all messages in gen2's range
	for i := uint64(0); i <= 49; i++ {
		tracker.Ack("gen2", 0, 50, i)
	}

	// Query again - gen2 should have 0 unacked
	reports = tracker.GeneratorReport(recentTime)
	if reports["gen2"].Unacked != 0 {
		t.Errorf("Expected gen2 to have 0 unacked when all messages are acked, got %d", reports["gen2"].Unacked)
	}
	if reports["gen1"].Unacked != 197 {
		t.Errorf("Expected gen1 to still have 197 unacked, got %d", reports["gen1"].Unacked)
	}
}

func TestTracker_UnackedOlderThan_MultipleRanges(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	baseTime := time.Now()
	old1 := baseTime.Add(-2 * time.Hour)
	old2 := baseTime.Add(-1 * time.Hour)
	recent := baseTime.Add(-10 * time.Minute)

	// Add multiple ranges with different timestamps
	tracker.AddRange("gen1", 0, 10, old1)    // 10 messages
	tracker.AddRange("gen1", 10, 10, old2)   // 10 messages
	tracker.AddRange("gen1", 20, 10, recent) // 10 messages

	// Ack 5 messages in first range
	for i := uint64(0); i < 5; i++ {
		tracker.Ack("gen1", 0, 10, i)
	}

	// Ack 3 messages in second range
	for i := uint64(10); i < 13; i++ {
		tracker.Ack("gen1", 10, 10, i)
	}

	// Ack 2 messages in third range
	tracker.Ack("gen1", 20, 10, 20)
	tracker.Ack("gen1", 20, 10, 21)

	// Query with timestamp between old2 and recent
	queryTime := baseTime.Add(-30 * time.Minute)
	reports := tracker.GeneratorReport(queryTime)

	// Should include first two ranges: (10 - 5) + (10 - 3) = 5 + 7 = 12
	if reports["gen1"].Unacked != 12 {
		t.Errorf("Expected gen1 to have 12 unacked in old ranges, got %d", reports["gen1"].Unacked)
	}

	// Query with timestamp after old1 but before old2
	queryTime2 := baseTime.Add(-90 * time.Minute)
	reports2 := tracker.GeneratorReport(queryTime2)

	// Should only include first range: 10 - 5 = 5
	if reports2["gen1"].Unacked != 5 {
		t.Errorf("Expected gen1 to have 5 unacked, got %d", reports2["gen1"].Unacked)
	}
}

func TestMessageRange_TotalAckedCount(t *testing.T) {
	mr := NewMessageRange(0, 100)

	// Initially zero
	if mr.TotalAckedCount() != 0 {
		t.Errorf("Expected TotalAckedCount to be 0, got %d", mr.TotalAckedCount())
	}

	// Ack some messages
	mr.Ack(10)
	mr.Ack(20)
	mr.Ack(30)

	if mr.TotalAckedCount() != 3 {
		t.Errorf("Expected TotalAckedCount to be 3, got %d", mr.TotalAckedCount())
	}

	// Ack duplicate - count should not increase
	mr.Ack(10)

	if mr.TotalAckedCount() != 3 {
		t.Errorf("Expected TotalAckedCount to still be 3, got %d", mr.TotalAckedCount())
	}

	// Ack more messages
	for i := uint64(40); i < 50; i++ {
		mr.Ack(i)
	}

	if mr.TotalAckedCount() != 13 {
		t.Errorf("Expected TotalAckedCount to be 13, got %d", mr.TotalAckedCount())
	}
}

func TestTracker_ComplexScenario(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	baseTime := time.Now()
	oldTime := baseTime.Add(-2 * time.Hour)
	recentTime := baseTime.Add(-30 * time.Minute)

	// Setup: Create multiple generators with multiple ranges
	// gen1: 2 old ranges, 1 recent range
	tracker.AddRange("gen1", 0, 100, oldTime)
	tracker.AddRange("gen1", 100, 100, oldTime)
	tracker.AddRange("gen1", 200, 100, recentTime)

	// gen2: 1 old range
	tracker.AddRange("gen2", 0, 50, oldTime)

	// gen3: 1 recent range
	tracker.AddRange("gen3", 0, 75, recentTime)

	// Ack various messages
	// gen1: 30 in first range, 40 in second range, 25 in third range
	for i := uint64(0); i < 30; i++ {
		tracker.Ack("gen1", 0, 100, i)
	}
	for i := uint64(100); i < 140; i++ {
		tracker.Ack("gen1", 100, 100, i)
	}
	for i := uint64(200); i < 225; i++ {
		tracker.Ack("gen1", 200, 100, i)
	}

	// gen2: 10 acked
	for i := uint64(0); i < 10; i++ {
		tracker.Ack("gen2", 0, 50, i)
	}

	// gen3: 50 acked
	for i := uint64(0); i < 50; i++ {
		tracker.Ack("gen3", 0, 75, i)
	}

	// Test AckedCount
	acked := tracker.AckedCount()
	if acked["gen1"] != 95 { // 30 + 40 + 25
		t.Errorf("Expected gen1 to have 95 acked, got %d", acked["gen1"])
	}
	if acked["gen2"] != 10 {
		t.Errorf("Expected gen2 to have 10 acked, got %d", acked["gen2"])
	}
	if acked["gen3"] != 50 {
		t.Errorf("Expected gen3 to have 50 acked, got %d", acked["gen3"])
	}

	// Test GeneratorReport with queryTime between old and recent
	queryTime := baseTime.Add(-1 * time.Hour)
	reports := tracker.GeneratorReport(queryTime)

	// gen1 old ranges: (100 - 30) + (100 - 40) = 70 + 60 = 130
	if reports["gen1"].Unacked != 130 {
		t.Errorf("Expected gen1 to have 130 unacked in old ranges, got %d", reports["gen1"].Unacked)
	}

	// gen2: 50 - 10 = 40
	if reports["gen2"].Unacked != 40 {
		t.Errorf("Expected gen2 to have 40 unacked, got %d", reports["gen2"].Unacked)
	}

	// gen3 should have 0 unacked (its range is recent)
	if reports["gen3"].Unacked != 0 {
		t.Errorf("Expected gen3 to have 0 unacked in old ranges, got %d", reports["gen3"].Unacked)
	}

	// Test GeneratorReport with very recent time (should include all ranges)
	veryRecentTime := baseTime.Add(-1 * time.Minute)
	reportsAll := tracker.GeneratorReport(veryRecentTime)

	// gen1: 130 (old) + (100 - 25) (recent) = 130 + 75 = 205
	if reportsAll["gen1"].Unacked != 205 {
		t.Errorf("Expected gen1 to have 205 total unacked, got %d", reportsAll["gen1"].Unacked)
	}

	// gen2: 40
	if reportsAll["gen2"].Unacked != 40 {
		t.Errorf("Expected gen2 to have 40 unacked, got %d", reportsAll["gen2"].Unacked)
	}

	// gen3: 75 - 50 = 25
	if reportsAll["gen3"].Unacked != 25 {
		t.Errorf("Expected gen3 to have 25 unacked, got %d", reportsAll["gen3"].Unacked)
	}
}

func TestMessageRange_BoundaryConditions(t *testing.T) {
	// Test with single message range
	mr1 := NewMessageRange(100, 1)

	_, success := mr1.Ack(100)
	if !success {
		t.Error("Expected single message to be ackable")
	}

	if mr1.TotalMessages() != 1 {
		t.Errorf("Expected TotalMessages to be 1, got %d", mr1.TotalMessages())
	}

	if mr1.UnackedCount() != 0 {
		t.Errorf("Expected UnackedCount to be 0, got %d", mr1.UnackedCount())
	}

	// Test with range at uint64 boundaries
	maxStart := uint64(1<<63 - 1000)
	mr2 := NewMessageRange(maxStart, 100)

	_, success = mr2.Ack(maxStart)
	if !success {
		t.Error("Expected first message in large range to be ackable")
	}

	_, success = mr2.Ack(maxStart + 50)
	if !success {
		t.Error("Expected middle message in large range to be ackable")
	}

	_, success = mr2.Ack(maxStart + 99)
	if !success {
		t.Error("Expected last message in large range to be ackable")
	}

	if mr2.TotalAckedCount() != 3 {
		t.Errorf("Expected 3 acked messages, got %d", mr2.TotalAckedCount())
	}
}

func TestTracker_NonExistentGenerator(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Query non-existent generator
	if tracker.IsAcked("nonexistent", 0, 100, 50) {
		t.Error("Expected non-existent generator to have no acked messages")
	}

	acked := tracker.AckedCount()
	if len(acked) != 0 {
		t.Error("Expected empty acked count for non-existent generator")
	}

	reports := tracker.GeneratorReport(time.Now())
	if reports["nonexistent"].Unacked != 0 {
		t.Error("Expected no unacked messages for non-existent generator")
	}
}

func TestTracker_UpdateRange_Basic(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Add a range with 1000 messages
	tracker.AddRange("gen1", 0, 1000, time.Now())

	// Ack some messages
	tracker.Ack("gen1", 0, 1000, 100)
	tracker.Ack("gen1", 0, 1000, 200)
	tracker.Ack("gen1", 0, 1000, 300)

	// Verify unacked count with original range
	tracker.mu.RLock()
	gt := tracker.generators["gen1"]
	tracker.mu.RUnlock()

	gt.mu.RLock()
	r := gt.ranges[0]
	initialUnacked := r.UnackedCount()
	gt.mu.RUnlock()

	if initialUnacked != 997 { // 1000 - 3 acked
		t.Errorf("Expected 997 unacked initially, got %d", initialUnacked)
	}

	// Update range to be smaller (truncated to 500 messages)
	tracker.UpdateRange("gen1", 0, 500)

	// Verify the range length was updated
	gt.mu.RLock()
	r = gt.ranges[0]
	updatedLen := r.RangeLen
	updatedUnacked := r.UnackedCount()
	gt.mu.RUnlock()

	if updatedLen != 500 {
		t.Errorf("Expected RangeLen to be updated to 500, got %d", updatedLen)
	}

	if updatedUnacked != 497 { // 500 - 3 acked
		t.Errorf("Expected 497 unacked after update, got %d", updatedUnacked)
	}

	// Verify acked messages are still tracked
	if !tracker.IsAcked("gen1", 0, 500, 100) {
		t.Error("Expected message 100 to still be acked after range update")
	}
	if !tracker.IsAcked("gen1", 0, 500, 200) {
		t.Error("Expected message 200 to still be acked after range update")
	}
	if !tracker.IsAcked("gen1", 0, 500, 300) {
		t.Error("Expected message 300 to still be acked after range update")
	}

	// Verify messages beyond new range are no longer valid
	if tracker.IsAcked("gen1", 0, 500, 600) {
		t.Error("Expected message 600 to not be acked (beyond updated range)")
	}
}

func TestTracker_UpdateRange_WithAckedMessages(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	// Add a range
	tracker.AddRange("gen1", 0, 1000, time.Now())

	// Ack messages throughout the range
	for i := uint64(0); i < 100; i++ {
		tracker.Ack("gen1", 0, 1000, i*10)
	}

	// Verify initial acked count
	acked := tracker.AckedCount()
	if acked["gen1"] != 100 {
		t.Errorf("Expected 100 acked messages, got %d", acked["gen1"])
	}

	// Update range to be much smaller (only includes first 500 messages)
	tracker.UpdateRange("gen1", 0, 500)

	// Verify acked count remains the same (acked messages don't change)
	acked = tracker.AckedCount()
	if acked["gen1"] != 100 {
		t.Errorf("Expected 100 acked messages after range update, got %d", acked["gen1"])
	}

	// Verify messages within new range are still acked
	if !tracker.IsAcked("gen1", 0, 500, 0) {
		t.Error("Expected message 0 to still be acked")
	}
	if !tracker.IsAcked("gen1", 0, 500, 100) {
		t.Error("Expected message 100 to still be acked")
	}
	if !tracker.IsAcked("gen1", 0, 500, 490) {
		t.Error("Expected message 490 to still be acked")
	}

	// Messages beyond new range should not be queryable with new range length
	// Note: IsAcked checks if msgID is in range, so 600 is out of range for length 500
	if tracker.IsAcked("gen1", 0, 500, 600) {
		t.Error("Expected message 600 to be out of range")
	}
}

func TestTracker_UpdateRange_AffectsUnackedCount(t *testing.T) {
	tracker := NewTracker(zap.NewNop())

	baseTime := time.Now()
	oldTime := baseTime.Add(-1 * time.Hour)

	// Add a range with old timestamp
	tracker.AddRange("gen1", 0, 1000, oldTime)

	// Ack 100 messages
	for i := uint64(0); i < 100; i++ {
		tracker.Ack("gen1", 0, 1000, i)
	}

	// Check unacked count before update
	reports := tracker.GeneratorReport(baseTime)
	if reports["gen1"].Unacked != 900 { // 1000 - 100
		t.Errorf("Expected 900 unacked before update, got %d", reports["gen1"].Unacked)
	}

	// Update range to be smaller (200 messages)
	tracker.UpdateRange("gen1", 0, 200)

	// Check unacked count after update
	reports = tracker.GeneratorReport(baseTime)
	if reports["gen1"].Unacked != 100 { // 200 - 100
		t.Errorf("Expected 100 unacked after update, got %d", reports["gen1"].Unacked)
	}

	// Verify TotalAcked remains the same
	if reports["gen1"].TotalAcked != 100 {
		t.Errorf("Expected TotalAcked to remain 100, got %d", reports["gen1"].TotalAcked)
	}
}
