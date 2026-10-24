package marketlog_test

import (
	"errors"
	"testing"
	"tickraft/internal/marketlog"
	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

func TestIdempotentRetrySameHash(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{DatasetId: "d1"}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 1000,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}
	payloadHash := marketlog.ComputeBatchPayloadHash(events)

	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "batch-idem-1",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: payloadHash,
		Events:      events,
	}
	bytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)

	// First application
	resp1, err := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: bytes})
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	if resp1.(map[string]interface{})["is_duplicate"].(bool) {
		t.Fatalf("first apply should not be duplicate")
	}

	// Retry identical batch (same batch_id, same payload_hash)
	resp2, err2 := sm.Apply(&raftpb.LogEntry{Index: 3, Term: 1, Data: bytes})
	if err2 != nil {
		t.Fatalf("idempotent retry should succeed, got err: %v", err2)
	}
	if !resp2.(map[string]interface{})["is_duplicate"].(bool) {
		t.Fatalf("retry should be marked as duplicate")
	}

	// Event count in state machine should still be 1 (NOT duplicated!)
	manifest := sm.GetManifest()
	if manifest.AppliedEvents != 1 {
		t.Fatalf("expected 1 applied event, got %d", manifest.AppliedEvents)
	}
}

func TestConflictSameIdDifferentHash(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{DatasetId: "d1"}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	events1 := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 1000,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}
	cmd1 := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "batch-conflict-1",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events1),
		Events:      events1,
	}
	bytes1, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd1)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: bytes1})

	// Propose SAME batch ID with DIFFERENT events / different payload hash
	events2 := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 2000, // Different timestamp!
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}
	cmd2 := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "batch-conflict-1", // SAME ID!
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events2), // DIFFERENT HASH!
		Events:      events2,
	}
	bytes2, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd2)

	_, errConflict := sm.Apply(&raftpb.LogEntry{Index: 3, Term: 1, Data: bytes2})
	if errConflict == nil {
		t.Fatalf("expected conflicting batch to be rejected, got nil error")
	}
	if !errors.Is(errConflict, marketlog.ErrDuplicateBatchConflict) {
		t.Fatalf("expected ErrDuplicateBatchConflict, got: %v", errConflict)
	}
}
