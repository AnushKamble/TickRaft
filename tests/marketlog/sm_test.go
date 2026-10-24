package marketlog_test

import (
	"errors"
	"testing"
	"tickraft/internal/marketlog"
	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

func TestMarketLogSchemaValidation(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	// Begin Dataset
	beginCmd := &marketlogpb.BeginDatasetCommand{
		DatasetId:          "dataset-001",
		Symbol:             "AAPL",
		SessionDate:        "2026-10-24",
		ExpectedEvents:     2,
		Levels:             5,
		SourceManifestHash: "mock-hash",
		SchemaVersion:      "v1",
	}
	bytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, err := sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bytes})
	if err != nil {
		t.Fatalf("failed to apply begin dataset: %v", err)
	}

	manifest := sm.GetManifest()
	if manifest.DatasetID != "dataset-001" || manifest.Symbol != "AAPL" {
		t.Fatalf("unexpected manifest values: %+v", manifest)
	}
}

func TestAppendEventBatchAndDigest(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{
		DatasetId:      "dataset-seq-1",
		ExpectedEvents: 2,
	}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 1000,
			EventType:   1,
			OrderId:     1,
			Size:        100,
			Price:       10000,
			Direction:   1,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
		{
			SourceSeq:   2,
			TimestampNs: 2000,
			EventType:   1,
			OrderId:     2,
			Size:        50,
			Price:       10050,
			Direction:   -1,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 150}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}

	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "b1",
		DatasetId:   "dataset-seq-1",
		BatchSeq:    1,
		SeqStart:    1,
		SeqEnd:      2,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}
	aBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)
	resp, err := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: aBytes})
	if err != nil {
		t.Fatalf("failed to apply append batch: %v", err)
	}

	respMap, ok := resp.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map response")
	}
	if respMap["event_count"].(uint64) != 2 {
		t.Fatalf("expected event count 2, got %v", respMap["event_count"])
	}

	digest := respMap["rolling_digest"].(string)
	if digest == "" {
		t.Fatalf("rolling digest should not be empty")
	}
}

func TestSequenceGapRejection(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{DatasetId: "d1", ExpectedEvents: 10}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	// Propose batch starting at seq 5 (skipping 1..4!)
	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   5,
			TimestampNs: 5000,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "b-gap",
		SeqStart:    5,
		SeqEnd:      5,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}
	aBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)
	_, err := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: aBytes})

	if err == nil {
		t.Fatalf("expected error for sequence gap, got nil")
	}
	if !errors.Is(err, marketlog.ErrSequenceGap) {
		t.Fatalf("expected ErrSequenceGap, got: %v", err)
	}
}

func TestOrderBookInvariants(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{DatasetId: "d1"}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	// Crossed order book: bid price 10050 >= ask price 10040
	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 1000,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10040, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
			},
		},
	}
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "b-crossed",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}
	aBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)
	_, err := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: aBytes})

	if err == nil {
		t.Fatalf("expected error for crossed book, got nil")
	}
	if !errors.Is(err, marketlog.ErrInvalidOrderBook) {
		t.Fatalf("expected ErrInvalidOrderBook, got: %v", err)
	}
}

func TestFinalizeDataset(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{
		DatasetId:      "d-fin",
		ExpectedEvents: 1,
	}
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
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "b1",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}
	aBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)
	resp, _ := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: aBytes})
	computedDigest := resp.(map[string]interface{})["rolling_digest"].(string)

	// Finalize with correct expected values
	finCmd := &marketlogpb.FinalizeDatasetCommand{
		DatasetId:              "d-fin",
		ExpectedEvents:         1,
		FinalSeq:               1,
		ExpectedRollingDigest:  computedDigest,
	}
	fBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_FINALIZE_DATASET, finCmd)
	finResp, err := sm.Apply(&raftpb.LogEntry{Index: 3, Term: 1, Data: fBytes})
	if err != nil {
		t.Fatalf("failed to finalize dataset: %v", err)
	}

	manifest := finResp.(*marketlog.DatasetManifest)
	if !manifest.IsFinalized || manifest.AppliedEvents != 1 {
		t.Fatalf("manifest should be finalized: %+v", manifest)
	}

	// Any subsequent append should be rejected as already finalized
	_, errPost := sm.Apply(&raftpb.LogEntry{Index: 4, Term: 1, Data: aBytes})
	if !errors.Is(errPost, marketlog.ErrDatasetAlreadyFinalized) {
		t.Fatalf("expected ErrDatasetAlreadyFinalized, got: %v", errPost)
	}
}

func TestBatchSizeLimits(t *testing.T) {
	sm := marketlog.NewMarketLogStateMachine()

	beginCmd := &marketlogpb.BeginDatasetCommand{
		DatasetId: "d-batchsize",
	}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = sm.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	// 1. Empty batch must be rejected
	emptyCmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:  "b-empty",
		SeqStart: 1,
		SeqEnd:   1,
		Events:   nil,
	}
	eBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, emptyCmd)
	_, errEmpty := sm.Apply(&raftpb.LogEntry{Index: 2, Term: 1, Data: eBytes})
	if !errors.Is(errEmpty, marketlog.ErrInvalidBatchSize) {
		t.Fatalf("expected ErrInvalidBatchSize for empty batch, got: %v", errEmpty)
	}

	// 2. Oversized batch (>10,000) must be rejected
	oversized := make([]*marketlogpb.MarketEvent, marketlog.MaxEventsPerBatch+1)
	for i := range oversized {
		oversized[i] = &marketlogpb.MarketEvent{
			SourceSeq:   uint64(i + 1),
			TimestampNs: int64(1000 + i),
		}
	}
	overCmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:  "b-over",
		SeqStart: 1,
		SeqEnd:   uint64(len(oversized)),
		Events:   oversized,
	}
	oBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, overCmd)
	_, errOver := sm.Apply(&raftpb.LogEntry{Index: 3, Term: 1, Data: oBytes})
	if !errors.Is(errOver, marketlog.ErrInvalidBatchSize) {
		t.Fatalf("expected ErrInvalidBatchSize for oversized batch, got: %v", errOver)
	}
}
