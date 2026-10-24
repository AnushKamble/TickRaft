package marketlog

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

var (
	ErrSequenceGap            = errors.New("sequence gap detected in market event batch")
	ErrTimestampRegression    = errors.New("timestamp regression detected in market events")
	ErrInvalidOrderBook       = errors.New("order book invariant violated: bid_price >= ask_price or zero size")
	ErrManifestMismatch       = errors.New("finalized dataset manifest mismatch (event count, sequence, or rolling digest)")
	ErrDatasetAlreadyFinalized= errors.New("dataset has already been finalized")
)

type MarketLogStateMachine struct {
	mu sync.RWMutex

	// Dataset active session
	activeDatasetID    string
	symbol             string
	sessionDate        string
	expectedEvents     uint64
	levels             uint32
	sourceManifestHash string
	schemaVersion      string

	// Sequential progress
	lastAppliedSeq uint64
	lastTimestamp  int64
	eventCount     uint64
	rollingDigest  string
	isFinalized    bool

	// Committed events store for export
	committedEvents []*marketlogpb.MarketEvent

	// Idempotency dedup table
	idempotency *IdempotencyTracker
}

func NewMarketLogStateMachine() *MarketLogStateMachine {
	return &MarketLogStateMachine{
		idempotency:     NewIdempotencyTracker(),
		committedEvents: make([]*marketlogpb.MarketEvent, 0),
	}
}

func (sm *MarketLogStateMachine) Apply(entry *raftpb.LogEntry) (interface{}, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(entry.Data) == 0 {
		return nil, nil // No-op entry
	}

	cmd, err := DecodeStateMachineCommand(entry.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode log entry command: %w", err)
	}

	switch cmd.Type {
	case marketlogpb.StateMachineCommand_BEGIN_DATASET:
		beginCmd := &marketlogpb.BeginDatasetCommand{}
		if err := proto.Unmarshal(cmd.Payload, beginCmd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BeginDatasetCommand: %w", err)
		}
		return sm.applyBeginDataset(beginCmd)

	case marketlogpb.StateMachineCommand_APPEND_BATCH:
		appendCmd := &marketlogpb.AppendEventBatchCommand{}
		if err := proto.Unmarshal(cmd.Payload, appendCmd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal AppendEventBatchCommand: %w", err)
		}
		return sm.applyAppendBatch(appendCmd)

	case marketlogpb.StateMachineCommand_FINALIZE_DATASET:
		finalizeCmd := &marketlogpb.FinalizeDatasetCommand{}
		if err := proto.Unmarshal(cmd.Payload, finalizeCmd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal FinalizeDatasetCommand: %w", err)
		}
		return sm.applyFinalizeDataset(finalizeCmd)

	default:
		return nil, fmt.Errorf("unknown state machine command type: %v", cmd.Type)
	}
}

func (sm *MarketLogStateMachine) applyBeginDataset(cmd *marketlogpb.BeginDatasetCommand) (interface{}, error) {
	sm.activeDatasetID = cmd.DatasetId
	sm.symbol = cmd.Symbol
	sm.sessionDate = cmd.SessionDate
	sm.expectedEvents = cmd.ExpectedEvents
	sm.levels = cmd.Levels
	sm.sourceManifestHash = cmd.SourceManifestHash
	sm.schemaVersion = cmd.SchemaVersion

	sm.lastAppliedSeq = 0
	sm.lastTimestamp = 0
	sm.eventCount = 0
	sm.rollingDigest = ""
	sm.isFinalized = false
	sm.committedEvents = make([]*marketlogpb.MarketEvent, 0)

	return &DatasetManifest{
		DatasetID:          sm.activeDatasetID,
		Symbol:             sm.symbol,
		SessionDate:        sm.sessionDate,
		ExpectedEvents:     sm.expectedEvents,
		SourceManifestHash: sm.sourceManifestHash,
	}, nil
}

func (sm *MarketLogStateMachine) applyAppendBatch(cmd *marketlogpb.AppendEventBatchCommand) (interface{}, error) {
	if sm.isFinalized {
		return nil, ErrDatasetAlreadyFinalized
	}

	// 1. Check idempotency contract
	isDuplicate, err := sm.idempotency.CheckAndRecord(
		cmd.BatchId,
		cmd.PayloadHash,
		cmd.SeqStart,
		cmd.SeqEnd,
	)
	if err != nil {
		return nil, err // ErrDuplicateBatchConflict
	}
	if isDuplicate {
		// Idempotent retry: do not re-apply duplicate events to state machine
		return map[string]interface{}{
			"batch_id":     cmd.BatchId,
			"is_duplicate": true,
			"applied_seq":  sm.lastAppliedSeq,
		}, nil
	}

	// 2. Validate contiguous sequence numbering
	if sm.lastAppliedSeq+1 != cmd.SeqStart {
		return nil, fmt.Errorf("%w: expected seq_start %d, got %d",
			ErrSequenceGap, sm.lastAppliedSeq+1, cmd.SeqStart)
	}

	// 3. Process events and validate order book invariants
	for _, event := range cmd.Events {
		// Timestamp monotonicity
		if event.TimestampNs < sm.lastTimestamp {
			return nil, fmt.Errorf("%w: event timestamp %d < prev %d",
				ErrTimestampRegression, event.TimestampNs, sm.lastTimestamp)
		}
		sm.lastTimestamp = event.TimestampNs

		// Order book invariants
		if event.Book != nil && len(event.Book.Bids) > 0 && len(event.Book.Asks) > 0 {
			bestBid := event.Book.Bids[0]
			bestAsk := event.Book.Asks[0]
			if bestBid.Price >= bestAsk.Price {
				return nil, fmt.Errorf("%w: crossed book bid=%d >= ask=%d",
					ErrInvalidOrderBook, bestBid.Price, bestAsk.Price)
			}
			if bestBid.Size == 0 || bestAsk.Size == 0 {
				return nil, fmt.Errorf("%w: zero size at top of book", ErrInvalidOrderBook)
			}
		}

		// Update rolling SHA-256 digest
		sm.rollingDigest = ComputeRollingDigest(sm.rollingDigest, event)
		sm.committedEvents = append(sm.committedEvents, event)
		sm.eventCount++
	}

	sm.lastAppliedSeq = cmd.SeqEnd

	return map[string]interface{}{
		"batch_id":       cmd.BatchId,
		"is_duplicate":   false,
		"last_seq":       sm.lastAppliedSeq,
		"event_count":    sm.eventCount,
		"rolling_digest": sm.rollingDigest,
	}, nil
}

func (sm *MarketLogStateMachine) applyFinalizeDataset(cmd *marketlogpb.FinalizeDatasetCommand) (interface{}, error) {
	if sm.eventCount != cmd.ExpectedEvents {
		return nil, fmt.Errorf("%w: expected %d events, applied %d",
			ErrManifestMismatch, cmd.ExpectedEvents, sm.eventCount)
	}
	if sm.lastAppliedSeq != cmd.FinalSeq {
		return nil, fmt.Errorf("%w: expected final seq %d, applied %d",
			ErrManifestMismatch, cmd.FinalSeq, sm.lastAppliedSeq)
	}
	if cmd.ExpectedRollingDigest != "" && sm.rollingDigest != cmd.ExpectedRollingDigest {
		return nil, fmt.Errorf("%w: expected digest %s, computed %s",
			ErrManifestMismatch, cmd.ExpectedRollingDigest, sm.rollingDigest)
	}

	sm.isFinalized = true

	return &DatasetManifest{
		DatasetID:          sm.activeDatasetID,
		Symbol:             sm.symbol,
		SessionDate:        sm.sessionDate,
		ExpectedEvents:     sm.expectedEvents,
		AppliedEvents:      sm.eventCount,
		SeqStart:           1,
		SeqEnd:             sm.lastAppliedSeq,
		RollingDigest:      sm.rollingDigest,
		SourceManifestHash: sm.sourceManifestHash,
		IsFinalized:        true,
	}, nil
}

func (sm *MarketLogStateMachine) GetManifest() DatasetManifest {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return DatasetManifest{
		DatasetID:          sm.activeDatasetID,
		Symbol:             sm.symbol,
		SessionDate:        sm.sessionDate,
		ExpectedEvents:     sm.expectedEvents,
		AppliedEvents:      sm.eventCount,
		SeqStart:           1,
		SeqEnd:             sm.lastAppliedSeq,
		RollingDigest:      sm.rollingDigest,
		SourceManifestHash: sm.sourceManifestHash,
		IsFinalized:        sm.isFinalized,
	}
}

func (sm *MarketLogStateMachine) GetCommittedEvents(startSeq, endSeq uint64) []*marketlogpb.MarketEvent {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var result []*marketlogpb.MarketEvent
	for _, e := range sm.committedEvents {
		if e.SourceSeq >= startSeq && (endSeq == 0 || e.SourceSeq <= endSeq) {
			result = append(result, e)
		}
	}
	return result
}
