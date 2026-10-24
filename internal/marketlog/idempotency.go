package marketlog

import (
	"errors"
	"sync"
)

var (
	ErrDuplicateBatchConflict = errors.New("conflicting batch proposal: batch_id already exists with differing payload hash")
)

type BatchDedupRecord struct {
	BatchID     string
	PayloadHash string
	SeqStart    uint64
	SeqEnd      uint64
}

type IdempotencyTracker struct {
	mu      sync.RWMutex
	records map[string]BatchDedupRecord
}

func NewIdempotencyTracker() *IdempotencyTracker {
	return &IdempotencyTracker{
		records: make(map[string]BatchDedupRecord),
	}
}

// CheckAndRecord checks idempotency rules:
// - returns isDuplicate=false, err=nil if new batch
// - returns isDuplicate=true, err=nil if identical idempotent retry (same ID & same hash)
// - returns isDuplicate=false, err=ErrDuplicateBatchConflict if same ID with different hash
func (t *IdempotencyTracker) CheckAndRecord(batchID, payloadHash string, seqStart, seqEnd uint64) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	existing, exists := t.records[batchID]
	if !exists {
		t.records[batchID] = BatchDedupRecord{
			BatchID:     batchID,
			PayloadHash: payloadHash,
			SeqStart:    seqStart,
			SeqEnd:      seqEnd,
		}
		return false, nil
	}

	if existing.PayloadHash == payloadHash {
		// Idempotent retry of identical batch
		return true, nil
	}

	// Deterministic conflict: same batch ID proposed with different payload
	return false, ErrDuplicateBatchConflict
}
