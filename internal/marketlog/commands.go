package marketlog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	marketlogpb "tickraft/proto/marketlog"
)

const (
	MaxEventsPerBatch = 10000
)

var (
	ErrInvalidBatchSize = errors.New("batch event count must be between 1 and 10000")
)

// Helper to wrap commands into raftpb.LogEntry data
func EncodeStateMachineCommand(cmdType marketlogpb.StateMachineCommand_CommandType, msg proto.Message) ([]byte, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inner command: %w", err)
	}

	cmd := &marketlogpb.StateMachineCommand{
		Type:    cmdType,
		Payload: payload,
	}

	return proto.Marshal(cmd)
}

func DecodeStateMachineCommand(data []byte) (*marketlogpb.StateMachineCommand, error) {
	cmd := &marketlogpb.StateMachineCommand{}
	if err := proto.Unmarshal(data, cmd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state machine command: %w", err)
	}
	return cmd, nil
}

// ComputeBatchPayloadHash calculates the deterministic SHA-256 hash of events in a batch
func ComputeBatchPayloadHash(events []*marketlogpb.MarketEvent) string {
	h := sha256.New()
	for _, e := range events {
		bytes, _ := proto.Marshal(e)
		h.Write(bytes)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ComputeRollingDigest updates a SHA-256 rolling digest
func ComputeRollingDigest(currentDigest string, event *marketlogpb.MarketEvent) string {
	h := sha256.New()
	if currentDigest != "" {
		prevBytes, _ := hex.DecodeString(currentDigest)
		h.Write(prevBytes)
	}
	bytes, _ := proto.Marshal(event)
	h.Write(bytes)
	return hex.EncodeToString(h.Sum(nil))
}
