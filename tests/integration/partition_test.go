package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"tickraft/internal/cluster"
	"tickraft/internal/marketlog"
	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
)

func TestMinorityCannotCommit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "partition_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cl, err := cluster.NewLocalCluster(tempDir, 0)
	if err != nil {
		t.Fatalf("failed to init cluster: %v", err)
	}
	if err := cl.StartCluster(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cl.StopAll()

	leaderID, _, err := cl.WaitForLeader(8 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	// Kill 2 followers so the leader is in an isolated minority of 1
	var followersToKill []string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if id != leaderID {
			followersToKill = append(followersToKill, id)
		}
	}

	for _, fID := range followersToKill {
		if err := cl.KillNode(fID); err != nil {
			t.Fatalf("failed to kill follower %s: %v", fID, err)
		}
	}

	// Now leader has lost quorum. Client proposes a batch.
	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	events := []*marketlogpb.MarketEvent{
		{SourceSeq: 1, TimestampNs: 1000, EventType: 1, Size: 10, Price: 1000, Direction: 1},
	}
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "minority-batch",
		DatasetId:   "minority-ds",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}

	// Proposal MUST NOT commit because leader cannot reach a majority quorum!
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := client.ProposeBatch(ctx, cmd)
	if err == nil && resp.Success {
		t.Fatalf("CRITICAL SAFETY VIOLATION: minority partition reported committed write!")
	}

	t.Logf("Minority write successfully failed closed: err=%v", err)
}
