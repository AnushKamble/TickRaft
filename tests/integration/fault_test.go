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

func TestLeaderCrashMidStream(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "leader_crash_*")
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

	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	ctx := context.Background()

	// Propose Batch 1 successfully
	events1 := []*marketlogpb.MarketEvent{
		{SourceSeq: 1, TimestampNs: 1000, EventType: 1, Size: 10, Price: 1000, Direction: 1},
	}
	cmd1 := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "crash-batch-1",
		DatasetId:   "crash-ds",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events1),
		Events:      events1,
	}
	resp1, err := client.ProposeBatch(ctx, cmd1)
	if err != nil || !resp1.Success {
		t.Fatalf("batch 1 failed: %v", err)
	}

	t.Logf("Killing active leader %s mid-stream...", leaderID)
	if err := cl.KillNode(leaderID); err != nil {
		t.Fatalf("failed to kill leader: %v", err)
	}

	// Remaining 2 nodes form majority quorum and must elect new leader
	time.Sleep(500 * time.Millisecond)
	newLeaderID, _, err := cl.WaitForLeader(8 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader after crash: %v", err)
	}
	t.Logf("Elected replacement leader %s", newLeaderID)

	if newLeaderID == leaderID {
		t.Fatalf("new leader cannot be the killed node")
	}

	// Propose Batch 2 to new leader
	events2 := []*marketlogpb.MarketEvent{
		{SourceSeq: 2, TimestampNs: 2000, EventType: 1, Size: 20, Price: 1000, Direction: -1},
	}
	cmd2 := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "crash-batch-2",
		DatasetId:   "crash-ds",
		SeqStart:    2,
		SeqEnd:      2,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events2),
		Events:      events2,
	}

	resp2, err := client.ProposeBatch(ctx, cmd2)
	if err != nil || !resp2.Success {
		t.Fatalf("batch 2 proposal failed under new leader: %v", err)
	}
	t.Logf("Batch 2 committed under new leader!")

	// Restart the crashed node from disk
	t.Logf("Restarting crashed node %s from WAL...", leaderID)
	if err := cl.RestartNode(leaderID); err != nil {
		t.Fatalf("failed to restart node %s: %v", leaderID, err)
	}

	// Wait for caught-up convergence
	time.Sleep(500 * time.Millisecond)

	// Check status across all 3 nodes
	for _, addr := range cl.GetAddrs() {
		ctxStatus, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		status, err := client.GetNodeStatus(ctxStatus, addr)
		cancel()
		if err == nil {
			if status.EventCount != 2 {
				t.Fatalf("node at %s has eventCount %d != 2 after healing", addr, status.EventCount)
			}
		}
	}
}

func TestLostClientResponseRetry(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lost_resp_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cl, err := cluster.NewLocalCluster(tempDir, 0)
	if err != nil {
		t.Fatalf("failed to init cluster: %v", err)
	}
	_ = cl.StartCluster()
	defer cl.StopAll()

	_, _, err = cl.WaitForLeader(8 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	events := []*marketlogpb.MarketEvent{
		{SourceSeq: 1, TimestampNs: 1000, EventType: 1, Size: 10, Price: 1000, Direction: 1},
	}
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "lost-resp-batch",
		DatasetId:   "lost-resp-ds",
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}

	// Initial submit
	resp1, err := client.ProposeBatch(context.Background(), cmd)
	if err != nil || !resp1.Success {
		t.Fatalf("first proposal failed: %v", err)
	}

	// Simulate client assuming failure/timeout and retrying identical batch ID + hash
	resp2, err := client.ProposeBatch(context.Background(), cmd)
	if err != nil || !resp2.Success {
		t.Fatalf("retry failed: %v", err)
	}
	if !resp2.IsDuplicate {
		t.Fatalf("expected retry to be recognized as idempotent duplicate")
	}
}
