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

func TestThreeNodeClusterLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cluster_test_*")
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

	leaderID, leaderAddr, err := cl.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed waiting for leader: %v", err)
	}
	t.Logf("Elected leader %s at %s", leaderID, leaderAddr)

	// Create resilient client
	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	// Propose a market event batch via gRPC client
	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 34200000000000,
			EventType:   1,
			OrderId:     101,
			Size:        100,
			Price:       10000,
			Direction:   1,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 100}},
			},
		},
	}

	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "batch-test-001",
		DatasetId:   "dataset-synthetic",
		BatchSeq:    1,
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ProposeBatch(ctx, cmd)
	if err != nil {
		t.Fatalf("batch proposal failed via gRPC: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected batch success, got error: %s", resp.ErrorMessage)
	}
	if resp.CommittedIndex == 0 {
		t.Fatalf("expected non-zero committed index")
	}

	t.Logf("Batch successfully committed at index %d", resp.CommittedIndex)

	// Wait for followers to catch up
	time.Sleep(200 * time.Millisecond)

	for _, addr := range cl.GetAddrs() {
		status, err := client.GetNodeStatus(ctx, addr)
		if err != nil {
			t.Fatalf("failed to get status from %s: %v", addr, err)
		}
		if status.CommitIndex < resp.CommittedIndex {
			t.Fatalf("node at %s has commitIndex %d < %d", addr, status.CommitIndex, resp.CommittedIndex)
		}
		if status.EventCount != 1 {
			t.Fatalf("node at %s has eventCount %d != 1", addr, status.EventCount)
		}
	}
}

func TestFollowerLeaderRedirect(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "redirect_test_*")
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

	_, _, err = cl.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	// Client starts with list of all addresses
	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	events := []*marketlogpb.MarketEvent{
		{
			SourceSeq:   1,
			TimestampNs: 34200000000000,
			EventType:   1,
			OrderId:     201,
			Size:        50,
			Price:       10000,
			Direction:   1,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{{Price: 10050, Size: 100}},
				Bids: []*marketlogpb.OrderBookLevel{{Price: 10000, Size: 50}},
			},
		},
	}

	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "redirect-batch-1",
		DatasetId:   "dataset-redirect",
		BatchSeq:    1,
		SeqStart:    1,
		SeqEnd:      1,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ProposeBatch(ctx, cmd)
	if err != nil {
		t.Fatalf("proposal with redirection failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected redirect success, got: %s", resp.ErrorMessage)
	}
}

func TestHealthStatusRPC(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "health_test_*")
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

	_, _, err = cl.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	leaderCount := 0
	followerCount := 0

	for _, addr := range cl.GetAddrs() {
		status, err := client.GetNodeStatus(ctx, addr)
		if err != nil {
			t.Fatalf("failed to query status from %s: %v", addr, err)
		}
		if status.NodeId == "" {
			t.Fatalf("node_id should not be empty")
		}
		if status.Term == 0 {
			t.Fatalf("term should be >= 1")
		}

		if status.Role == "LEADER" {
			leaderCount++
		} else if status.Role == "FOLLOWER" {
			followerCount++
		}
	}

	if leaderCount != 1 || followerCount != 2 {
		t.Fatalf("expected 1 leader and 2 followers, got %d leaders, %d followers", leaderCount, followerCount)
	}
}
