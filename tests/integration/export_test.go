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

func TestExportCommittedOnly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "export_test_*")
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

	_, leaderAddr, err := cl.WaitForLeader(8 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose 2 events in batch
	events := []*marketlogpb.MarketEvent{
		{SourceSeq: 1, TimestampNs: 1000, EventType: 1, Size: 10, Price: 1000, Direction: 1},
		{SourceSeq: 2, TimestampNs: 2000, EventType: 1, Size: 20, Price: 1000, Direction: -1},
	}
	cmd := &marketlogpb.AppendEventBatchCommand{
		BatchId:     "exp-batch-1",
		DatasetId:   "exp-ds-1",
		SeqStart:    1,
		SeqEnd:      2,
		PayloadHash: marketlog.ComputeBatchPayloadHash(events),
		Events:      events,
	}

	resp, err := client.ProposeBatch(ctx, cmd)
	if err != nil || !resp.Success {
		t.Fatalf("propose failed: %v", err)
	}

	// Export committed from leader
	expResp, err := client.ExportCommitted(ctx, leaderAddr, "exp-ds-1", 1, 0)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	if expResp.EventCount != 2 {
		t.Fatalf("expected 2 exported events, got %d", expResp.EventCount)
	}
	if expResp.Events[0].SourceSeq != 1 || expResp.Events[1].SourceSeq != 2 {
		t.Fatalf("unexpected sequence order in exported events")
	}
}

func TestExportManifestVerification(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "manifest_verif_*")
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

	_, leaderAddr, err := cl.WaitForLeader(8 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	msgPath := "data/fixtures/synthetic_message.csv"
	bookPath := "data/fixtures/synthetic_orderbook.csv"
	if _, err := os.Stat(msgPath); os.IsNotExist(err) {
		msgPath = "../../data/fixtures/synthetic_message.csv"
		bookPath = "../../data/fixtures/synthetic_orderbook.csv"
	}

	// Parse synthetic fixtures
	events, _, _, err := marketlog.ParseLOBSTERFiles(msgPath, bookPath, 5)
	if err != nil {
		t.Fatalf("failed to parse synthetic fixture: %v", err)
	}

	client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Propose all events in batches of 4
	batchSize := 4
	expectedDigest := ""
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		subEvents := events[i:end]
		for _, e := range subEvents {
			expectedDigest = marketlog.ComputeRollingDigest(expectedDigest, e)
		}

		cmd := &marketlogpb.AppendEventBatchCommand{
			BatchId:     string(rune('A' + i/batchSize)),
			DatasetId:   "synth-verif-ds",
			SeqStart:    subEvents[0].SourceSeq,
			SeqEnd:      subEvents[len(subEvents)-1].SourceSeq,
			PayloadHash: marketlog.ComputeBatchPayloadHash(subEvents),
			Events:      subEvents,
		}
		resp, err := client.ProposeBatch(ctx, cmd)
		if err != nil || !resp.Success {
			t.Fatalf("batch append failed at %d: %v", i, err)
		}
	}

	// Export committed and verify against source
	expResp, err := client.ExportCommitted(ctx, leaderAddr, "synth-verif-ds", 1, 0)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	if expResp.EventCount != uint64(len(events)) {
		t.Fatalf("event count mismatch: exported %d, source %d", expResp.EventCount, len(events))
	}

	if expResp.RollingDigest != expectedDigest {
		t.Fatalf("rolling digest mismatch: exported %s, expected %s", expResp.RollingDigest, expectedDigest)
	}
}
