package benchmark_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"tickraft/internal/cluster"
	"tickraft/internal/marketlog"
	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

type BenchmarkRunResult struct {
	BatchSize     int     `json:"batch_size"`
	TotalEvents   int     `json:"total_events"`
	TotalBatches  int     `json:"total_batches"`
	DurationMs    float64 `json:"duration_ms"`
	EventsPerSec  float64 `json:"events_per_sec"`
	BatchesPerSec float64 `json:"batches_per_sec"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`
	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
	TotalWALBytes int64   `json:"total_wal_bytes"`
}

type FullBenchmarkReport struct {
	Environment struct {
		OS        string `json:"os"`
		Arch      string `json:"arch"`
		NumCPU    int    `json:"num_cpu"`
		GoVersion string `json:"go_version"`
		Timestamp string `json:"timestamp"`
	} `json:"environment"`
	DirectReplay struct {
		TotalEvents  int     `json:"total_events"`
		DurationMs   float64 `json:"duration_ms"`
		EventsPerSec float64 `json:"events_per_sec"`
	} `json:"direct_replay"`
	ReplicatedRuns []BenchmarkRunResult `json:"replicated_runs"`
}

func generateSyntheticEvents(count int) []*marketlogpb.MarketEvent {
	events := make([]*marketlogpb.MarketEvent, count)
	for i := 0; i < count; i++ {
		seq := uint64(i + 1)
		events[i] = &marketlogpb.MarketEvent{
			SourceSeq:   seq,
			TimestampNs: int64(34200_000_000_000 + i*10_000_000),
			EventType:   1,
			OrderId:     seq,
			Size:        100,
			Price:       10000 + int64((i%10)*5),
			Direction:   1,
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: []*marketlogpb.OrderBookLevel{
					{Price: 10050, Size: 100},
					{Price: 10060, Size: 200},
				},
				Bids: []*marketlogpb.OrderBookLevel{
					{Price: 10000, Size: 100},
					{Price: 9990, Size: 200},
				},
			},
		}
	}
	return events
}

func TestBenchmarkSweep(t *testing.T) {
	totalEvents := 3000
	events := generateSyntheticEvents(totalEvents)

	var report FullBenchmarkReport
	report.Environment.OS = runtime.GOOS
	report.Environment.Arch = runtime.GOARCH
	report.Environment.NumCPU = runtime.NumCPU()
	report.Environment.GoVersion = runtime.Version()
	report.Environment.Timestamp = time.Now().UTC().Format(time.RFC3339)

	// 1. Benchmark Direct Local Replay (no consensus, single-process baseline)
	t.Log("Benchmarking direct local replay...")
	smLocal := marketlog.NewMarketLogStateMachine()
	beginCmd := &marketlogpb.BeginDatasetCommand{DatasetId: "bench-direct", ExpectedEvents: uint64(totalEvents)}
	bBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_BEGIN_DATASET, beginCmd)
	_, _ = smLocal.Apply(&raftpb.LogEntry{Index: 1, Term: 1, Data: bBytes})

	directStart := time.Now()
	// Apply in batches of 100
	directBatchSize := 100
	for i := 0; i < totalEvents; i += directBatchSize {
		end := i + directBatchSize
		if end > totalEvents {
			end = totalEvents
		}
		sub := events[i:end]
		cmd := &marketlogpb.AppendEventBatchCommand{
			BatchId:     fmt.Sprintf("direct-b-%d", i),
			SeqStart:    sub[0].SourceSeq,
			SeqEnd:      sub[len(sub)-1].SourceSeq,
			PayloadHash: marketlog.ComputeBatchPayloadHash(sub),
			Events:      sub,
		}
		cBytes, _ := marketlog.EncodeStateMachineCommand(marketlogpb.StateMachineCommand_APPEND_BATCH, cmd)
		_, _ = smLocal.Apply(&raftpb.LogEntry{Index: uint64(i/directBatchSize + 2), Term: 1, Data: cBytes})
	}
	directDuration := time.Since(directStart)
	report.DirectReplay.TotalEvents = totalEvents
	report.DirectReplay.DurationMs = float64(directDuration.Milliseconds())
	report.DirectReplay.EventsPerSec = float64(totalEvents) / directDuration.Seconds()
	t.Logf("Direct local replay: %.1f events/s (%v)", report.DirectReplay.EventsPerSec, directDuration)

	// 2. Benchmark Replicated Ingest across batch sizes 100, 500, 1000
	batchSizes := []int{100, 500, 1000}
	for _, bs := range batchSizes {
		t.Logf("Benchmarking replicated cluster at batch size %d...", bs)

		tempDir, err := os.MkdirTemp("", fmt.Sprintf("bench_cluster_%d_*", bs))
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}

		cl, err := cluster.NewLocalCluster(tempDir, 0)
		if err != nil {
			t.Fatalf("failed to init cluster: %v", err)
		}
		if err := cl.StartCluster(); err != nil {
			t.Fatalf("failed to start cluster: %v", err)
		}

		_, _, err = cl.WaitForLeader(8 * time.Second)
		if err != nil {
			cl.StopAll()
			os.RemoveAll(tempDir)
			t.Fatalf("leader election failed: %v", err)
		}

		client := transportgrpc.NewResilientClusterClient(cl.GetAddrs())

		// Begin dataset
		ctx := context.Background()
		_ = client.BeginDataset(ctx, &marketlogpb.BeginDatasetCommand{
			DatasetId:      fmt.Sprintf("bench-replicated-%d", bs),
			ExpectedEvents: uint64(totalEvents),
		})

		var latencies []float64
		numBatches := 0
		replicatedStart := time.Now()

		for i := 0; i < totalEvents; i += bs {
			end := i + bs
			if end > totalEvents {
				end = totalEvents
			}
			sub := events[i:end]
			batchID := fmt.Sprintf("rep-b-%d-%d", bs, i/bs)
			cmd := &marketlogpb.AppendEventBatchCommand{
				BatchId:     batchID,
				DatasetId:   fmt.Sprintf("bench-replicated-%d", bs),
				BatchSeq:    uint64(i/bs + 1),
				SeqStart:    sub[0].SourceSeq,
				SeqEnd:      sub[len(sub)-1].SourceSeq,
				PayloadHash: marketlog.ComputeBatchPayloadHash(sub),
				Events:      sub,
			}

			bStart := time.Now()
			resp, err := client.ProposeBatch(ctx, cmd)
			bLat := float64(time.Since(bStart).Microseconds()) / 1000.0 // ms
			if err == nil && resp.Success {
				latencies = append(latencies, bLat)
				numBatches++
			}
		}

		replicatedDuration := time.Since(replicatedStart)
		client.Close()
		cl.StopAll()

		// Measure WAL size on leader
		var walBytes int64 = 0
		_ = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && filepath.Ext(path) == ".wal" {
				walBytes += info.Size()
			}
			return nil
		})
		os.RemoveAll(tempDir)

		sort.Float64s(latencies)
		var p50, p95, p99, meanLat float64
		if len(latencies) > 0 {
			p50 = latencies[len(latencies)*50/100]
			p95 = latencies[len(latencies)*95/100]
			p99 = latencies[len(latencies)*99/100]
			var sum float64
			for _, l := range latencies {
				sum += l
			}
			meanLat = sum / float64(len(latencies))
		}

		runRes := BenchmarkRunResult{
			BatchSize:     bs,
			TotalEvents:   totalEvents,
			TotalBatches:  numBatches,
			DurationMs:    float64(replicatedDuration.Milliseconds()),
			EventsPerSec:  float64(totalEvents) / replicatedDuration.Seconds(),
			BatchesPerSec: float64(numBatches) / replicatedDuration.Seconds(),
			LatencyMeanMs: meanLat,
			LatencyP50Ms:  p50,
			LatencyP95Ms:  p95,
			LatencyP99Ms:  p99,
			TotalWALBytes: walBytes,
		}

		report.ReplicatedRuns = append(report.ReplicatedRuns, runRes)
		t.Logf("Replicated (batch=%d): %.1f events/s, %.1f batches/s, p50=%.2fms, p95=%.2fms, p99=%.2fms",
			bs, runRes.EventsPerSec, runRes.BatchesPerSec, p50, p95, p99)
	}

	// Save raw measurements
	outPath := filepath.Join("../../results/benchmarks", "raw_metrics.json")
	if _, err := os.Stat("../../results/benchmarks"); os.IsNotExist(err) {
		outPath = filepath.Join("results/benchmarks", "raw_metrics.json")
	}
	bytes, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		_ = os.WriteFile(outPath, bytes, 0644)
		t.Logf("Raw benchmark metrics saved to %s", outPath)
	}
}
