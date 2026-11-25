package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"tickraft/internal/marketlog"
	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
)

func main() {
	datasetID := flag.String("dataset-id", "synthetic-dataset-01", "Unique dataset session identifier")
	symbol := flag.String("symbol", "SYNTH", "Ticker symbol")
	msgPath := flag.String("message-file", "data/fixtures/synthetic_message.csv", "Path to LOBSTER message CSV")
	bookPath := flag.String("book-file", "data/fixtures/synthetic_orderbook.csv", "Path to LOBSTER orderbook CSV")
	levels := flag.Int("levels", 5, "Number of order book levels")
	batchSize := flag.Int("batch-size", 5, "Max events per batch")
	clusterAddrsFlag := flag.String("cluster-addrs", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "Comma-separated cluster addresses")
	flag.Parse()

	if *batchSize <= 0 || *batchSize > marketlog.MaxEventsPerBatch {
		log.Fatalf("Invalid batch-size %d: must be between 1 and %d", *batchSize, marketlog.MaxEventsPerBatch)
	}

	log.Printf("[TickRaft-Import] Parsing input files %s and %s...", *msgPath, *bookPath)
	events, msgHash, bookHash, err := marketlog.ParseLOBSTERFiles(*msgPath, *bookPath, *levels)
	if err != nil {
		log.Fatalf("Parse error: %v", err)
	}

	totalEvents := len(events)
	log.Printf("[TickRaft-Import] Loaded %d events. MsgHash: %s, BookHash: %s", totalEvents, msgHash[:8], bookHash[:8])

	clusterAddrs := strings.Split(*clusterAddrsFlag, ",")
	client := transportgrpc.NewResilientClusterClient(clusterAddrs)
	defer client.Close()

	ctx := context.Background()

	// 1. BeginDataset command
	log.Printf("[TickRaft-Import] Proposing BeginDataset for %s...", *datasetID)
	beginCmd := &marketlogpb.BeginDatasetCommand{
		DatasetId:          *datasetID,
		Symbol:             *symbol,
		SessionDate:        time.Now().Format("2006-01-02"),
		ExpectedEvents:     uint64(totalEvents),
		Levels:             uint32(*levels),
		SourceManifestHash: msgHash + ":" + bookHash,
		SchemaVersion:      "v1",
	}

	if err := client.BeginDataset(ctx, beginCmd); err != nil {
		log.Fatalf("BeginDataset failed: %v", err)
	}

	// 2. Submit batches
	rollingDigest := ""
	batchSeq := uint64(1)
	startTime := time.Now()

	for i := 0; i < totalEvents; i += *batchSize {
		end := i + *batchSize
		if end > totalEvents {
			end = totalEvents
		}

		batchEvents := events[i:end]
		for _, e := range batchEvents {
			rollingDigest = marketlog.ComputeRollingDigest(rollingDigest, e)
		}

		batchID := fmt.Sprintf("%s-batch-%05d", *datasetID, batchSeq)
		cmd := &marketlogpb.AppendEventBatchCommand{
			BatchId:     batchID,
			DatasetId:   *datasetID,
			BatchSeq:    batchSeq,
			SeqStart:    batchEvents[0].SourceSeq,
			SeqEnd:      batchEvents[len(batchEvents)-1].SourceSeq,
			PayloadHash: marketlog.ComputeBatchPayloadHash(batchEvents),
			Events:      batchEvents,
		}

		resp, err := client.ProposeBatch(ctx, cmd)
		if err != nil {
			log.Fatalf("AppendEventBatch %s failed: %v", batchID, err)
		}
		if !resp.Success {
			log.Fatalf("AppendEventBatch %s rejected: %s", batchID, resp.ErrorMessage)
		}

		batchSeq++
	}

	elapsed := time.Since(startTime)
	log.Printf("[TickRaft-Import] Appended %d events in %d batches (duration: %v, rate: %.1f events/s)",
		totalEvents, batchSeq-1, elapsed, float64(totalEvents)/elapsed.Seconds())

	// 3. FinalizeDataset command
	log.Printf("[TickRaft-Import] Finalizing dataset %s with digest %s...", *datasetID, rollingDigest[:8])
	finCmd := &marketlogpb.FinalizeDatasetCommand{
		DatasetId:             *datasetID,
		ExpectedEvents:        uint64(totalEvents),
		FinalSeq:              uint64(totalEvents),
		ExpectedRollingDigest: rollingDigest,
		SourceManifestHash:    msgHash + ":" + bookHash,
	}

	if err := client.FinalizeDataset(ctx, finCmd); err != nil {
		log.Fatalf("FinalizeDataset failed: %v", err)
	}

	log.Printf("[TickRaft-Import] Successfully imported and finalized dataset %s!", *datasetID)
}
