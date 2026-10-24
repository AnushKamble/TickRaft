package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
)

type ExportManifest struct {
	DatasetID          string `json:"dataset_id"`
	ExportTime         string `json:"export_time"`
	EventCount         uint64 `json:"event_count"`
	SeqStart           uint64 `json:"seq_start"`
	SeqEnd             uint64 `json:"seq_end"`
	RollingDigest      string `json:"rolling_digest"`
	ExportFile         string `json:"export_file"`
	ExportFileSHA256   string `json:"export_file_sha256"`
	SourceManifestHash string `json:"source_manifest_hash"`
}

func main() {
	datasetID := flag.String("dataset-id", "synthetic-dataset-01", "Dataset to export")
	outDir := flag.String("out-dir", "data/export", "Directory for export outputs")
	clusterAddrsFlag := flag.String("cluster-addrs", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "Cluster addresses")
	flag.Parse()

	_ = os.MkdirAll(*outDir, 0755)

	clusterAddrs := strings.Split(*clusterAddrsFlag, ",")
	client := transportgrpc.NewResilientClusterClient(clusterAddrs)
	defer client.Close()

	ctx := context.Background()
	log.Printf("[TickRaft-Export] Exporting committed replay for %s...", *datasetID)

	var resp *marketlogpb.ExportCommittedResponse
	var err error

	for _, addr := range clusterAddrs {
		resp, err = client.ExportCommitted(ctx, addr, *datasetID, 1, 0)
		if err == nil && len(resp.Events) > 0 {
			break
		}
	}

	if err != nil || resp == nil || len(resp.Events) == 0 {
		log.Fatalf("ExportCommitted failed: %v", err)
	}

	log.Printf("[TickRaft-Export] Received %d committed events. Seq %d to %d, Digest %s",
		resp.EventCount, resp.SeqStart, resp.SeqEnd, resp.RollingDigest[:8])

	// Materialize to CSV format
	exportFileName := fmt.Sprintf("%s_committed_replay.csv", *datasetID)
	exportFilePath := filepath.Join(*outDir, exportFileName)

	f, err := os.Create(exportFilePath)
	if err != nil {
		log.Fatalf("failed to create export file: %v", err)
	}

	hExport := sha256.New()

	// Write CSV header
	header := "source_seq,timestamp_ns,event_type,order_id,size,price,direction," +
		"ask_price_1,ask_size_1,bid_price_1,bid_size_1," +
		"ask_price_2,ask_size_2,bid_price_2,bid_size_2," +
		"ask_price_3,ask_size_3,bid_price_3,bid_size_3," +
		"ask_price_4,ask_size_4,bid_price_4,bid_size_4," +
		"ask_price_5,ask_size_5,bid_price_5,bid_size_5\n"

	f.WriteString(header)
	hExport.Write([]byte(header))

	for _, e := range resp.Events {
		line := fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d",
			e.SourceSeq, e.TimestampNs, e.EventType, e.OrderId, e.Size, e.Price, e.Direction)

		if e.Book != nil {
			for lvl := 0; lvl < 5; lvl++ {
				var ap, bp int64
				var as, bs uint64
				if lvl < len(e.Book.Asks) {
					ap = e.Book.Asks[lvl].Price
					as = e.Book.Asks[lvl].Size
				}
				if lvl < len(e.Book.Bids) {
					bp = e.Book.Bids[lvl].Price
					bs = e.Book.Bids[lvl].Size
				}
				line += fmt.Sprintf(",%d,%d,%d,%d", ap, as, bp, bs)
			}
		}
		line += "\n"
		f.WriteString(line)
		hExport.Write([]byte(line))
	}
	_ = f.Close()

	fileHash := hex.EncodeToString(hExport.Sum(nil))

	manifest := ExportManifest{
		DatasetID:        *datasetID,
		ExportTime:       time.Now().UTC().Format(time.RFC3339),
		EventCount:       resp.EventCount,
		SeqStart:         resp.SeqStart,
		SeqEnd:           resp.SeqEnd,
		RollingDigest:    resp.RollingDigest,
		ExportFile:       exportFileName,
		ExportFileSHA256: fileHash,
	}

	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	manifestPath := filepath.Join(*outDir, fmt.Sprintf("%s_manifest.json", *datasetID))
	_ = os.WriteFile(manifestPath, manifestBytes, 0644)

	log.Printf("[TickRaft-Export] Export completed to %s (manifest: %s)", exportFilePath, manifestPath)
}
