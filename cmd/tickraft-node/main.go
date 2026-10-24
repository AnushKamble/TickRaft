package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"tickraft/internal/marketlog"
	"tickraft/internal/raft"
	"tickraft/internal/storage/wal"
	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

func main() {
	id := flag.String("id", "node-1", "Node identifier")
	listenAddr := flag.String("listen", ":50051", "Listen address for gRPC")
	peersFlag := flag.String("peers", "", "Comma-separated peer mappings: peerID=host:port")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for durable WAL and hard-state")
	flag.Parse()

	log.Printf("[TickRaft] Starting node %s on %s (data-dir: %s)", *id, *listenAddr, *dataDir)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	storage, err := wal.NewDurableWALStorage(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	sm := marketlog.NewMarketLogStateMachine()

	peerAddrs := make(map[string]string)
	var peerIDs []string
	if *peersFlag != "" {
		pairs := strings.Split(*peersFlag, ",")
		for _, pair := range pairs {
			parts := strings.Split(strings.TrimSpace(pair), "=")
			if len(parts) == 2 {
				peerIDs = append(peerIDs, parts[0])
				peerAddrs[parts[0]] = parts[1]
			}
		}
	}

	grpcTrans := transportgrpc.NewGRPCTransport(peerAddrs)
	defer grpcTrans.Close()

	cfg := raft.NodeConfig{
		ID:                 *id,
		Peers:              peerIDs,
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}

	node, err := raft.NewRaftNode(cfg, storage, grpcTrans, sm, time.Now().UnixNano())
	if err != nil {
		log.Fatalf("Failed to initialize Raft node: %v", err)
	}
	defer node.Stop()

	grpcTrans.SetNode(node)

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to bind tcp port %s: %v", *listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, grpcTrans)
	marketlogpb.RegisterMarketLogServiceServer(grpcServer, transportgrpc.NewMarketLogServiceServer(node, sm))

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server terminated: %v", err)
		}
	}()

	// Wall-clock ticker driving Raft ticks
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			node.Tick(10 * 1_000_000)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("[TickRaft] Shutting down node %s...", *id)
	grpcServer.GracefulStop()
	fmt.Println("Node shutdown complete.")
}
