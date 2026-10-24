package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	transportgrpc "tickraft/internal/transport/grpc"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "status":
		statusCmd := flag.NewFlagSet("status", flag.ExitOnError)
		addrsFlag := statusCmd.String("cluster-addrs", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "Cluster addresses")
		_ = statusCmd.Parse(os.Args[2:])

		addrs := strings.Split(*addrsFlag, ",")
		client := transportgrpc.NewResilientClusterClient(addrs)
		defer client.Close()

		fmt.Println("=== TickRaft Cluster Status ===")
		for _, addr := range addrs {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			st, err := client.GetNodeStatus(ctx, addr)
			cancel()
			if err != nil {
				fmt.Printf("[%s] ERROR: %v\n", addr, err)
			} else {
				fmt.Printf("[%s] Node: %-8s Role: %-9s Term: %-3d Leader: %-8s CommitIdx: %-4d AppliedIdx: %-4d Events: %-4d Digest: %s\n",
					addr, st.NodeId, st.Role, st.Term, st.LeaderId, st.CommitIndex, st.LastApplied, st.EventCount, truncate(st.LastAppliedDigest, 12))
			}
		}

	case "help":
		printUsage()

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func printUsage() {
	fmt.Println("Usage: tickraftctl <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  status      Query health, role, term, and applied state across all cluster nodes")
	fmt.Println("  help        Print this help message")
}
