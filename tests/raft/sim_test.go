package raft_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"tickraft/internal/raft"
	"tickraft/internal/storage/wal"
	"tickraft/simulator"
)

// TestSimCrashRestart tests crash and restart of a Raft node preserving WAL and HardState files
// in a simulated environment (fulfills SIM-CRASH-01).
func TestSimCrashRestart(t *testing.T) {
	tempBase, err := os.MkdirTemp("", "sim_crash_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempBase)

	net := simulator.NewSimNetwork(99)
	sched := simulator.NewClusterScheduler()

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	nodes := make([]*raft.RaftNode, 3)
	dirs := make([]string, 3)

	for i, id := range nodeIDs {
		dirs[i] = filepath.Join(tempBase, id)
		store, err := wal.NewDurableWALStorage(dirs[i])
		if err != nil {
			t.Fatalf("failed to create durable storage for %s: %v", id, err)
		}

		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		cfg := raft.NodeConfig{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 100 * 1_000_000,
			ElectionTimeoutMax: 200 * 1_000_000,
			HeartbeatInterval:  30 * 1_000_000,
		}

		node, err := raft.NewRaftNode(cfg, store, net, nil, int64(i*50+10))
		if err != nil {
			t.Fatalf("failed to create raft node %s: %v", id, err)
		}
		nodes[i] = node
		net.RegisterNode(id, node)
		sched.AddNode(node, node.GetStatus())
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()

	// Step scheduler until leader is elected
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leader *raft.RaftNode
	var leaderIdx int
	for i, n := range nodes {
		if n.GetStatus().Role == raft.RoleLeader {
			leader = n
			leaderIdx = i
			break
		}
	}
	if leader == nil {
		t.Fatalf("failed to elect a leader")
	}

	// Submit proposal 1 to leader
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	res1Chan := make(chan error, 1)
	go func() {
		_, err := leader.SubmitProposal(ctx1, []byte("val-1"))
		res1Chan <- err
	}()

	for i := 0; i < 20; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	if err := <-res1Chan; err != nil {
		t.Fatalf("proposal 1 failed: %v", err)
	}

	// Select a follower to crash
	crashIdx := (leaderIdx + 1) % 3
	crashID := nodeIDs[crashIdx]
	t.Logf("Simulating crash on follower %s (dir: %s)", crashID, dirs[crashIdx])

	nodes[crashIdx].Stop()
	nodes[crashIdx] = nil // mark dead

	// Now propose val-2 to the leader while follower is crashed (quorum = 2/3 succeeds)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	res2Chan := make(chan error, 1)
	go func() {
		_, err := leader.SubmitProposal(ctx2, []byte("val-2"))
		res2Chan <- err
	}()

	for i := 0; i < 20; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	if err := <-res2Chan; err != nil {
		t.Fatalf("proposal 2 on 2/3 quorum failed: %v", err)
	}

	// Now RESTART the crashed node from disk files
	restartedStore, err := wal.NewDurableWALStorage(dirs[crashIdx])
	if err != nil {
		t.Fatalf("failed to open storage for crashed node %s: %v", crashID, err)
	}

	// Verify hard state and entries recovered from disk
	recTerm, _, err := restartedStore.LoadHardState()
	if err != nil {
		t.Fatalf("failed to load recovered hard state: %v", err)
	}
	if recTerm == 0 {
		t.Fatalf("recovered term should be > 0, got %d", recTerm)
	}

	entries, err := restartedStore.LoadEntries()
	if err != nil {
		t.Fatalf("failed to load recovered entries: %v", err)
	}
	if len(entries) < 1 {
		t.Fatalf("expected at least 1 entry in recovered storage, got %d", len(entries))
	}

	// Recreate node with the recovered storage
	var peers []string
	for _, other := range nodeIDs {
		if other != crashID {
			peers = append(peers, other)
		}
	}
	cfg := raft.NodeConfig{
		ID:                 crashID,
		Peers:              peers,
		ElectionTimeoutMin: 100 * 1_000_000,
		ElectionTimeoutMax: 200 * 1_000_000,
		HeartbeatInterval:  30 * 1_000_000,
	}

	restartedNode, err := raft.NewRaftNode(cfg, restartedStore, net, nil, 777)
	if err != nil {
		t.Fatalf("failed to create restarted node: %v", err)
	}
	nodes[crashIdx] = restartedNode
	net.RegisterNode(crashID, restartedNode)
	sched.AddNode(restartedNode, restartedNode.GetStatus())

	// Step scheduler: leader should catch up the restarted node with val-2
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	leaderCommit := leader.GetStatus().CommitIndex
	restartedCommit := restartedNode.GetStatus().CommitIndex
	if restartedCommit != leaderCommit {
		t.Fatalf("restarted node commitIndex (%d) mismatch with leader (%d)",
			restartedCommit, leaderCommit)
	}

	t.Logf("Node %s successfully recovered from disk and caught up to commitIndex %d",
		crashID, restartedCommit)
}
