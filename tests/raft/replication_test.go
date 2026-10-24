package raft_test

import (
	"context"
	"testing"
	"time"

	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
)

func TestHeartbeats(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Step to elect leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leader *raft.RaftNode
	var followers []*raft.RaftNode
	for _, n := range nodes {
		if n.GetStatus().Role == raft.RoleLeader {
			leader = n
		} else {
			followers = append(followers, n)
		}
	}
	if leader == nil {
		t.Fatalf("expected a leader")
	}

	leaderTerm := leader.GetStatus().Term

	// Heartbeats from leader keep followers from starting new elections
	for i := 0; i < 50; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	// All followers should still be followers in the same term
	for _, f := range followers {
		st := f.GetStatus()
		if st.Role != raft.RoleFollower || st.Term != leaderTerm {
			t.Fatalf("follower changed state: %s at term %d (expected follower at term %d)", st.Role, st.Term, leaderTerm)
		}
	}
}

func TestLogMatching(t *testing.T) {
	memStorage := raft.NewMemoryStorage()
	_ = memStorage.SaveHardState(1, "")
	_ = memStorage.AppendEntries([]*raftpb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	cfg := raft.NodeConfig{
		ID:                 "follower-1",
		Peers:              []string{"leader-1"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	follower, err := raft.NewRaftNode(cfg, memStorage, nil, nil, 42)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer follower.Stop()

	// 1. PrevLogIndex beyond follower's log: reject
	resp := follower.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader-1",
		PrevLogIndex: 10,
		PrevLogTerm:  1,
	})
	if resp.Success {
		t.Fatalf("expected reject for missing prev log index")
	}

	// 2. PrevLogTerm mismatch at index 2: reject
	resp2 := follower.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  2, // our entry at index 2 has Term 1!
	})
	if resp2.Success {
		t.Fatalf("expected reject for prev log term mismatch")
	}

	// 3. PrevLogIndex and PrevLogTerm matching: accept and append
	resp3 := follower.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []*raftpb.LogEntry{
			{Index: 3, Term: 2, Data: []byte("entry-3")},
		},
	})
	if !resp3.Success {
		t.Fatalf("expected success when prev log index and term match")
	}

	st := follower.GetStatus()
	if st.LastLogIndex != 3 || st.LastLogTerm != 2 {
		t.Fatalf("expected log length 3 at term 2, got idx=%d, term=%d", st.LastLogIndex, st.LastLogTerm)
	}
}

func TestConflictRepairUncommittedOnly(t *testing.T) {
	memStorage := raft.NewMemoryStorage()
	_ = memStorage.SaveHardState(3, "")
	// Log has: index 1 (term 1), index 2 (term 1), index 3 (term 2)
	_ = memStorage.AppendEntries([]*raftpb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2}, // uncommitted conflicting entry
	})

	cfg := raft.NodeConfig{
		ID:                 "follower-1",
		Peers:              []string{"leader-1"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	follower, err := raft.NewRaftNode(cfg, memStorage, nil, nil, 42)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer follower.Stop()

	// Leader at term 3 sends entry at index 3 with term 3 (repairing the conflict)
	resp := follower.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []*raftpb.LogEntry{
			{Index: 3, Term: 3, Data: []byte("repaired-entry-3")},
			{Index: 4, Term: 3, Data: []byte("new-entry-4")},
		},
		LeaderCommit: 3,
	})

	if !resp.Success {
		t.Fatalf("expected conflict repair to succeed")
	}

	st := follower.GetStatus()
	if st.LastLogIndex != 4 || st.LastLogTerm != 3 {
		t.Fatalf("expected repaired log index 4 term 3, got idx=%d term=%d", st.LastLogIndex, st.LastLogTerm)
	}
	if st.CommitIndex != 3 {
		t.Fatalf("expected commit index advanced to 3, got %d", st.CommitIndex)
	}
}

func TestLeaderIndexTracking(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Elect leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leader *raft.RaftNode
	for _, n := range nodes {
		if n.GetStatus().Role == raft.RoleLeader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("expected leader")
	}

	// Submit proposal
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = leader.SubmitProposal(ctx, []byte("test-tracking-data"))
	}()

	// Drive ticks for replication
	for i := 0; i < 20; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	// All nodes should now have commitIndex >= 1
	for _, n := range nodes {
		st := n.GetStatus()
		if st.CommitIndex < 1 {
			t.Fatalf("node %s has commitIndex %d (expected >= 1)", st.ID, st.CommitIndex)
		}
	}
}

func TestIgnoreStaleResponses(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Elect leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leader *raft.RaftNode
	for _, n := range nodes {
		if n.GetStatus().Role == raft.RoleLeader {
			leader = n
			break
		}
	}

	// Force higher term step down
	leader.HandleRequestVote(&raftpb.RequestVoteRequest{
		Term:         leader.GetStatus().Term + 10,
		CandidateId:  "external-node",
		LastLogIndex: 10,
		LastLogTerm:  leader.GetStatus().Term + 10,
	})

	if leader.GetStatus().Role != raft.RoleFollower {
		t.Fatalf("expected node to step down to follower")
	}
}
