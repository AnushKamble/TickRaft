package raft_test

import (
	"testing"
	"time"

	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
	"tickraft/simulator"
)

func createTestCluster(t *testing.T, count int) ([]*raft.RaftNode, *simulator.SimNetwork, *simulator.ClusterScheduler) {
	net := simulator.NewSimNetwork(42)
	sched := simulator.NewClusterScheduler()

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	if count < 3 {
		nodeIDs = nodeIDs[:count]
	}

	nodes := make([]*raft.RaftNode, len(nodeIDs))
	for i, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		cfg := raft.NodeConfig{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 150 * 1_000_000, // 150 ms
			ElectionTimeoutMax: 300 * 1_000_000, // 300 ms
			HeartbeatInterval:  50 * 1_000_000,  // 50 ms
		}
		memStorage := raft.NewMemoryStorage()
		node, err := raft.NewRaftNode(cfg, memStorage, net, nil, int64(i*100+1))
		if err != nil {
			t.Fatalf("failed to create node: %v", err)
		}
		nodes[i] = node
		net.RegisterNode(id, node)
		sched.AddNode(node, node.GetStatus())
	}

	return nodes, net, sched
}

func cleanupNodes(nodes []*raft.RaftNode) {
	for _, n := range nodes {
		n.Stop()
	}
}

func TestElectionTimeout(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Initially all nodes are followers at term 0
	for _, n := range nodes {
		st := n.GetStatus()
		if st.Role != raft.RoleFollower || st.Term != 0 {
			t.Fatalf("expected follower at term 0, got %s term %d", st.Role, st.Term)
		}
	}

	// Step scheduler forward past election timeout (e.g. 400ms in 10ms steps)
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond) // yield goroutine
	}

	// Verify exactly one leader is elected with term >= 1
	leaderCount := 0
	var leaderTerm uint64 = 0
	for _, n := range nodes {
		st := n.GetStatus()
		if st.Role == raft.RoleLeader {
			leaderCount++
			leaderTerm = st.Term
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
	if leaderTerm < 1 {
		t.Fatalf("expected leader term >= 1, got %d", leaderTerm)
	}
}

func TestRequestVoteRules(t *testing.T) {
	memStorage := raft.NewMemoryStorage()
	cfg := raft.NodeConfig{
		ID:                 "node-1",
		Peers:              []string{"node-2", "node-3"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	node, err := raft.NewRaftNode(cfg, memStorage, nil, nil, 42)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	// Initial term is 0. RequestVote with term 1 should grant vote.
	req := &raftpb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := node.HandleRequestVote(req)
	if !resp.VoteGranted || resp.Term != 1 {
		t.Fatalf("expected vote granted at term 1, got granted=%v, term=%d", resp.VoteGranted, resp.Term)
	}

	// Same term, different candidate should be REJECTED (at most 1 vote per term)
	req2 := &raftpb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp2 := node.HandleRequestVote(req2)
	if resp2.VoteGranted {
		t.Fatalf("expected vote rejected for second candidate in same term")
	}

	// Stale term (term 0 < current term 1) should be REJECTED
	reqStale := &raftpb.RequestVoteRequest{
		Term:         0,
		CandidateId:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	respStale := node.HandleRequestVote(reqStale)
	if respStale.VoteGranted || respStale.Term != 1 {
		t.Fatalf("expected stale term rejected, got granted=%v, term=%d", respStale.VoteGranted, respStale.Term)
	}
}

func TestCandidateLogUpToDate(t *testing.T) {
	memStorage := raft.NewMemoryStorage()
	// Populate local storage with entries up to index 3, term 2
	_ = memStorage.SaveHardState(2, "")
	_ = memStorage.AppendEntries([]*raftpb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})

	cfg := raft.NodeConfig{
		ID:                 "node-1",
		Peers:              []string{"node-2"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	node, err := raft.NewRaftNode(cfg, memStorage, nil, nil, 42)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	// Candidate with lower last-log-term (term 1 vs our term 2) should be rejected
	reqLowerTerm := &raftpb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-2",
		LastLogIndex: 10,
		LastLogTerm:  1,
	}
	resp := node.HandleRequestVote(reqLowerTerm)
	if resp.VoteGranted {
		t.Fatalf("candidate with lower last log term should be rejected")
	}

	// Candidate with same term (2) but shorter log (index 2 < our index 3) should be rejected
	reqShorterLog := &raftpb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-2",
		LastLogIndex: 2,
		LastLogTerm:  2,
	}
	resp2 := node.HandleRequestVote(reqShorterLog)
	if resp2.VoteGranted {
		t.Fatalf("candidate with shorter log should be rejected")
	}

	// Candidate with higher last-log-term (term 3 > our term 2) should be accepted
	reqHigherTerm := &raftpb.RequestVoteRequest{
		Term:         4,
		CandidateId:  "node-2",
		LastLogIndex: 1,
		LastLogTerm:  3,
	}
	resp3 := node.HandleRequestVote(reqHigherTerm)
	if !resp3.VoteGranted {
		t.Fatalf("candidate with higher last log term should be accepted")
	}
}

func TestHigherTermStepDown(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Step forward to elect a leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leaderNode *raft.RaftNode
	for _, n := range nodes {
		if n.GetStatus().Role == raft.RoleLeader {
			leaderNode = n
			break
		}
	}
	if leaderNode == nil {
		t.Fatalf("no leader elected")
	}

	leaderTerm := leaderNode.GetStatus().Term

	// Send an RPC with a higher term (leaderTerm + 5)
	resp := leaderNode.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         leaderTerm + 5,
		LeaderId:     "node-external",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if resp.Term != leaderTerm+5 {
		t.Fatalf("expected stepped down term %d, got %d", leaderTerm+5, resp.Term)
	}

	// Verify leader stepped down to follower
	st := leaderNode.GetStatus()
	if st.Role != raft.RoleFollower || st.Term != leaderTerm+5 {
		t.Fatalf("expected follower with term %d, got %s term %d", leaderTerm+5, st.Role, st.Term)
	}
}
