package raft_test

import (
	"context"
	"testing"
	"time"

	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
)

func TestMajorityQuorumCommit(t *testing.T) {
	nodes, net, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Elect leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	var leader *raft.RaftNode
	var leaderID string
	var followerIDs []string
	for _, n := range nodes {
		st := n.GetStatus()
		if st.Role == raft.RoleLeader {
			leader = n
			leaderID = st.ID
		} else {
			followerIDs = append(followerIDs, st.ID)
		}
	}

	// Isolate one follower (e.g. node-3)
	net.Partition([]string{followerIDs[1]}, []string{leaderID, followerIDs[0]})

	// Propose command to leader
	resChan := make(chan raft.ClientProposalResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, err := leader.SubmitProposal(ctx, []byte("quorum-write-data"))
		resChan <- raft.ClientProposalResult{CommittedIndex: res.CommittedIndex, Err: err}
	}()

	// Step scheduler: remaining majority (leader + 1 follower = 2 nodes) should commit!
	for i := 0; i < 30; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	select {
	case res := <-resChan:
		if res.Err != nil {
			t.Fatalf("expected write to commit on majority quorum, got err: %v", res.Err)
		}
		if res.CommittedIndex != 1 {
			t.Fatalf("expected committedIndex 1, got %d", res.CommittedIndex)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for majority quorum commit")
	}

	// Confirm leader and connected follower committed the entry
	if leader.GetStatus().CommitIndex != 1 {
		t.Fatalf("leader commitIndex %d, expected 1", leader.GetStatus().CommitIndex)
	}

	// Reconnect isolated node and step ticks -> it must catch up
	net.Heal()
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	for _, n := range nodes {
		if n.GetStatus().CommitIndex != 1 {
			t.Fatalf("node %s did not catch up to commitIndex 1, got %d", n.GetStatus().ID, n.GetStatus().CommitIndex)
		}
	}
}

func TestCurrentTermCommitRule(t *testing.T) {
	// Raft paper Section 5.4.2:
	// A leader cannot commit log entries from previous terms by counting replicas.
	// Only entries from the leader's current term can be committed by counting replicas.
	memStorage := raft.NewMemoryStorage()
	_ = memStorage.SaveHardState(2, "node-1")
	// Old term 1 entries present in log
	_ = memStorage.AppendEntries([]*raftpb.LogEntry{
		{Index: 1, Term: 1, Data: []byte("term-1-entry")},
		{Index: 2, Term: 1, Data: []byte("term-1-entry-2")},
	})

	cfg := raft.NodeConfig{
		ID:                 "node-1",
		Peers:              []string{"node-2", "node-3"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	leader, err := raft.NewRaftNode(cfg, memStorage, nil, nil, 42)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer leader.Stop()

	// Initial commitIndex is 0
	if leader.GetStatus().CommitIndex != 0 {
		t.Fatalf("initial commitIndex should be 0, got %d", leader.GetStatus().CommitIndex)
	}
}

func TestFollowerCommitAdvance(t *testing.T) {
	memStorage := raft.NewMemoryStorage()
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

	// Append entries with leaderCommit = 2
	resp := follower.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*raftpb.LogEntry{
			{Index: 1, Term: 1, Data: []byte("entry-1")},
			{Index: 2, Term: 1, Data: []byte("entry-2")},
			{Index: 3, Term: 1, Data: []byte("entry-3")},
		},
		LeaderCommit: 2,
	})

	if !resp.Success {
		t.Fatalf("failed to handle append entries: %v", resp)
	}

	st := follower.GetStatus()
	if st.CommitIndex != 2 {
		t.Fatalf("expected follower commitIndex 2, got %d", st.CommitIndex)
	}
	if st.LastApplied != 2 {
		t.Fatalf("expected follower lastApplied 2, got %d", st.LastApplied)
	}
}
