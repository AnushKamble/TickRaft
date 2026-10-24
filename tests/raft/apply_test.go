package raft_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
	"tickraft/simulator"
)

type recordingStateMachine struct {
	mu            sync.Mutex
	appliedEvents []*raftpb.LogEntry
}

func (r *recordingStateMachine) Apply(entry *raftpb.LogEntry) (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appliedEvents = append(r.appliedEvents, entry)
	return "applied:" + string(entry.Data), nil
}

func TestOrderedApplyThroughCommit(t *testing.T) {
	net := simulator.NewSimNetwork(42)
	sched := simulator.NewClusterScheduler()

	sm1 := &recordingStateMachine{}
	sm2 := &recordingStateMachine{}

	cfgLeader := raft.NodeConfig{
		ID:                 "node-1",
		Peers:              []string{"node-2"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}
	cfgFollower := raft.NodeConfig{
		ID:                 "node-2",
		Peers:              []string{"node-1"},
		ElectionTimeoutMin: 150 * 1_000_000,
		ElectionTimeoutMax: 300 * 1_000_000,
		HeartbeatInterval:  50 * 1_000_000,
	}

	n1, _ := raft.NewRaftNode(cfgLeader, raft.NewMemoryStorage(), net, sm1, 1)
	n2, _ := raft.NewRaftNode(cfgFollower, raft.NewMemoryStorage(), net, sm2, 2)
	defer n1.Stop()
	defer n2.Stop()

	net.RegisterNode("node-1", n1)
	net.RegisterNode("node-2", n2)
	sched.AddNode(n1, n1.GetStatus())
	sched.AddNode(n2, n2.GetStatus())

	// Step forward to elect leader
	for i := 0; i < 40; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	leader := n1
	leaderSM := sm1
	if n2.GetStatus().Role == raft.RoleLeader {
		leader = n2
		leaderSM = sm2
	}

	// Submit 3 consecutive proposals sequentially
	for i := 1; i <= 3; i++ {
		payload := []byte("cmd-" + string(rune('0'+i)))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resChan := make(chan raft.ClientProposalResult, 1)
		go func(data []byte) {
			defer cancel()
			res, err := leader.SubmitProposal(ctx, data)
			resChan <- res
			if err != nil {
				t.Logf("proposal error: %v", err)
			}
		}(payload)

		for tStep := 0; tStep < 15; tStep++ {
			sched.StepTicks(1, 10*1_000_000)
			time.Sleep(1 * time.Millisecond)
		}
		<-resChan
	}

	leaderSM.mu.Lock()
	defer leaderSM.mu.Unlock()

	// Verify entries were applied strictly in increasing index order 1, 2, 3
	if len(leaderSM.appliedEvents) != 3 {
		t.Fatalf("expected exactly 3 applied events, got %d", len(leaderSM.appliedEvents))
	}

	for i := 0; i < 3; i++ {
		expectedIndex := uint64(i + 1)
		if leaderSM.appliedEvents[i].Index != expectedIndex {
			t.Fatalf("applied entry at position %d had index %d, expected %d", i, leaderSM.appliedEvents[i].Index, expectedIndex)
		}
	}
}

func TestClientWaitersOnApply(t *testing.T) {
	nodes, _, sched := createTestCluster(t, 3)
	defer cleanupNodes(nodes)

	// Step forward to elect leader
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

	replyChan := make(chan raft.ClientProposalResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, err := leader.SubmitProposal(ctx, []byte("waiter-test"))
		replyChan <- raft.ClientProposalResult{CommittedIndex: res.CommittedIndex, Err: err}
	}()

	// Drive scheduler forward
	for i := 0; i < 20; i++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(1 * time.Millisecond)
	}

	select {
	case res := <-replyChan:
		if res.Err != nil {
			t.Fatalf("expected proposal success, got err: %v", res.Err)
		}
		if res.CommittedIndex == 0 {
			t.Fatalf("committedIndex should be >= 1")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("client proposal waiter timed out")
	}
}
