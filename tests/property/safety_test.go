package property_test

import (
	"sync"
	"testing"
	"time"

	"tickraft/internal/raft"
	"tickraft/simulator"
)

func TestElectionSafety(t *testing.T) {
	// Raft Election Safety Property: At most one leader can be elected in a given term.
	for seed := int64(1); seed <= 5; seed++ {
		net := simulator.NewSimNetwork(seed)
		sched := simulator.NewClusterScheduler()

		nodeIDs := []string{"node-1", "node-2", "node-3"}
		nodes := make([]*raft.RaftNode, 3)

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
				ElectionTimeoutMin: 150 * 1_000_000,
				ElectionTimeoutMax: 300 * 1_000_000,
				HeartbeatInterval:  50 * 1_000_000,
			}
			n, _ := raft.NewRaftNode(cfg, raft.NewMemoryStorage(), net, nil, seed*100+int64(i))
			nodes[i] = n
			net.RegisterNode(id, n)
			sched.AddNode(n, n.GetStatus())
		}

		// Run 100 ticks and record every (term, leader) occurrence
		leadersByTerm := make(map[uint64]string)
		var mu sync.Mutex

		for tick := 0; tick < 50; tick++ {
			sched.StepTicks(1, 10*1_000_000)
			time.Sleep(500 * time.Microsecond)

			mu.Lock()
			for _, n := range nodes {
				st := n.GetStatus()
				if st.Role == raft.RoleLeader {
					if existing, found := leadersByTerm[st.Term]; found {
						if existing != st.ID {
							t.Fatalf("Election Safety VIOLATION at seed %d, term %d: two leaders (%s and %s)",
								seed, st.Term, existing, st.ID)
						}
					} else {
						leadersByTerm[st.Term] = st.ID
					}
				}
			}
			mu.Unlock()
		}

		for _, n := range nodes {
			n.Stop()
		}
	}
}

func TestNeverApplyUncommitted(t *testing.T) {
	// State Machine Safety Property: lastApplied <= commitIndex at all times.
	net := simulator.NewSimNetwork(99)
	sched := simulator.NewClusterScheduler()

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	nodes := make([]*raft.RaftNode, 3)

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
			ElectionTimeoutMin: 150 * 1_000_000,
			ElectionTimeoutMax: 300 * 1_000_000,
			HeartbeatInterval:  50 * 1_000_000,
		}
		n, _ := raft.NewRaftNode(cfg, raft.NewMemoryStorage(), net, nil, 99*100+int64(i))
		nodes[i] = n
		net.RegisterNode(id, n)
		sched.AddNode(n, n.GetStatus())
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	for tick := 0; tick < 60; tick++ {
		sched.StepTicks(1, 10*1_000_000)
		time.Sleep(500 * time.Microsecond)

		for _, n := range nodes {
			st := n.GetStatus()
			if st.LastApplied > st.CommitIndex {
				t.Fatalf("INVARIANT VIOLATION: node %s has lastApplied=%d > commitIndex=%d",
					st.ID, st.LastApplied, st.CommitIndex)
			}
		}
	}
}
