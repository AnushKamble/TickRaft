package simulator

import (
	"sync"
	"tickraft/internal/raft"
)

type ClusterScheduler struct {
	mu          sync.Mutex
	nodes       map[string]*raft.RaftNode
	currentNano int64
}

func NewClusterScheduler() *ClusterScheduler {
	return &ClusterScheduler{
		nodes:       make(map[string]*raft.RaftNode),
		currentNano: 0,
	}
}

func (s *ClusterScheduler) AddNode(node *raft.RaftNode, status raft.NodeStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[status.ID] = node
}

func (s *ClusterScheduler) StepTicks(ticks int, dtNano int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for t := 0; t < ticks; t++ {
		s.currentNano += dtNano
		for _, node := range s.nodes {
			node.Tick(dtNano)
		}
	}
}

func (s *ClusterScheduler) CurrentTimeNano() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentNano
}
