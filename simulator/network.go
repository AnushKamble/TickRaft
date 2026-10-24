package simulator

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
)

var (
	ErrNetworkPartition = errors.New("network partition: destination unreachable")
	ErrPacketDropped    = errors.New("network packet dropped by simulator")
)

type SimNetwork struct {
	mu sync.RWMutex

	nodes map[string]*raft.RaftNode

	// blocked[from][to] == true means communication is blocked
	blocked map[string]map[string]bool

	dropRate float64
	rng      *rand.Rand
}

func NewSimNetwork(seed int64) *SimNetwork {
	return &SimNetwork{
		nodes:    make(map[string]*raft.RaftNode),
		blocked:  make(map[string]map[string]bool),
		dropRate: 0.0,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (net *SimNetwork) RegisterNode(id string, node *raft.RaftNode) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
	if _, ok := net.blocked[id]; !ok {
		net.blocked[id] = make(map[string]bool)
	}
}

func (net *SimNetwork) SetDropRate(rate float64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.dropRate = rate
}

func (net *SimNetwork) Partition(group1 []string, group2 []string) {
	net.mu.Lock()
	defer net.mu.Unlock()

	for _, n1 := range group1 {
		if _, ok := net.blocked[n1]; !ok {
			net.blocked[n1] = make(map[string]bool)
		}
		for _, n2 := range group2 {
			net.blocked[n1][n2] = true
			if _, ok := net.blocked[n2]; !ok {
				net.blocked[n2] = make(map[string]bool)
			}
			net.blocked[n2][n1] = true
		}
	}
}

func (net *SimNetwork) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	for k := range net.blocked {
		net.blocked[k] = make(map[string]bool)
	}
}

func (net *SimNetwork) IsBlocked(from, to string) bool {
	net.mu.RLock()
	defer net.mu.RUnlock()

	if net.dropRate > 0 && net.rng.Float64() < net.dropRate {
		return true
	}

	if b, ok := net.blocked[from]; ok {
		if b[to] {
			return true
		}
	}
	return false
}

func (net *SimNetwork) SendRequestVote(
	ctx context.Context,
	to string,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	if net.IsBlocked(req.CandidateId, to) {
		return nil, ErrNetworkPartition
	}

	net.mu.RLock()
	targetNode, exists := net.nodes[to]
	net.mu.RUnlock()

	if !exists {
		return nil, errors.New("node not found")
	}

	resp := targetNode.HandleRequestVote(req)
	if net.IsBlocked(to, req.CandidateId) {
		return nil, ErrNetworkPartition
	}

	return resp, nil
}

func (net *SimNetwork) SendAppendEntries(
	ctx context.Context,
	to string,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	if net.IsBlocked(req.LeaderId, to) {
		return nil, ErrNetworkPartition
	}

	net.mu.RLock()
	targetNode, exists := net.nodes[to]
	net.mu.RUnlock()

	if !exists {
		return nil, errors.New("node not found")
	}

	resp := targetNode.HandleAppendEntries(req)
	if net.IsBlocked(to, req.LeaderId) {
		return nil, ErrNetworkPartition
	}

	return resp, nil
}
