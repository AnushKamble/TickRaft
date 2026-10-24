package raft

import (
	"context"
	"errors"
	raftpb "tickraft/proto/raft"
)

var (
	ErrNotLeader       = errors.New("node is not leader")
	ErrStopped         = errors.New("node is stopped")
	ErrLogConflict     = errors.New("log entry conflict")
	ErrStaleTerm       = errors.New("stale term")
	ErrProposalDropped = errors.New("proposal dropped due to leadership loss")
)

type Role string

const (
	RoleFollower  Role = "FOLLOWER"
	RoleCandidate Role = "CANDIDATE"
	RoleLeader    Role = "LEADER"
)

// Storage defines the durability interface for Raft persistent hard-state and log entries.
type Storage interface {
	SaveHardState(term uint64, votedFor string) error
	LoadHardState() (term uint64, votedFor string, err error)
	AppendEntries(entries []*raftpb.LogEntry) error
	TruncateSuffix(fromIndex uint64) error
	LoadEntries() ([]*raftpb.LogEntry, error)
	Close() error
}

// Transport abstracts message passing between Raft nodes (in-memory simulator or gRPC).
type Transport interface {
	SendRequestVote(ctx context.Context, to string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, to string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
}

// StateMachine abstracts the replicated state machine (e.g., MarketLog).
type StateMachine interface {
	Apply(entry *raftpb.LogEntry) (interface{}, error)
}

// ClientProposal represents a client command submitted to the cluster leader.
type ClientProposal struct {
	Data      []byte
	ReplyChan chan ClientProposalResult
}

type ClientProposalResult struct {
	CommittedIndex uint64
	Term           uint64
	Response       interface{}
	Err            error
}

// Clock provides monotonic time abstraction for deterministic testing and production wall clock.
type Clock interface {
	NowNano() int64
}

// RealClock uses standard system time.
type RealClock struct{}

func (RealClock) NowNano() int64 {
	return 0 // or time.Now().UnixNano()
}

// NodeConfig holds static cluster configuration for a Raft node.
type NodeConfig struct {
	ID                  string
	Peers               []string
	ElectionTimeoutMin  int64 // Nanoseconds
	ElectionTimeoutMax  int64 // Nanoseconds
	HeartbeatInterval   int64 // Nanoseconds
}
