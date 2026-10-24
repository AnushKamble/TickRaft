package raft

import (
	"context"
	"math/rand"
	"sync"
	raftpb "tickraft/proto/raft"
)

type RaftNode struct {
	mu sync.RWMutex

	// Node identification and peers
	id    string
	peers []string

	// Role and leadership
	role          Role
	currentLeader string

	// Persistent state on all servers (must persist before responding to RPCs)
	currentTerm uint64
	votedFor    string
	log         []*raftpb.LogEntry // 1-indexed, log[0] is sentinel entry

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Election and heartbeat timers
	electionTimeoutMin int64
	electionTimeoutMax int64
	heartbeatInterval  int64
	electionTimeout    int64
	electionElapsed    int64
	heartbeatElapsed   int64

	// Votes gathered in candidate state
	votesReceived map[string]bool

	// Client proposal waiters: index -> reply channel
	waiters map[uint64]chan ClientProposalResult

	// Subsystems
	storage      Storage
	transport    Transport
	stateMachine StateMachine

	// Random source for randomized timeouts
	rng *rand.Rand

	// Channel for serializing all state mutations via actor loop
	msgChan chan actorMsg
	stopChan chan struct{}
	stopped  bool
}

type actorMsg interface{}

type tickMsg struct {
	dt int64
}

type clientProposalMsg struct {
	data      []byte
	replyChan chan ClientProposalResult
}

type requestVoteRPCMsg struct {
	req   *raftpb.RequestVoteRequest
	reply chan *raftpb.RequestVoteResponse
}

type requestVoteRespMsg struct {
	from string
	resp *raftpb.RequestVoteResponse
}

type appendEntriesRPCMsg struct {
	req   *raftpb.AppendEntriesRequest
	reply chan *raftpb.AppendEntriesResponse
}

type appendEntriesRespMsg struct {
	from string
	req  *raftpb.AppendEntriesRequest
	resp *raftpb.AppendEntriesResponse
}

type statusQueryMsg struct {
	reply chan NodeStatus
}

type NodeStatus struct {
	ID            string
	Role          Role
	Term          uint64
	Leader        string
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	LogLength     int
}

func NewRaftNode(
	cfg NodeConfig,
	storage Storage,
	transport Transport,
	sm StateMachine,
	seed int64,
) (*RaftNode, error) {
	rng := rand.New(rand.NewSource(seed))

	// Initial log with index 0 sentinel
	initialLog := []*raftpb.LogEntry{
		{Index: 0, Term: 0, EntryType: 0},
	}

	var term uint64 = 0
	var votedFor string = ""

	if storage != nil {
		persistedTerm, persistedVote, err := storage.LoadHardState()
		if err == nil {
			term = persistedTerm
			votedFor = persistedVote
		}
		persistedEntries, err := storage.LoadEntries()
		if err == nil && len(persistedEntries) > 0 {
			initialLog = append(initialLog, persistedEntries...)
		}
	}

	n := &RaftNode{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		role:               RoleFollower,
		currentLeader:      "",
		currentTerm:        term,
		votedFor:           votedFor,
		log:                initialLog,
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		votesReceived:      make(map[string]bool),
		waiters:            make(map[uint64]chan ClientProposalResult),
		storage:            storage,
		transport:          transport,
		stateMachine:       sm,
		rng:                rng,
		msgChan:            make(chan actorMsg, 1024),
		stopChan:           make(chan struct{}),
	}

	n.resetElectionTimeout()
	go n.runActorLoop()

	return n, nil
}

func (n *RaftNode) resetElectionTimeout() {
	delta := n.electionTimeoutMax - n.electionTimeoutMin
	if delta <= 0 {
		n.electionTimeout = n.electionTimeoutMin
	} else {
		n.electionTimeout = n.electionTimeoutMin + n.rng.Int63n(delta)
	}
	n.electionElapsed = 0
}

func (n *RaftNode) runActorLoop() {
	for {
		select {
		case <-n.stopChan:
			n.cleanupOnStop()
			return
		case msg := <-n.msgChan:
			n.handleActorMessage(msg)
		}
	}
}

func (n *RaftNode) handleActorMessage(msg actorMsg) {
	switch m := msg.(type) {
	case tickMsg:
		n.onTick(m.dt)
	case clientProposalMsg:
		n.onClientProposal(m.data, m.replyChan)
	case requestVoteRPCMsg:
		resp := n.onRequestVote(m.req)
		m.reply <- resp
	case requestVoteRespMsg:
		n.onRequestVoteResponse(m.from, m.resp)
	case appendEntriesRPCMsg:
		resp := n.onAppendEntries(m.req)
		m.reply <- resp
	case appendEntriesRespMsg:
		n.onAppendEntriesResponse(m.from, m.req, m.resp)
	case statusQueryMsg:
		m.reply <- n.getStatus()
	}
}

func (n *RaftNode) cleanupOnStop() {
	// Drop all waiting client proposals
	for idx, ch := range n.waiters {
		ch <- ClientProposalResult{
			CommittedIndex: idx,
			Err:            ErrStopped,
		}
		close(ch)
	}
	n.waiters = make(map[uint64]chan ClientProposalResult)
}

func (n *RaftNode) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopChan)
	n.mu.Unlock()
}

func (n *RaftNode) Tick(dt int64) {
	n.mu.RLock()
	if n.stopped {
		n.mu.RUnlock()
		return
	}
	n.mu.RUnlock()
	n.msgChan <- tickMsg{dt: dt}
}

func (n *RaftNode) SubmitProposal(ctx context.Context, data []byte) (ClientProposalResult, error) {
	n.mu.RLock()
	if n.stopped {
		n.mu.RUnlock()
		return ClientProposalResult{}, ErrStopped
	}
	n.mu.RUnlock()

	replyChan := make(chan ClientProposalResult, 1)
	select {
	case n.msgChan <- clientProposalMsg{data: data, replyChan: replyChan}:
	case <-ctx.Done():
		return ClientProposalResult{}, ctx.Err()
	}

	select {
	case res := <-replyChan:
		return res, res.Err
	case <-ctx.Done():
		return ClientProposalResult{}, ctx.Err()
	}
}

func (n *RaftNode) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.RLock()
	if n.stopped {
		term := n.currentTerm
		n.mu.RUnlock()
		return &raftpb.RequestVoteResponse{
			Term:        term,
			VoteGranted: false,
		}
	}
	n.mu.RUnlock()

	reply := make(chan *raftpb.RequestVoteResponse, 1)
	select {
	case n.msgChan <- requestVoteRPCMsg{req: req, reply: reply}:
		return <-reply
	case <-n.stopChan:
		return &raftpb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}
}

func (n *RaftNode) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	n.mu.RLock()
	if n.stopped {
		term := n.currentTerm
		n.mu.RUnlock()
		return &raftpb.AppendEntriesResponse{
			Term:    term,
			Success: false,
		}
	}
	n.mu.RUnlock()

	reply := make(chan *raftpb.AppendEntriesResponse, 1)
	select {
	case n.msgChan <- appendEntriesRPCMsg{req: req, reply: reply}:
		return <-reply
	case <-n.stopChan:
		return &raftpb.AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}
}

func (n *RaftNode) GetStatus() NodeStatus {
	n.mu.RLock()
	if n.stopped {
		status := n.getStatus()
		n.mu.RUnlock()
		return status
	}
	n.mu.RUnlock()

	reply := make(chan NodeStatus, 1)
	select {
	case n.msgChan <- statusQueryMsg{reply: reply}:
		return <-reply
	case <-n.stopChan:
		n.mu.RLock()
		status := n.getStatus()
		n.mu.RUnlock()
		return status
	}
}

func (n *RaftNode) getStatus() NodeStatus {
	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	return NodeStatus{
		ID:           n.id,
		Role:         n.role,
		Term:         n.currentTerm,
		Leader:       n.currentLeader,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
		LogLength:    len(n.log) - 1,
	}
}

func (n *RaftNode) lastLogIndexAndTerm() (uint64, uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// stepDown transitions node to Follower for a higher term and clears vote.
func (n *RaftNode) stepDown(term uint64) {
	n.role = RoleFollower
	n.currentTerm = term
	n.votedFor = ""
	n.currentLeader = ""
	n.votesReceived = make(map[string]bool)
	n.resetElectionTimeout()

	// Persist hard-state before answering any further RPCs
	if n.storage != nil {
		_ = n.storage.SaveHardState(n.currentTerm, n.votedFor)
	}

	// Any in-flight client proposals that this node proposed as leader can no longer be committed by it
	for idx, ch := range n.waiters {
		ch <- ClientProposalResult{
			CommittedIndex: idx,
			Err:            ErrProposalDropped,
		}
		close(ch)
	}
	n.waiters = make(map[uint64]chan ClientProposalResult)
}
