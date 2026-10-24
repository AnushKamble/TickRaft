package raft

import (
	"context"
	raftpb "tickraft/proto/raft"
)

func (n *RaftNode) onTick(dt int64) {
	if n.role == RoleLeader {
		n.heartbeatElapsed += dt
		if n.heartbeatElapsed >= n.heartbeatInterval {
			n.heartbeatElapsed = 0
			n.broadcastAppendEntries()
		}
	} else {
		n.electionElapsed += dt
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			n.startElection()
		}
	}
}

func (n *RaftNode) startElection() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.currentLeader = ""
	n.votesReceived = make(map[string]bool)
	n.votesReceived[n.id] = true
	n.resetElectionTimeout()

	// Raft rule: persist term and vote before proceeding
	if n.storage != nil {
		_ = n.storage.SaveHardState(n.currentTerm, n.votedFor)
	}

	totalNodes := len(n.peers) + 1
	majority := (totalNodes / 2) + 1

	// In single-node edge-case, candidate wins immediately
	if len(n.votesReceived) >= majority {
		n.becomeLeader()
		return
	}

	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	req := &raftpb.RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateId:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	for _, peer := range n.peers {
		peerID := peer
		go func() {
			if n.transport == nil {
				return
			}
			resp, err := n.transport.SendRequestVote(context.Background(), peerID, req)
			if err == nil && resp != nil {
				n.msgChan <- requestVoteRespMsg{from: peerID, resp: resp}
			}
		}()
	}
}

func (n *RaftNode) onRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	// Rule 1: Reject stale term
	if req.Term < n.currentTerm {
		return &raftpb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 2: If term > currentTerm, update term and step down to follower
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	// Rule 3: Candidate log up-to-date check per Raft paper section 5.4.1:
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	logOk := false
	if req.LastLogTerm > lastTerm {
		logOk = true
	} else if req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx {
		logOk = true
	}

	canVote := (n.votedFor == "" || n.votedFor == req.CandidateId)
	voteGranted := false

	if canVote && logOk {
		voteGranted = true
		n.votedFor = req.CandidateId
		n.resetElectionTimeout()

		// Persist hard-state before returning vote granted
		if n.storage != nil {
			_ = n.storage.SaveHardState(n.currentTerm, n.votedFor)
		}
	}

	return &raftpb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}
}

func (n *RaftNode) onRequestVoteResponse(from string, resp *raftpb.RequestVoteResponse) {
	// If response has higher term, step down immediately
	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}

	// Ignore stale response if no longer candidate or term mismatch
	if n.role != RoleCandidate || resp.Term != n.currentTerm {
		return
	}

	if resp.VoteGranted {
		n.votesReceived[from] = true
		totalNodes := len(n.peers) + 1
		majority := (totalNodes / 2) + 1

		if len(n.votesReceived) >= majority {
			n.becomeLeader()
		}
	}
}

func (n *RaftNode) becomeLeader() {
	n.role = RoleLeader
	n.currentLeader = n.id
	n.votesReceived = make(map[string]bool)

	lastIdx, _ := n.lastLogIndexAndTerm()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	// Reset heartbeat timer and broadcast immediate heartbeat
	n.heartbeatElapsed = 0
	n.broadcastAppendEntries()
}
