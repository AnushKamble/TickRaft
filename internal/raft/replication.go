package raft

import (
	"context"
	raftpb "tickraft/proto/raft"
)

func (n *RaftNode) broadcastAppendEntries() {
	for _, peer := range n.peers {
		peerID := peer
		req := n.makeAppendEntriesRequest(peerID)
		go func(p string, r *raftpb.AppendEntriesRequest) {
			if n.transport == nil {
				return
			}
			resp, err := n.transport.SendAppendEntries(context.Background(), p, r)
			if err == nil && resp != nil {
				n.msgChan <- appendEntriesRespMsg{from: p, req: r, resp: resp}
			}
		}(peerID, req)
	}
}

func (n *RaftNode) makeAppendEntriesRequest(peer string) *raftpb.AppendEntriesRequest {
	nxt, ok := n.nextIndex[peer]
	if !ok || nxt == 0 {
		nxt = 1
		n.nextIndex[peer] = nxt
	}

	prevLogIndex := nxt - 1
	var prevLogTerm uint64 = 0
	if prevLogIndex < uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	var entries []*raftpb.LogEntry
	if nxt < uint64(len(n.log)) {
		entries = make([]*raftpb.LogEntry, len(n.log)-int(nxt))
		copy(entries, n.log[nxt:])
	}

	return &raftpb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

func (n *RaftNode) onAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	// Rule 1: Reject stale term
	if req.Term < n.currentTerm {
		return &raftpb.AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}

	// Rule 2: If term > currentTerm, update term and step down
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	} else if n.role == RoleCandidate {
		// Valid leader for current term converts candidate to follower
		n.role = RoleFollower
	}

	n.currentLeader = req.LeaderId
	n.resetElectionTimeout()

	// Rule 3: Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
	lastLogIdx, _ := n.lastLogIndexAndTerm()
	if req.PrevLogIndex > lastLogIdx {
		return &raftpb.AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: lastLogIdx + 1,
			ConflictTerm:  0,
		}
	}

	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		conflictTerm := n.log[req.PrevLogIndex].Term
		var conflictIndex uint64 = req.PrevLogIndex
		// Find first index for conflictTerm
		for i := req.PrevLogIndex; i >= 1; i-- {
			if n.log[i].Term == conflictTerm {
				conflictIndex = i
			} else {
				break
			}
		}
		return &raftpb.AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictTerm:  conflictTerm,
			ConflictIndex: conflictIndex,
		}
	}

	// Rule 4: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (only uncommitted entries!).
	var entriesToPersist []*raftpb.LogEntry
	for i, entry := range req.Entries {
		idx := entry.Index
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				// Safety check: Never truncate committed entries!
				if idx <= n.commitIndex {
					panic("safety violation: attempting to truncate committed entry")
				}
				// Truncate in-memory log
				n.log = n.log[:idx]
				if n.storage != nil {
					_ = n.storage.TruncateSuffix(idx)
				}
				// Take this and all remaining entries to append
				entriesToPersist = req.Entries[i:]
				for _, rem := range entriesToPersist {
					n.log = append(n.log, rem)
				}
				break
			}
		} else {
			// Beyond current log, append
			n.log = append(n.log, entry)
			entriesToPersist = append(entriesToPersist, entry)
		}
	}

	// Durably persist new entries before returning success
	if len(entriesToPersist) > 0 && n.storage != nil {
		_ = n.storage.AppendEntries(entriesToPersist)
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > n.commitIndex {
		lastNewIdx, _ := n.lastLogIndexAndTerm()
		newCommit := req.LeaderCommit
		if lastNewIdx < newCommit {
			newCommit = lastNewIdx
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCommittedEntries()
		}
	}

	matchIdx, _ := n.lastLogIndexAndTerm()
	return &raftpb.AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: matchIdx,
	}
}

func (n *RaftNode) onAppendEntriesResponse(
	from string,
	req *raftpb.AppendEntriesRequest,
	resp *raftpb.AppendEntriesResponse,
) {
	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}

	// Ignore stale responses from earlier terms or if no longer leader
	if n.role != RoleLeader || resp.Term != n.currentTerm {
		return
	}

	if resp.Success {
		if resp.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = resp.MatchIndex
		}
		if resp.MatchIndex+1 > n.nextIndex[from] {
			n.nextIndex[from] = resp.MatchIndex + 1
		}
		n.maybeAdvanceCommitIndex()
	} else {
		// Decrement nextIndex with conflict hint
		if resp.ConflictIndex > 0 && resp.ConflictIndex < n.nextIndex[from] {
			n.nextIndex[from] = resp.ConflictIndex
		} else if n.nextIndex[from] > 1 {
			n.nextIndex[from]--
		}
		if n.nextIndex[from] < 1 {
			n.nextIndex[from] = 1
		}
	}
}
