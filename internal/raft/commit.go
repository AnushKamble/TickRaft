package raft

import (
	"sort"
	raftpb "tickraft/proto/raft"
)

func (n *RaftNode) onClientProposal(data []byte, replyChan chan ClientProposalResult) {
	if n.role != RoleLeader {
		replyChan <- ClientProposalResult{
			Err:      ErrNotLeader,
			Response: n.currentLeader,
		}
		close(replyChan)
		return
	}

	lastIdx, _ := n.lastLogIndexAndTerm()
	newIdx := lastIdx + 1
	entry := &raftpb.LogEntry{
		Index:     newIdx,
		Term:      n.currentTerm,
		Data:      data,
		EntryType: 0,
	}

	n.log = append(n.log, entry)

	// Persist entry to storage before broadcasting
	if n.storage != nil {
		_ = n.storage.AppendEntries([]*raftpb.LogEntry{entry})
	}

	n.waiters[newIdx] = replyChan
	n.broadcastAppendEntries()

	// In single-node edge-case, advance immediately
	totalNodes := len(n.peers) + 1
	if totalNodes == 1 {
		n.commitIndex = newIdx
		n.applyCommittedEntries()
	}
}

// maybeAdvanceCommitIndex enforces the Raft Section 5.4.2 current-term commitment rule:
// A leader cannot determine commitment by counting replicas on entries from older terms.
// Only log entries from the leader's current term are committed by counting replicas;
// once an entry from the current term is committed, older entries are committed indirectly.
func (n *RaftNode) maybeAdvanceCommitIndex() {
	if n.role != RoleLeader {
		return
	}

	totalNodes := len(n.peers) + 1
	majority := (totalNodes / 2) + 1

	lastLogIdx, _ := n.lastLogIndexAndTerm()
	matches := make([]uint64, 0, totalNodes)
	matches = append(matches, lastLogIdx) // Leader's own log

	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	// Median matchIndex is at index (majority - 1)
	N := matches[majority-1]

	if N > n.commitIndex {
		// Strict check: entry at N MUST be from currentTerm
		if N < uint64(len(n.log)) && n.log[N].Term == n.currentTerm {
			n.commitIndex = N
			n.applyCommittedEntries()
		}
	}
}

func (n *RaftNode) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		var resp interface{}
		var err error
		if n.stateMachine != nil {
			resp, err = n.stateMachine.Apply(entry)
		}

		// Acknowledge client waiter if registered on this node
		if waiter, exists := n.waiters[n.lastApplied]; exists {
			waiter <- ClientProposalResult{
				CommittedIndex: entry.Index,
				Term:           entry.Term,
				Response:       resp,
				Err:            err,
			}
			close(waiter)
			delete(n.waiters, n.lastApplied)
		}
	}
}
