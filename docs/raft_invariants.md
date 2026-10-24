# TickRaft Distributed Consensus Invariants

This document formally specifies the safety invariants enforced and verified by TickRaft's from-scratch Raft consensus core (per Ongaro & Ousterhout 2014).

---

## 1. Core Protocol Safety Invariants

### Invariant 1: Election Safety
*At most one leader can be elected in a given term.*
- **Enforcement:**
  - Each node grants at most one vote per term (`votedFor` is persisted to disk before sending vote response).
  - To become leader, a candidate must receive votes from a strict majority quorum ($\lfloor N/2 \rfloor + 1$).
  - For a 3-node cluster ($N=3$), quorum is 2. Since any two majorities must intersect in at least one node, two candidates cannot both receive majority approval in the same term.
- **Verification:** Verified across 5 randomized seeds in `tests/property/safety_test.go` (`TestElectionSafety`).

### Invariant 2: Candidate Log Up-to-Date Rule
*A voter denies its vote if the candidate’s log is less up-to-date than its own.*
- **Enforcement:**
  - Candidate log is compared by last log term, then last log index:
    $\text{CandidateTerm} > \text{VoterTerm} \lor (\text{CandidateTerm} == \text{VoterTerm} \land \text{CandidateIndex} \ge \text{VoterIndex})$.
- **Verification:** Verified in `tests/raft/election_test.go` (`TestCandidateLogUpToDate`).

### Invariant 3: Leader Append-Only
*A leader never overwrites or truncates its own log entries; it only appends new entries.*
- **Enforcement:** Proposals submitted by clients append sequentially to the leader's in-memory and durable WAL log.

### Invariant 4: Log Matching Invariant
*If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.*
- **Enforcement:**
  - AppendEntries RPC includes `prevLogIndex` and `prevLogTerm`. If the follower does not find a matching entry, it rejects the RPC.
  - Induction ensures all preceding entries match.
- **Verification:** Verified in `tests/raft/replication_test.go` (`TestLogMatching`).

### Invariant 5: Uncommitted Suffix Truncation Only
*A node may only truncate conflicting uncommitted entries (`index > commitIndex`); committed entries are NEVER truncated.*
- **Enforcement:**
  - In `replication.go`, conflict repair checks `idx <= commitIndex`. If a conflict occurs at or before `commitIndex`, a safety panic is triggered.
- **Verification:** Verified in `tests/raft/replication_test.go` (`TestConflictRepairUncommittedOnly`).

### Invariant 6: Current-Term Commitment Rule (Raft Section 5.4.2)
*A leader cannot commit log entries from previous terms by counting replicas.*
- **Enforcement:**
  - When calculating median `matchIndex` $N$, the leader only advances `commitIndex = N` if $log[N].\text{Term} == \text{currentTerm}$.
  - Entries from earlier terms are committed indirectly once an entry from the current term is committed.
- **Verification:** Verified in `tests/raft/commit_test.go` (`TestCurrentTermCommitRule`).

### Invariant 7: State Machine Safety
*If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.*
- **Enforcement:**
  - Entries are applied strictly in sequential order up to `commitIndex`.
  - Invariant $\text{lastApplied} \le \text{commitIndex}$ holds at all times.
- **Verification:** Verified across all nodes in `tests/property/safety_test.go` (`TestNeverApplyUncommitted`).

### Invariant 8: Quorum Non-Blocking and Minority Fail-Closed
*A majority partition ($N \ge 2$) makes progress; an isolated minority partition ($N=1$) cannot acknowledge committed writes.*
- **Enforcement:**
  - Leaders verify majority acknowledgments before advancing `commitIndex`.
  - An isolated leader in a minority partition times out client proposals without committing them.
- **Verification:** Verified in `tests/integration/partition_test.go` (`TestMinorityCannotCommit`).
