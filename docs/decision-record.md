# TickRaft Architecture Decision Record (ADR)

## ADR-001: From-Scratch Raft Implementation vs Third-Party Raft Libraries
- **Status:** Accepted
- **Context:** The system needs distributed replication for market-data event batches. Standard libraries (`etcd/raft`, `hashicorp/raft`) abstract consensus internals.
- **Decision:** Implement the complete Raft consensus protocol from scratch in Go per Ongaro & Ousterhout (2014).
- **Rationale:**
  1. Full control over state machine application, term transitions, and current-term commit enforcement.
  2. Eliminates opaque abstractions, allowing direct verification of invariants (majority quorum, monotonic commitIndex, atomic hard-state fsync, uncommitted suffix truncation).
  3. Enables direct deterministic network and fault simulation with logical virtual clocks.

## ADR-002: Fixed 3-Node Topology and Static Cluster Quorum
- **Status:** Accepted
- **Context:** Raft supports dynamic membership changes (joint consensus), which adds substantial protocol complexity.
- **Decision:** Fix cluster membership at exactly 3 nodes (`node-1`, `node-2`, `node-3`) with a static quorum threshold of 2.
- **Rationale:**
  1. A 3-node cluster provides single-fault tolerance (tolerates 1 crashed/isolated node) which completely proves quorum safety.
  2. Keeps focus on core market-event ordering, WAL durability, fault recovery, and microstructure research rather than distributed re-configuration.

## ADR-003: Single-Owner Actor/Channel Concurrency Model for Raft Node
- **Status:** Accepted
- **Context:** Multi-threaded Raft implementations often suffer from subtle race conditions when mutexes are held across RPC boundaries or state updates.
- **Decision:** Design the core Raft engine with a single-goroutine state owner (actor loop) handling all state mutations, timer ticks, RPC dispatches, and incoming messages via Go channels.
- **Rationale:**
  1. Eliminates mutex deadlock hazards and prevents data races (`go test -race` compliance).
  2. Guarantees serialized, deterministic state transitions.

## ADR-004: Versioned Binary Length-Framed WAL with CRC32 Checksums
- **Status:** Accepted
- **Context:** Market event batches must be persisted durably to disk before acknowledging replication success.
- **Decision:** Log entries are written to a Write-Ahead Log (WAL) formatted as: `[RecordLength: 4B uint32][Magic/Type: 2B][CRC32: 4B uint32][Payload: N bytes]`. Hard-state (`currentTerm`, `votedFor`) is written atomically to a dedicated state file with atomic rename and fsync.
- **Rationale:**
  1. Length framing allows sequential record recovery and early bounds checking.
  2. CRC32 provides end-to-end data integrity validation.
  3. Clear crash recovery policy: corrupted or partial final tail records can be safely truncated during crash recovery; mid-file corruption triggers a loud fatal error to prevent silent data loss.

## ADR-005: Market-Log State Machine Commands and Idempotency Contract
- **Status:** Accepted
- **Context:** Network retry and failover can cause clients to re-send event batches.
- **Decision:** State machine commands are strictly typed: `BeginDataset`, `AppendEventBatch`, and `FinalizeDataset`. State machine stores a map of committed `batch_id -> payload_hash`.
  - Re-proposing an identical batch (same `batch_id` and same `payload_hash`) is treated as an idempotent no-op returning the original success status.
  - Submitting an identical `batch_id` with a differing `payload_hash` returns an immediate, deterministic error.
  - FinalizeDataset checks that contiguous source sequences were applied and that the computed rolling SHA-256 digest matches the declared manifest.

## ADR-006: Local Multi-Process and Docker Compose Deployment Strategy
- **Status:** Accepted
- **Context:** The development and testing environment runs on Windows without an active Docker daemon in the system path.
- **Decision:**
  1. Provide full `Dockerfile` and `docker-compose.yml` configurations for standard containerized deployment.
  2. For local testing, verification, and automated test suites, provide a native multi-process orchestration module (`internal/cluster`) where 3 distinct OS processes run on loopback ports (e.g. 50051, 50052, 50053) with separate data directories.
- **Rationale:** Guarantees complete end-to-end multi-process distributed systems verification regardless of local Docker availability.

## ADR-007: Integer Fixed-Point Units and Nanosecond Timestamps for Market Data
- **Status:** Accepted
- **Context:** Binary floating-point representation causes roundoff error in financial order-book processing and price comparisons.
- **Decision:** All prices are preserved as integer fixed-point units (`price_int`, as provided by LOBSTER, e.g. price in cents or 10000ths of a dollar). Timestamps are converted from seconds-past-midnight into exact integer nanoseconds using exact decimal conversion. Original row ordering is preserved for events with identical timestamps.

## ADR-008: Quote-Price Aware Level-1 OFI and Project-Defined Depth Extension
- **Status:** Accepted
- **Context:** Naive order flow imbalance simplifies delta to `ΔBidSize - ΔAskSize`, which is mathematically incorrect when quote prices change.
- **Decision:** Implement Level-1 OFI following Cont, Kukanov & Stoikov (2014):
  - Bid component: `+ΔBidSize` if BidPrice rises, `BidSize` if BidPrice unchanged, `0` or `-PrevBidSize` if BidPrice drops.
  - Ask component: `-ΔAskSize` if AskPrice drops, `-AskSize` if AskPrice unchanged, `0` or `+PrevAskSize` if AskPrice rises.
  - Multi-level depth extension: Project-defined vector `[OFI_level_1, ..., OFI_level_L]` for $L=5$ levels, explicitly documenting the rank-shift limitation when the top level moves.

## ADR-009: Chronological Splits, Embargo, and Newey-West HAC Inference
- **Status:** Accepted
- **Context:** High-frequency microstructure data exhibits strong serial autocorrelation; overlapping horizons create mechanical moving-average errors; random train/test splits cause severe lookahead leakage.
- **Decision:**
  1. Chronological time-series split with an explicit embargo period equal to at least the maximum target horizon (e.g. 60 seconds) around split boundaries.
  2. Fit future price target $Y_{t+h} = \text{mid}_{t+h} - \text{mid}_t$ strictly using regular 1-second grid observations.
  3. Estimate linear regression coefficients using statsmodels with Newey-West / HAC robust covariance, evaluating lag sensitivity.
  4. Multiple-testing correction (Holm-Bonferroni) across horizon sweeps.
