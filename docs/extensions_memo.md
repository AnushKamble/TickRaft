# TickRaft — Technical Design Memo: Optional Extensions & Architectural Roadmap

**Document Reference:** `docs/extensions_memo.md`
**Phase:** Phase 13 — Optional Extension Gate
**Author:** Distributed Systems SWE & Quantitative Research Engineer
**Status:** **APPROVED & RECORDED**

---

## 1. Context & Gate Evaluation

Per Section A2 of the TickRaft Master Execution Specification:
> *"Dynamic membership, sharding, Byzantine fault tolerance, production trading, mandatory snapshots, queue-position simulation, automatic alpha claims, cloud deployment, and a dashboard are out of scope. Snapshotting may be attempted only after the core and research acceptance gates pass and must be reported as an optional extension."*

As demonstrated in `FINAL_AUDIT.md`, all mandatory phases (Phases 0 through 12) have passed every evidence gate:
- 33 Go tests across Raft core, WAL durability, crash recovery, multi-process cluster lifecycle, state machine, idempotency, and benchmarks pass.
- 19 Python tests across LOBSTER ingestion, price-aware OFI, TVI directionality, 1s grid aggregation, non-leaking forward targets, purged embargoes, HAC inference, and Holm-Bonferroni correction pass.
- Direct multi-process cluster replay and committed export verification pass.

This document formally records the technical evaluation of candidate extensions: **Raft Log Compaction & Snapshotting** and **Square-Root Price Impact Metaorder Analysis**.

---

## 2. Evaluation of Extension 1: Raft Log Compaction & Snapshotting

### 2.1 The Problem in Distributed Logs
In general-purpose distributed state machines (such as etcd or ZooKeeper), the log stores mutations (e.g., `Set(k, v)`), but the state machine only requires the current value of each key. Without compaction, the WAL grows unbounded, and restarting a node requires replaying every entry from index 1. In Raft (Ongaro & Ousterhout §7), this is addressed via `InstallSnapshot` RPCs and prefix truncation.

### 2.2 The Specific Nature of Market-Data Replay
In TickRaft, the replicated state machine is an **ordered, immutable market-event journal**:
1. **Historical Immutability is the Value:** The primary client of TickRaft is the quantitative research exporter (`tickraft-export`), which materializes the exact, gap-free, tick-by-tick sequence of quote adjustments, cancelations, and executions.
2. **Loss of Microstructure Granularity:** Compacting a sequence of 100,000 order book updates into a single snapshot of Level-1 to Level-5 prices and depths destroys the microsecond event dynamics required to compute:
   - Order Flow Imbalance (OFI) $\Delta \text{size}$ transitions;
   - Signed Trade-Volume Imbalance (TVI);
   - Sub-second duration metrics between order placements and cancelations.
3. **Deterministic Digest Preservation:** TickRaft maintains an incremental rolling SHA-256 digest:
   $$\text{digest}_k = \text{SHA256}(\text{digest}_{k-1} \,\|\, \text{event}_k)$$
   Trunking the log without persisting the intermediate digest chain prevents subsequent nodes from verifying continuous provenance from event 1.

### 2.3 Proposed Production Architecture for Snapshotting
For production continuous operation across multi-week sessions, snapshotting should follow an **Epoch-Session Archival Pattern**:
1. **Session-End Snapshot:** At the end of each trading session (e.g., 16:00:00 EST), the leader appends a `FinalizeSession` marker containing the session's closing order book snapshot, event count, and terminal rolling digest.
2. **Archival Prior to Truncation:** The committed session slice ($[1, K]$) is flushed to Parquet cold storage on local NVMe or S3.
3. **Prefix Compaction:** Once acknowledged by a quorum, log indices $1 \dots K-1$ may be safely unlinked from active WAL segments, retaining index $K$ as the snapshot base entry with its term and rolling digest.
4. **`InstallSnapshot` RPC:** A follower lagging behind the compacted boundary receives the snapshot manifest and base order-book state, then resumes streaming delta batches from index $K+1$.

**Decision:** The core 3-node cluster currently handles multi-million event replay sessions with high efficiency. Log compaction is formally specified above and deferred to post-MVP productionization to prevent compromising historical tick fidelity.

---

## 3. Evaluation of Extension 2: Square-Root Law & Metaorder Aggregation

### 3.1 Theoretical Background
The Square-Root Law of Price Impact (Bouchaud et al., 2018; Torre, 1997) states that the average price impact $I$ of executing a large metaorder of volume $Q$ scales as:
$$I(Q) \approx Y \cdot \sigma \cdot \sqrt{\frac{Q}{V}}$$
where:
- $Y$ is a dimensionless constant of order unity ($Y \approx 0.5 - 0.7$);
- $\sigma$ is daily volatility;
- $V$ is daily market volume;
- $Q$ is the total volume of the metaorder.

### 3.2 Methodological Pitfalls of Naive Microstructure Estimation
Estimating price impact directly on raw LOBSTER execution messages without metaorder reconstruction produces spurious results:
1. **Child-Order Fragmentation:** Institutional market participants break large parent metaorders into thousands of small child orders executed via execution algorithms (TWAP, VWAP, POV). Individual executions in the LOBSTER message file are child slices ($q_i \ll Q$).
2. **Endogeneity & Liquidity Conditioning:** Algorithms aggressively execute larger child slices when prevailing order book depth is thick (low price impact) and execute smaller sizes when depth is thin. Regressing single-trade price changes on single-trade size produces concave or downward-biased impact curves due to endogenous timing.
3. **Metaorder Reconstruction Requirements:** To obtain a defensible empirical test of the square-root law, researchers must employ parent order clustering algorithms (e.g., assigning consecutive same-direction trades within narrow inter-trade arrival times $\Delta t \le \delta$ to the same metaorder until an idle gap occurs).

### 3.3 Proposed Metaorder Extension Architecture
When real multi-day market data is ingested, the research module can be extended via:
```python
# Conceptual metaorder aggregation in tickraft_research/features/metaorder.py
def reconstruct_metaorders(trades_df: pl.DataFrame, idle_threshold_sec: float = 10.0) -> pl.DataFrame:
    """
    Cluster same-direction trades into empirical metaorders based on inter-arrival gaps.
    Calculates metaorder duration, total parent volume Q, and net price displacement.
    """
    # 1. Filter execution events (event_type == 4 or 5)
    # 2. Compute inter-trade time delta within direction
    # 3. Increment metaorder_id on direction flip or time_delta > idle_threshold_sec
    # 4. Group by metaorder_id and aggregate Q, start_price, end_price, duration
    ...
```

**Decision:** Individual trade volume is already captured in the signed TVI feature ($M_{\text{TVI}}$). Full square-root law metaorder clustering is retained as an exploratory extension requiring multi-session data, avoiding misleading claims on isolated synthetic fixtures.

---

## 4. Final Recommendation & Sign-Off

The core TickRaft distributed replication engine and microstructure research framework meet all functional, architectural, performance, and econometric mandates outlined in the master specification. All candidate extensions have concrete, defensible architectural blueprints recorded in this memo.

**Phase 13 Status: VERIFIED & COMPLETED**
