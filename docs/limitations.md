# TickRaft Project Limitations & Non-Guarantees

To maintain transparency and research honesty, this document discloses known system boundaries, trade-offs, and negative or non-guaranteed findings.

---

## 1. Systems & Distributed Engineering Limitations

1. **Fixed 3-Node Topology:**
   - TickRaft is strictly scoped to a static 3-node cluster. Dynamic membership changes (joint consensus), node addition/removal, and sharding are not supported.
2. **Crash-Fault Model (CFT, not BFT):**
   - Raft assumes a fail-stop, non-Byzantine model. Nodes may crash, restart, lose messages, delay packets, or partition, but cannot behave adversarially or forge RPC cryptographic signatures.
3. **Not a Production Exchange Gateway:**
   - Designed as a research replay engine. It does not provide sub-microsecond kernel-bypass (DPDK/Solarflare) networking, FPGA order matching, or real-time FIX/ITCH hardware feeds.
4. **Failure Trace Example:**
   - When a leader is partitioned or killed, in-flight client proposals that have not yet reached majority quorum are dropped with `ErrProposalDropped`. Clients must rely on the idempotency contract (`batch_id` + `payload_hash`) to safely retry.

---

## 2. Quantitative & Microstructure Limitations

1. **Depth-Aware OFI Rank Shifts:**
   - Level-2 to Level-5 prices are non-stationary when Level-1 quotes move. A simple fixed-level vector does not map to constant absolute price points.
2. **Proprietary Data Restrictions & Synthetic Fallback:**
   - LOBSTER limit order book data is proprietary subscription data. The repository includes authored synthetic fixtures for verification. Empirical analysis on external data requires lawful licensing.
3. **No Economic Alpha or Profitability Claims:**
   - Statistical predictive association ($R^2$ or IC) does not imply a tradable trading strategy after transaction costs, taker fees, exchange rebates, latency slippage, and queue positioning.
