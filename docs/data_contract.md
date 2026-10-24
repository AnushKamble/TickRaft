# TickRaft Data Contract & Schema Specifications

This document defines the data contracts, serialization formats, schema validations, and idempotency guarantees for TickRaft.

---

## 1. Input Data Contracts (LOBSTER Format)

### 1.1 Message File Contract
- **File Format:** No-header CSV.
- **Columns (exactly 6):**
  1. `time_seconds`: Timestamp in seconds after midnight (e.g. `34200.123456789`). Converted to exact integer nanoseconds using exact decimal conversion.
  2. `event_type`: Integer event category:
     - `1`: Submission of limit order
     - `2`: Cancellation (partial reduction)
     - `3`: Deletion (total removal)
     - `4`: Execution of visible limit order
     - `5`: Execution of hidden limit order
     - `6`: Cross / auction trade
     - `7`: Trading halt status
  3. `order_id`: Unique 64-bit integer order ID.
  4. `size`: Number of shares (positive unsigned integer).
  5. `price_int`: Integer fixed-point price (dollar cents or dollar/10000). Never converted to floating point.
  6. `direction`: Resting order side: `-1` = Sell limit order resting; `+1` = Buy limit order resting.

### 1.2 Order Book File Contract
- **File Format:** No-header CSV.
- **Columns:** Exactly $4 \times L$ columns for $L$ book levels.
  - Per level in order: `AskPrice_k, AskSize_k, BidPrice_k, BidSize_k` for $k = 1 \dots L$.
- **Invariants:**
  - `BidPrice_1 < AskPrice_1` (no crossed or locked markets).
  - All sizes $> 0$.
  - Equal row count between message file and order book file.
  - Each book row represents the post-event state of the order book immediately after the corresponding message event.

---

## 2. Replicated State Machine Commands

Commands proposed to the Raft consensus cluster are strictly typed in `proto/marketlog/marketlog.proto`:

### 2.1 `BeginDatasetCommand`
Initializes a new market replay session:
```protobuf
message BeginDatasetCommand {
  string dataset_id = 1;
  string symbol = 2;
  string session_date = 3;
  uint64 expected_events = 4;
  uint32 levels = 5;
  string source_manifest_hash = 6;
  string schema_version = 7;
}
```

### 2.2 `AppendEventBatchCommand`
Transfers a bounded batch of normalized market events:
```protobuf
message AppendEventBatchCommand {
  string batch_id = 1;         // Deterministic unique batch identifier
  string dataset_id = 2;
  uint64 batch_seq = 3;        // Monotonic batch sequence (1, 2, ...)
  uint64 seq_start = 4;        // Monotonic event start sequence
  uint64 seq_end = 5;          // Monotonic event end sequence
  string payload_hash = 6;     // SHA-256 of batch payload
  repeated MarketEvent events = 7;
}
```

### 2.3 `FinalizeDatasetCommand`
Finalizes and audits dataset integrity:
```protobuf
message FinalizeDatasetCommand {
  string dataset_id = 1;
  uint64 expected_events = 2;
  uint64 final_seq = 3;
  string expected_rolling_digest = 4;
  string source_manifest_hash = 5;
}
```

---

## 3. Idempotency & Deduplication Contract

1. **Deterministic Identity:** Every batch is tagged with a stable `batch_id` and a SHA-256 `payload_hash`.
2. **Idempotent Retry:** If a client submits a batch with a known `batch_id` and the **same** `payload_hash`, the state machine treats it as an idempotent no-op, returning `success=true, is_duplicate=true`.
3. **Conflict Rejection:** If a client proposes a batch with an existing `batch_id` but a **different** `payload_hash`, the state machine rejects it with `ErrDuplicateBatchConflict`.
4. **Manifest Audit:** Finalization verifies that applied events equal `expected_events`, final sequence matches `final_seq`, and the rolling SHA-256 digest matches the pre-computed hash.
