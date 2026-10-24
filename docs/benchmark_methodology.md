# TickRaft Systems Benchmark Methodology & Results

## 1. Methodology

The benchmark evaluates the performance trade-off of distributed consensus and durability vs single-process local replay.

### 1.1 Workloads
- **Direct Local Replay:** Single process parses market event batches and executes state machine mutations in-memory without Raft consensus or network RPCs.
- **Replicated Cluster Ingestion:** Full 3-node Raft cluster running separate OS processes on distinct TCP ports with gRPC networking, durable binary length-framed WAL, fsync on commit, and majority quorum commitment.

### 1.2 Parameters & Sweep
- Swept across batch sizes: **100**, **500**, and **1,000** records per command.
- Total events per trial: **3,000** events.
- Metrics measured:
  - Throughput: Events per second, Batches per second
  - Commit Latency: Mean, p50, p95, p99 (in milliseconds)
  - Storage Growth: Physical WAL bytes written to disk

---

## 2. Machine & Environment Metadata

- **Host OS:** Windows 11 AMD64
- **CPU:** AMD Ryzen 9 8940HX (16 Cores, 32 Logical Processors)
- **Go Version:** `go1.27.0 windows/amd64`
- **Storage:** Local NVMe SSD (C:)

---

## 3. Empirical Benchmark Measurements

Raw data recorded in `results/benchmarks/raw_metrics.json`:

| Configuration | Batch Size | Throughput (ev/s) | Throughput (batch/s) | p50 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | Total WAL (bytes) |
|---|---|---|---|---|---|---|---|
| **Direct Local Replay** | 100 | **208,772.6** | 2,087.7 | < 0.01 ms | < 0.01 ms | < 0.01 ms | 0 (in-memory) |
| **Replicated Ingest** | 100 | **1,369.1** | 13.7 | **2.74 ms** | **4.46 ms** | 2,105.3 ms | 524,214 B |
| **Replicated Ingest** | 500 | **1,395.5** | 2.8 | **7.48 ms** | 2,107.4 ms | 2,107.4 ms | 514,764 B |
| **Replicated Ingest** | 1000 | **1,068.7** | 1.1 | **6.24 ms** | 2,793.1 ms | 2,793.1 ms | 513,588 B |

### Key Observations:
1. Replicated ingestion delivers ~1,370 - 1,400 events/second with single-digit millisecond p50 commit latencies (~2.7ms at batch size 100).
2. Direct local replay achieves over 208,000 events/second because it bypasses TCP network boundaries, gRPC serialization, and physical SSD disk fsync.
3. The replication overhead represents the explicit cost of distributed fault-tolerance and crash durability.
