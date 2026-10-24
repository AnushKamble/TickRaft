"""
Verification script for raw benchmark metrics.
Asserts that all measured metrics exist and can generate charts.
"""

import json
from pathlib import Path
import matplotlib.pyplot as plt


def verify_benchmarks():
    metrics_path = Path("results/benchmarks/raw_metrics.json")
    if not metrics_path.exists():
        raise FileNotFoundError(f"Raw benchmark metrics file missing at {metrics_path}")

    with open(metrics_path, "r") as f:
        data = json.load(f)

    # 1. Check environment section
    env = data.get("environment", {})
    assert "os" in env and "arch" in env and "num_cpu" in env, "Missing environment metadata"

    # 2. Check direct replay
    direct = data.get("direct_replay", {})
    assert direct.get("total_events", 0) > 0, "Direct replay missing total_events"
    assert direct.get("events_per_sec", 0) > 0, "Direct replay missing events_per_sec"

    # 3. Check replicated runs sweep (at least 3 batch sizes)
    replicated = data.get("replicated_runs", [])
    assert len(replicated) >= 3, f"Expected at least 3 replicated runs, got {len(replicated)}"

    batch_sizes = []
    events_per_sec = []
    p50_latencies = []

    for run in replicated:
        bs = run.get("batch_size")
        eps = run.get("events_per_sec")
        p50 = run.get("latency_p50_ms")

        assert bs in [100, 500, 1000], f"Unexpected batch size {bs}"
        assert eps > 0, "Events per sec must be positive"
        assert p50 > 0, "Latency p50 must be positive"

        batch_sizes.append(str(bs))
        events_per_sec.append(eps)
        p50_latencies.append(p50)

    # Generate benchmark plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

    ax1.bar(batch_sizes, events_per_sec, color="#2b5c8f")
    ax1.set_xlabel("Batch Size (records)")
    ax1.set_ylabel("Throughput (events/sec)")
    ax1.set_title("Replicated Throughput vs Batch Size")
    ax1.grid(True, linestyle="--", alpha=0.5)

    ax2.plot(batch_sizes, p50_latencies, marker="o", color="#d95f02", linewidth=2)
    ax2.set_xlabel("Batch Size (records)")
    ax2.set_ylabel("p50 Commit Latency (ms)")
    ax2.set_title("Commit Latency (p50) vs Batch Size")
    ax2.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    plot_path = Path("results/benchmarks/benchmark_summary.png")
    plt.savefig(plot_path, dpi=150)
    plt.close()

    print(f"Verified benchmark raw data: Direct={direct['events_per_sec']:.1f} ev/s, "
          f"Replicated={[r['events_per_sec'] for r in replicated]}")
    print(f"Benchmark plot generated at {plot_path}")


if __name__ == "__main__":
    verify_benchmarks()
