"""
End-to-end integration test for the research pipeline and report generation.
"""

import json
from pathlib import Path
import pytest
import polars as pl
from tickraft_research.reports.build_report import run_research_pipeline


def test_research_pipeline_execution(tmp_path):
    # Create sample replay CSV with sufficient rows across 120 seconds
    seconds = list(range(1000, 1150))
    rows = []
    for s in seconds:
        rows.append({
            "source_seq": s - 999,
            "timestamp_ns": s * 1_000_000_000,
            "event_type": 1,
            "order_id": s,
            "size": 100,
            "price": 10000,
            "direction": 1,
            "ask_price_1": 10050 + (s % 5) * 10,
            "ask_size_1": 100,
            "bid_price_1": 10000 + (s % 5) * 10,
            "bid_size_1": 100,
            "ask_price_2": 10060,
            "ask_size_2": 200,
            "bid_price_2": 9990,
            "bid_size_2": 200,
            "ask_price_3": 10070,
            "ask_size_3": 300,
            "bid_price_3": 9980,
            "bid_size_3": 300,
            "ask_price_4": 10080,
            "ask_size_4": 400,
            "bid_price_4": 9970,
            "bid_size_4": 400,
            "ask_price_5": 10090,
            "ask_size_5": 500,
            "bid_price_5": 9960,
            "bid_size_5": 500,
        })
    df = pl.DataFrame(rows)
    export_csv = tmp_path / "sample_replay.csv"
    df.write_csv(export_csv)

    manifest_path = tmp_path / "sample_manifest.json"
    manifest_path.write_text(json.dumps({"dataset_id": "test-dataset", "event_count": len(rows)}))

    output_dir = tmp_path / "results"
    output = run_research_pipeline(
        export_csv=export_csv,
        manifest_path=manifest_path,
        output_dir=output_dir,
        horizons=[1, 5],
        train_ratio=0.70,
        embargo_seconds=10
    )

    assert "horizons" in output
    assert "1s" in output["horizons"]
    assert (output_dir / "research_summary.json").exists()
    assert (output_dir / "horizon_oos_comparison.png").exists()

    h1_models = output["horizons"]["1s"]["models"]
    assert "M0_baseline" in h1_models
    assert "M_L1" in h1_models
    assert "M_depth" in h1_models
    assert "oos_r2" in h1_models["M_L1"]["oos_metrics"]
