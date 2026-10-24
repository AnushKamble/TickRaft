"""
Tests for chronological dataset splitting and embargo purging.
"""

from pathlib import Path
import pytest
import polars as pl
from tickraft_research.ingest.lobster import LobsterParser
from tickraft_research.features.ofi import add_ofi_features
from tickraft_research.features.trade_imbalance import add_tvi_feature
from tickraft_research.features.midprice import build_regular_grid_1s, add_forward_targets
from tickraft_research.evaluation.splits import chronological_split_with_embargo


FIXTURES_DIR = Path("data/fixtures")


def test_chronological_split_with_embargo():
    # Construct a synthetic regular series of 200 seconds
    seconds = list(range(1000, 1200))
    mids = [10000.0 + (i % 10) * 5 for i in range(len(seconds))]
    ofis = [(i % 5 - 2) * 10.0 for i in range(len(seconds))]
    tvis = [(i % 3 - 1) * 20.0 for i in range(len(seconds))]

    df = pl.DataFrame({
        "second_bin": seconds,
        "mid_price": mids,
        "ofi_l1_sum": ofis,
        "tvi_sum": tvis,
    })
    df = add_forward_targets(df, horizons=[1, 5, 10])

    embargo = 15
    train_ratio = 0.70
    train_df, test_df, report = chronological_split_with_embargo(
        df,
        train_ratio=train_ratio,
        embargo_seconds=embargo,
        target_col="target_10s"
    )

    # 1. Non-empty partitions
    assert len(train_df) > 0
    assert len(test_df) > 0

    # 2. Strict chronological ordering
    max_train_time = train_df["second_bin"].max()
    min_test_time = test_df["second_bin"].min()
    assert max_train_time < min_test_time

    # 3. Purged embargo gap
    assert min_test_time - max_train_time >= embargo

    # 4. No null targets in eligible partitions
    assert train_df["target_10s"].null_count() == 0
    assert test_df["target_10s"].null_count() == 0

    # 5. Report structure
    assert report["embargo_seconds"] == embargo
    assert report["total_rows"] == 200
    assert report["train_rows_raw"] == int(200 * train_ratio)
