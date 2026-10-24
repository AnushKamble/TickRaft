"""
Tests for causal integrity and absence of lookahead leakage.
Verifies that feature values at time t are invariant under future data perturbations.
"""

from pathlib import Path
import pytest
import polars as pl
from tickraft_research.ingest.lobster import LobsterParser
from tickraft_research.features.ofi import add_ofi_features
from tickraft_research.features.trade_imbalance import add_tvi_feature
from tickraft_research.features.midprice import compute_event_mid_price, build_regular_grid_1s, add_forward_targets


FIXTURES_DIR = Path("data/fixtures")


def test_features_invariant_to_future_perturbations():
    parser = LobsterParser(levels=5)
    df, _ = parser.parse_paired_files(
        FIXTURES_DIR / "synthetic_message.csv",
        FIXTURES_DIR / "synthetic_orderbook.csv"
    )

    # Compute features on original dataset
    df1 = add_ofi_features(df, levels=5)
    df1 = add_tvi_feature(df1)
    df1 = df1.with_columns(compute_event_mid_price(df1))

    # Create perturbed dataset where all events after row 5 are radically changed
    cutoff = 5
    perturbed_df = df.clone()
    
    # Mutate prices and sizes after cutoff
    perturbed_df = perturbed_df.with_columns([
        pl.when(pl.col("source_seq") > cutoff + 1)
          .then(pl.col("ask_price_1") + 5000)
          .otherwise(pl.col("ask_price_1"))
          .alias("ask_price_1"),
        pl.when(pl.col("source_seq") > cutoff + 1)
          .then(pl.col("size") * 10)
          .otherwise(pl.col("size"))
          .alias("size")
    ])

    df2 = add_ofi_features(perturbed_df, levels=5)
    df2 = add_tvi_feature(df2)
    df2 = df2.with_columns(compute_event_mid_price(df2))

    # Invariant: Features at or before cutoff must be identical down to exact equality!
    for col in ["ofi_l1", "tvi", "mid_price"]:
        val1 = df1[col][:cutoff].to_list()
        val2 = df2[col][:cutoff].to_list()
        assert val1 == val2, f"Future perturbation leaked into past features for column {col}"


def test_regular_grid_no_leakage():
    parser = LobsterParser(levels=5)
    df, _ = parser.parse_paired_files(
        FIXTURES_DIR / "synthetic_message.csv",
        FIXTURES_DIR / "synthetic_orderbook.csv"
    )
    df = add_ofi_features(df, levels=5)
    df = add_tvi_feature(df)

    grid1 = build_regular_grid_1s(df, levels=5)
    grid1_with_targets = add_forward_targets(grid1, horizons=[1, 5, 10])

    # Assert forward targets do not alter existing feature values
    for col in ["mid_price", "ofi_l1_sum", "tvi_sum"]:
        assert grid1[col].to_list() == grid1_with_targets[col].to_list()
