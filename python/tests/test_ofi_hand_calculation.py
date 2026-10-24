"""
Unit tests comparing calculated Level-1 OFI and grid features against hand-calculated values.
"""

import json
from pathlib import Path
import pytest
from tickraft_research.ingest.lobster import LobsterParser
from tickraft_research.features.ofi import add_ofi_features
from tickraft_research.features.trade_imbalance import add_tvi_feature
from tickraft_research.features.midprice import compute_event_mid_price, build_regular_grid_1s


FIXTURES_DIR = Path("data/fixtures")


def test_hand_calculated_ofi_and_tvi():
    # Load hand-calculated truth
    truth_file = FIXTURES_DIR / "expected_features.json"
    with open(truth_file, "r") as f:
        truth = json.load(f)

    parser = LobsterParser(levels=5)
    df, _ = parser.parse_paired_files(
        FIXTURES_DIR / "synthetic_message.csv",
        FIXTURES_DIR / "synthetic_orderbook.csv"
    )
    df = add_ofi_features(df, levels=5)
    df = add_tvi_feature(df)
    df = df.with_columns(compute_event_mid_price(df))

    # Test event-by-event hand calculations
    for expected in truth["rows"]:
        r = expected["row"]
        expected_ofi = expected["ofi_l1"]
        expected_tvi = expected["tvi"]
        expected_mid = expected["mid_price"]

        actual_ofi = df["ofi_l1"][r]
        actual_tvi = df["tvi"][r]
        actual_mid = df["mid_price"][r]

        if expected_ofi is None:
            assert actual_ofi is None or actual_ofi != actual_ofi  # null / nan check
        else:
            assert actual_ofi == pytest.approx(expected_ofi, 1e-6)

        assert actual_tvi == pytest.approx(expected_tvi, 1e-6)
        assert actual_mid == pytest.approx(expected_mid, 1e-6)


def test_hand_calculated_regular_grid():
    truth_file = FIXTURES_DIR / "expected_features.json"
    with open(truth_file, "r") as f:
        truth = json.load(f)

    parser = LobsterParser(levels=5)
    df, _ = parser.parse_paired_files(
        FIXTURES_DIR / "synthetic_message.csv",
        FIXTURES_DIR / "synthetic_orderbook.csv"
    )
    df = add_ofi_features(df, levels=5)
    df = add_tvi_feature(df)

    grid = build_regular_grid_1s(df, levels=5, forward_fill=True)

    for sec_str, exp_vals in truth["time_buckets_1s"].items():
        sec = int(sec_str)
        matched_row = grid.filter(grid["second_bin"] == sec)
        assert len(matched_row) == 1
        assert matched_row["ofi_l1_sum"][0] == pytest.approx(exp_vals["ofi_sum"], 1e-6)
        assert matched_row["tvi_sum"][0] == pytest.approx(exp_vals["tvi_sum"], 1e-6)
        assert matched_row["mid_price"][0] == pytest.approx(exp_vals["last_mid"], 1e-6)
