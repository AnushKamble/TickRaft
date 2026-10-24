"""
Unit tests for signed trade volume imbalance (TVI) logic.
Verifies LOBSTER resting vs aggressor direction semantics.
"""

from pathlib import Path
import pytest
from tickraft_research.ingest.lobster import LobsterParser
from tickraft_research.features.trade_imbalance import add_tvi_feature


FIXTURES_DIR = Path("data/fixtures")


def test_trade_imbalance_direction_semantics():
    parser = LobsterParser(levels=5)
    df, _ = parser.parse_paired_files(
        FIXTURES_DIR / "synthetic_message.csv",
        FIXTURES_DIR / "synthetic_orderbook.csv"
    )
    df = add_tvi_feature(df)

    # Row 3 is a visible execution against resting buy (+1). Aggressor is seller (-1). TVI must be -80.
    assert df["event_type"][3] == 4
    assert df["direction"][3] == 1
    assert df["size"][3] == 80
    assert df["tvi"][3] == -80.0

    # Row 6 is a visible execution against resting sell (-1). Aggressor is buyer (+1). TVI must be +40.
    assert df["event_type"][6] == 4
    assert df["direction"][6] == -1
    assert df["size"][6] == 40
    assert df["tvi"][6] == 40.0

    # Row 1 is a submission (event_type 1). TVI must be 0.
    assert df["event_type"][1] == 1
    assert df["tvi"][1] == 0.0
