"""
Unit tests for LOBSTER parser and schema validator.
Tests clean synthetic parsing and explicit rejection of malformed files.
"""

from pathlib import Path
import pytest
from tickraft_research.ingest.lobster import LobsterParser, seconds_to_nanoseconds


FIXTURES_DIR = Path("data/fixtures")


def test_seconds_to_nanoseconds_precision():
    # 34200.123456789 -> 34200123456789 ns exactly
    ns = seconds_to_nanoseconds("34200.123456789")
    assert ns == 34_200_123_456_789
    
    # 0.000000001 -> 1 ns
    assert seconds_to_nanoseconds("0.000000001") == 1


def test_parse_clean_synthetic_dataset():
    parser = LobsterParser(levels=5)
    msg_path = FIXTURES_DIR / "synthetic_message.csv"
    book_path = FIXTURES_DIR / "synthetic_orderbook.csv"
    
    df, report = parser.parse_paired_files(msg_path, book_path)
    
    assert len(df) == 16
    assert report["row_count"] == 16
    assert "source_seq" in df.columns
    assert "timestamp_ns" in df.columns
    assert df["source_seq"].to_list() == list(range(1, 17))
    assert df["bid_price_1"].dtype.is_integer()
    assert df["ask_price_1"].dtype.is_integer()


def test_reject_malformed_columns():
    parser = LobsterParser(levels=5)
    bad_msg = FIXTURES_DIR / "malformed_msg_5cols.csv"
    book = FIXTURES_DIR / "synthetic_orderbook.csv"
    
    with pytest.raises(ValueError, match="Malformed message row"):
        parser.parse_paired_files(bad_msg, book)


def test_reject_unaligned_row_counts():
    parser = LobsterParser(levels=5)
    bad_msg = FIXTURES_DIR / "malformed_unaligned_msg.csv"
    bad_book = FIXTURES_DIR / "malformed_unaligned_book.csv"
    
    with pytest.raises(ValueError, match="Row count mismatch"):
        parser.parse_paired_files(bad_msg, bad_book)


def test_reject_crossed_book():
    parser = LobsterParser(levels=5)
    msg = FIXTURES_DIR / "malformed_single_msg.csv"
    crossed_book = FIXTURES_DIR / "malformed_crossed_book.csv"
    
    with pytest.raises(ValueError, match="crossed or locked book rows"):
        parser.parse_paired_files(msg, crossed_book)


def test_reject_zero_size_level():
    parser = LobsterParser(levels=5)
    msg = FIXTURES_DIR / "malformed_single_msg.csv"
    zero_book = FIXTURES_DIR / "malformed_zero_size_book.csv"
    
    with pytest.raises(ValueError, match="zero or negative"):
        parser.parse_paired_files(msg, zero_book)


def test_equal_timestamps_preserve_sequence_order():
    parser = LobsterParser(levels=5)
    msg = FIXTURES_DIR / "equal_timestamp_msg.csv"
    book = FIXTURES_DIR / "equal_timestamp_book.csv"
    
    df, _ = parser.parse_paired_files(msg, book)
    assert len(df) == 2
    # Timestamps are identical
    assert df["timestamp_ns"][0] == df["timestamp_ns"][1]
    # Source sequence maintains distinct 1, 2 ordering
    assert df["source_seq"][0] == 1
    assert df["source_seq"][1] == 2
    assert df["order_id"][0] == 101
    assert df["order_id"][1] == 102
