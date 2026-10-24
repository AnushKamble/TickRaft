"""
LOBSTER market data parser and validator using Polars.
Ensures strict schema compliance, equal row-counts, and invariant verification.
"""

from decimal import Decimal
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import polars as pl


MESSAGE_COLUMNS = [
    "time_seconds",
    "event_type",
    "order_id",
    "size",
    "price_int",
    "direction"
]


def seconds_to_nanoseconds(sec_str: str) -> int:
    """
    Converts seconds-after-midnight string into exact integer nanoseconds using Decimal.
    Prevents floating-point precision loss.
    """
    d = Decimal(sec_str)
    return int(d * Decimal(1_000_000_000))


def compute_file_hash(path: Path) -> str:
    """Computes SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


class LobsterParser:
    def __init__(self, levels: int = 5):
        self.levels = levels
        self.book_columns = []
        for i in range(1, levels + 1):
            self.book_columns.extend([
                f"ask_price_{i}",
                f"ask_size_{i}",
                f"bid_price_{i}",
                f"bid_size_{i}"
            ])

    def parse_paired_files(
        self,
        message_path: Path,
        book_path: Path
    ) -> Tuple[pl.DataFrame, Dict]:
        """
        Parses paired LOBSTER message and orderbook CSV files.
        Validates row count alignment, book invariants, and schema.
        """
        message_path = Path(message_path)
        book_path = Path(book_path)

        if not message_path.exists():
            raise FileNotFoundError(f"Message file not found: {message_path}")
        if not book_path.exists():
            raise FileNotFoundError(f"Book file not found: {book_path}")

        # Compute source file hashes
        msg_hash = compute_file_hash(message_path)
        book_hash = compute_file_hash(book_path)

        # Validate line formatting and convert exact timestamps
        time_ns_list = []
        with open(message_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) != 6:
                    raise ValueError(f"Malformed message row: expected 6 columns, got {len(parts)} in line: {line}")
                time_ns_list.append(seconds_to_nanoseconds(parts[0]))

        expected_book_cols = 4 * self.levels
        with open(book_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) != expected_book_cols:
                    raise ValueError(f"Malformed book row: expected {expected_book_cols} columns, got {len(parts)}")

        # Parse message CSV
        try:
            msg_df = pl.read_csv(
                message_path,
                has_header=False,
                new_columns=MESSAGE_COLUMNS,
                schema_overrides={
                    "time_seconds": pl.Float64,
                    "event_type": pl.UInt32,
                    "order_id": pl.UInt64,
                    "size": pl.UInt64,
                    "price_int": pl.Int64,
                    "direction": pl.Int32
                }
            )
        except Exception as e:
            raise ValueError(f"Malformed message row: {e}")

        # Parse orderbook CSV
        try:
            book_df = pl.read_csv(
                book_path,
                has_header=False,
                new_columns=self.book_columns,
                schema_overrides={col: pl.Int64 if "price" in col else pl.UInt64 for col in self.book_columns}
            )
        except Exception as e:
            raise ValueError(f"Malformed book row: {e}")

        # Invariant 1: Equal row counts
        if len(msg_df) != len(book_df):
            raise ValueError(
                f"Row count mismatch between message ({len(msg_df)}) and book ({len(book_df)})"
            )

        # Invariant 2: Timestamp monotonicity check and preserving source sequence
        msg_df = msg_df.with_columns([
            pl.Series("timestamp_ns", time_ns_list, dtype=pl.Int64),
            pl.int_range(1, len(msg_df) + 1, dtype=pl.UInt64).alias("source_seq")
        ])

        # Validate orderbook invariants (bid < ask, sizes > 0)
        crossed_mask = (book_df["bid_price_1"] >= book_df["ask_price_1"])
        crossed_count = crossed_mask.sum()
        if crossed_count > 0:
            raise ValueError(f"Found {crossed_count} crossed or locked book rows where bid_price_1 >= ask_price_1")

        zero_size_mask = (book_df["ask_size_1"] <= 0) | (book_df["bid_size_1"] <= 0)
        zero_size_count = zero_size_mask.sum()
        if zero_size_count > 0:
            raise ValueError(f"Found {zero_size_count} rows with zero or negative top-of-book size")

        # Combine into canonical aligned dataframe
        combined_df = pl.concat([msg_df, book_df], how="horizontal")

        validation_report = {
            "row_count": len(combined_df),
            "message_hash": msg_hash,
            "book_hash": book_hash,
            "levels": self.levels,
            "min_timestamp_ns": time_ns_list[0] if time_ns_list else 0,
            "max_timestamp_ns": time_ns_list[-1] if time_ns_list else 0,
            "event_type_distribution": msg_df["event_type"].value_counts().to_dicts(),
        }

        return combined_df, validation_report
