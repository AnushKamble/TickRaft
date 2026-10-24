"""
Chronological train/test splitting with purged horizon embargo.
Prevents forward information leakage from overlapping target labels across split boundaries.
"""

from typing import Dict, List, Optional, Tuple
import polars as pl


def chronological_split_with_embargo(
    df: pl.DataFrame,
    train_ratio: float = 0.70,
    embargo_seconds: int = 60,
    target_col: str = "target_10s"
) -> Tuple[pl.DataFrame, pl.DataFrame, Dict]:
    """
    Splits contiguous regular time series into Train and Test sets:
    - Train: first train_ratio portion of the data.
    - Embargo: an interval of at least embargo_seconds after the training set is dropped.
    - Test: remaining portion of data after the embargo.
    - Excludes rows where target_col is null (e.g. end of series boundary).
    """
    total_len = len(df)
    if total_len == 0:
        raise ValueError("Cannot split empty dataframe")

    split_idx = int(total_len * train_ratio)
    if split_idx <= 0 or split_idx >= total_len:
        raise ValueError(f"Invalid split index {split_idx} for total length {total_len}")

    train_df = df[:split_idx]

    # Embargo cutoff based on time or rows
    # On regular 1-second grid, 1 row = 1 second
    embargo_end_idx = split_idx + embargo_seconds
    if embargo_end_idx >= total_len:
        # Embargo consumes remainder of dataset
        test_df = df[0:0]
    else:
        test_df = df[embargo_end_idx:]

    # Filter out rows with null targets in both sets
    train_eligible = train_df.filter(pl.col(target_col).is_not_null())
    test_eligible = test_df.filter(pl.col(target_col).is_not_null())

    split_report = {
        "total_rows": total_len,
        "train_rows_raw": len(train_df),
        "train_rows_eligible": len(train_eligible),
        "train_dropped_null_targets": len(train_df) - len(train_eligible),
        "embargo_seconds": embargo_seconds,
        "embargo_rows_dropped": min(embargo_seconds, total_len - split_idx),
        "test_rows_raw": len(test_df),
        "test_rows_eligible": len(test_eligible),
        "test_dropped_null_targets": len(test_df) - len(test_eligible),
    }

    return train_eligible, test_eligible, split_report
