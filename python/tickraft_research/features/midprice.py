"""
Mid-price calculation, regular 1-second grid construction, and causal forward targets.
"""

from typing import List, Optional
import polars as pl


def compute_event_mid_price(df: pl.DataFrame) -> pl.Series:
    """Computes exact mid-price for each row: (ask_price_1 + bid_price_1) / 2.0."""
    return ((df["ask_price_1"] + df["bid_price_1"]) / 2.0).alias("mid_price")


def build_regular_grid_1s(
    df: pl.DataFrame,
    levels: int = 5,
    forward_fill: bool = True
) -> pl.DataFrame:
    """
    Constructs a regular 1-second time grid:
    - Flow features (OFI, TVI) are summed within each 1-second bucket.
    - Mid-price is the last observed mid-price in the bucket.
    - If a bucket has no events, forward-fills the previous mid-price (if enabled), while flow features are 0.
    """
    df_with_mid = df.with_columns(compute_event_mid_price(df))

    # Calculate integer second timestamp
    df_with_sec = df_with_mid.with_columns(
        (pl.col("timestamp_ns") // 1_000_000_000).alias("second_bin")
    )

    # Aggregation expressions
    agg_exprs = [
        pl.col("mid_price").last().alias("mid_price"),
        pl.col("tvi").sum().alias("tvi_sum")
    ]
    for lvl in range(1, levels + 1):
        if f"ofi_l{lvl}" in df.columns:
            agg_exprs.append(pl.col(f"ofi_l{lvl}").sum().alias(f"ofi_l{lvl}_sum"))

    grid_df = df_with_sec.group_by("second_bin").agg(agg_exprs).sort("second_bin")

    if forward_fill and len(grid_df) > 1:
        # Create complete sequence of seconds from min to max
        min_sec = grid_df["second_bin"].min()
        max_sec = grid_df["second_bin"].max()
        all_seconds = pl.DataFrame({
            "second_bin": list(range(min_sec, max_sec + 1))
        })
        grid_df = all_seconds.join(grid_df, on="second_bin", how="left")

        # Forward fill mid-price
        grid_df = grid_df.with_columns(pl.col("mid_price").forward_fill())

        # Fill zero for missing flow sums
        fill_zeros = [pl.col("tvi_sum").fill_null(0.0)]
        for lvl in range(1, levels + 1):
            col_name = f"ofi_l{lvl}_sum"
            if col_name in grid_df.columns:
                fill_zeros.append(pl.col(col_name).fill_null(0.0))
        grid_df = grid_df.with_columns(fill_zeros)

    return grid_df


def add_forward_targets(
    grid_df: pl.DataFrame,
    horizons: List[int] = [1, 5, 10, 30, 60]
) -> pl.DataFrame:
    """
    Computes causal forward targets: target_h = mid_{t+h} - mid_t.
    Uses future rows at offset h. Rows where t+h exceeds the available series receive null.
    """
    target_exprs = []
    for h in horizons:
        # Since grid_df has 1-second contiguous steps:
        # mid_{t+h} is shift(-h)
        target_exprs.append(
            (pl.col("mid_price").shift(-h) - pl.col("mid_price")).alias(f"target_{h}s")
        )
    return grid_df.with_columns(target_exprs)
