"""
Trade Volume Imbalance (TVI) calculations using Polars.
Accurately implements LOBSTER execution event semantics:
In LOBSTER, direction represents the resting limit order side (-1=sell, +1=buy).
For executions (event_type 4 and 5), the aggressor side is the opposite:
- Resting Buy (+1) -> Aggressor Sell -> Signed trade volume = -size
- Resting Sell (-1) -> Aggressor Buy -> Signed trade volume = +size
"""

import polars as pl


def compute_trade_volume_imbalance(df: pl.DataFrame) -> pl.Series:
    """
    Computes signed trade volume imbalance for each row.
    Only execution events (type 4: visible, type 5: hidden) contribute.
    Other events (1: submission, 2: cancel, 3: delete, 6: cross, 7: halt) contribute 0.
    """
    is_execution = (df["event_type"] == 4) | (df["event_type"] == 5)
    
    # Aggressor sign is opposite of resting order direction
    # resting = +1 (buy) => aggressor = -1 (sell)
    # resting = -1 (sell) => aggressor = +1 (buy)
    aggressor_sign = -df["direction"]
    
    signed_vol = pl.when(is_execution) \
                   .then((aggressor_sign * df["size"]).cast(pl.Float64)) \
                   .otherwise(0.0)
                   
    return signed_vol.alias("tvi")


def add_tvi_feature(df: pl.DataFrame) -> pl.DataFrame:
    """Adds the signed TVI feature column to the dataframe."""
    return df.with_columns(compute_trade_volume_imbalance(df))
