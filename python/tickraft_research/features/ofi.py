"""
Order Flow Imbalance (OFI) calculations using Polars.
Implements quote-price aware Level-1 OFI (Cont, Kukanov & Stoikov 2014)
and a project-defined multi-level depth-aware OFI extension.
"""

from typing import List, Optional
import polars as pl


def compute_level_ofi(
    df: pl.DataFrame,
    level: int = 1
) -> pl.Series:
    """
    Computes quote-price aware OFI for a specific order book level k.
    Row 0 has no prior book row and will return null.
    """
    bid_p_curr = df[f"bid_price_{level}"]
    bid_p_prev = bid_p_curr.shift(1)
    bid_s_curr = df[f"bid_size_{level}"].cast(pl.Float64)
    bid_s_prev = bid_s_curr.shift(1)

    ask_p_curr = df[f"ask_price_{level}"]
    ask_p_prev = ask_p_curr.shift(1)
    ask_s_curr = df[f"ask_size_{level}"].cast(pl.Float64)
    ask_s_prev = ask_s_curr.shift(1)

    # Bid side component
    # If BidPrice rises: +bid_s_curr
    # If BidPrice unchanged: bid_s_curr - bid_s_prev
    # If BidPrice drops: -bid_s_prev
    bid_ofi = pl.when(bid_p_curr > bid_p_prev).then(bid_s_curr) \
                .when(bid_p_curr == bid_p_prev).then(bid_s_curr - bid_s_prev) \
                .when(bid_p_curr < bid_p_prev).then(-bid_s_prev) \
                .otherwise(None)

    # Ask side component
    # If AskPrice rises: +ask_s_prev
    # If AskPrice unchanged: -(ask_s_curr - ask_s_prev)
    # If AskPrice drops: -ask_s_curr
    ask_ofi = pl.when(ask_p_curr > ask_p_prev).then(ask_s_prev) \
                .when(ask_p_curr == ask_p_prev).then(ask_s_prev - ask_s_curr) \
                .when(ask_p_curr < ask_p_prev).then(-ask_s_curr) \
                .otherwise(None)

    total_ofi = bid_ofi + ask_ofi
    return total_ofi.alias(f"ofi_l{level}")


def add_ofi_features(df: pl.DataFrame, levels: int = 5) -> pl.DataFrame:
    """
    Adds Level-1 OFI and depth-aware OFI columns (ofi_l1 to ofi_lL) to the DataFrame.
    """
    ofi_series = [compute_level_ofi(df, lvl) for lvl in range(1, levels + 1)]
    return df.with_columns(ofi_series)
