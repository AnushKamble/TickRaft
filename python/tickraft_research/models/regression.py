"""
Model fitting and out-of-sample comparison suite.
Compares M0 (no-signal), M_TVI (trade imbalance), M_L1 (Level-1 OFI), and M_depth (multi-level OFI).
"""

from typing import Dict, List, Optional
import numpy as np
import polars as pl
from tickraft_research.evaluation.hac import fit_ols_with_hac
from tickraft_research.evaluation.metrics import compute_oos_metrics


class MicrostructureModelSuite:
    def __init__(self, depth_levels: int = 5):
        self.depth_levels = depth_levels

    def fit_and_evaluate(
        self,
        train_df: pl.DataFrame,
        test_df: pl.DataFrame,
        target_col: str,
        max_hac_lags: Optional[int] = None
    ) -> Dict:
        """
        Fits M0, M_TVI, M_L1, and M_depth on train_df and evaluates on test_df.
        All models are evaluated on identical row splits.
        """
        y_train = train_df[target_col].to_numpy()
        y_test = test_df[target_col].to_numpy()

        results = {}

        # 1. Model M0: Intercept only (No-signal baseline)
        X0_train = np.ones((len(y_train), 1))
        X0_test = np.ones((len(y_test), 1))
        # Constant fit
        mean_y = float(np.mean(y_train))
        y0_pred = np.full_like(y_test, mean_y)
        results["M0_baseline"] = {
            "in_sample": {"mean_target": mean_y},
            "oos_metrics": compute_oos_metrics(y_test, y0_pred, y_train),
        }

        # 2. Model M_TVI: Trade Volume Imbalance
        if "tvi_sum" in train_df.columns:
            X_tvi_train = train_df["tvi_sum"].to_numpy().reshape(-1, 1)
            X_tvi_test = test_df["tvi_sum"].to_numpy().reshape(-1, 1)

            tvi_fit = fit_ols_with_hac(X_tvi_train, y_train, max_lags=max_hac_lags, feature_names=["tvi"])
            beta_0 = tvi_fit["coefficients"]["intercept"]["coefficient"]
            beta_tvi = tvi_fit["coefficients"]["tvi"]["coefficient"]
            y_tvi_pred = beta_0 + beta_tvi * X_tvi_test.flatten()

            results["M_TVI"] = {
                "in_sample": tvi_fit,
                "oos_metrics": compute_oos_metrics(y_test, y_tvi_pred, y_train),
            }

        # 3. Model M_L1: Level-1 OFI
        if "ofi_l1_sum" in train_df.columns:
            X_l1_train = train_df["ofi_l1_sum"].to_numpy().reshape(-1, 1)
            X_l1_test = test_df["ofi_l1_sum"].to_numpy().reshape(-1, 1)

            l1_fit = fit_ols_with_hac(X_l1_train, y_train, max_lags=max_hac_lags, feature_names=["ofi_l1"])
            beta_0 = l1_fit["coefficients"]["intercept"]["coefficient"]
            beta_l1 = l1_fit["coefficients"]["ofi_l1"]["coefficient"]
            y_l1_pred = beta_0 + beta_l1 * X_l1_test.flatten()

            results["M_L1"] = {
                "in_sample": l1_fit,
                "oos_metrics": compute_oos_metrics(y_test, y_l1_pred, y_train),
            }

        # 4. Model M_depth: Multi-Level Depth OFI Extension (Levels 1 to L)
        depth_cols = [f"ofi_l{lvl}_sum" for lvl in range(1, self.depth_levels + 1) if f"ofi_l{lvl}_sum" in train_df.columns]
        if len(depth_cols) > 0:
            X_depth_train = train_df[depth_cols].to_numpy()
            X_depth_test = test_df[depth_cols].to_numpy()

            depth_fit = fit_ols_with_hac(X_depth_train, y_train, max_lags=max_hac_lags, feature_names=depth_cols)
            beta_0 = depth_fit["coefficients"]["intercept"]["coefficient"]
            betas = np.array([depth_fit["coefficients"][col]["coefficient"] for col in depth_cols])
            y_depth_pred = beta_0 + np.dot(X_depth_test, betas)

            results["M_depth"] = {
                "in_sample": depth_fit,
                "oos_metrics": compute_oos_metrics(y_test, y_depth_pred, y_train),
                "levels_used": len(depth_cols),
            }

        return results
