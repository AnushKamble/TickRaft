"""
Model evaluation metrics (OOS R2, MAE, RMSE, IC, Directional Accuracy)
and family-wise multiple testing corrections.
"""

from typing import Dict, List, Tuple
import numpy as np


def compute_oos_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_train: np.ndarray) -> Dict:
    """
    Computes out-of-sample explanatory metrics:
    - OOS R2 = 1 - sum((y_true - y_pred)^2) / sum((y_true - mean(y_train))^2)
      (Can be negative if model performs worse out-of-sample than training mean baseline).
    - MAE = mean(|y_true - y_pred|)
    - RMSE = sqrt(mean((y_true - y_pred)^2))
    - IC = Pearson correlation between y_pred and y_true
    - Directional accuracy = mean(sign(y_pred) == sign(y_true))
    """
    if len(y_true) == 0:
        return {}

    errors = y_true - y_pred
    ss_res = np.sum(errors ** 2)

    # Benchmark: mean of training targets
    train_mean = np.mean(y_train) if len(y_train) > 0 else 0.0
    ss_tot = np.sum((y_true - train_mean) ** 2)

    oos_r2 = float(1.0 - (ss_res / ss_tot)) if ss_tot > 1e-12 else 0.0
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors ** 2)))

    # IC (Pearson Correlation)
    std_true = np.std(y_true)
    std_pred = np.std(y_pred)
    if std_true > 1e-12 and std_pred > 1e-12:
        ic = float(np.corrcoef(y_true, y_pred)[0, 1])
    else:
        ic = 0.0

    # Directional Accuracy (excluding zeros)
    sign_true = np.sign(y_true)
    sign_pred = np.sign(y_pred)
    non_zero = (sign_true != 0) & (sign_pred != 0)
    if np.sum(non_zero) > 0:
        dir_acc = float(np.mean(sign_true[non_zero] == sign_pred[non_zero]))
    else:
        dir_acc = 0.5  # neutral baseline

    return {
        "oos_r2": oos_r2,
        "mae": mae,
        "rmse": rmse,
        "ic": ic,
        "directional_accuracy": dir_acc,
        "sample_size": len(y_true),
    }


def holm_bonferroni_correction(p_values: List[float]) -> List[float]:
    """
    Applies Holm-Bonferroni step-down correction for controlling Family-Wise Error Rate (FWER)
    across multiple horizon hypothesis tests.
    """
    m = len(p_values)
    if m == 0:
        return []

    indexed = sorted(enumerate(p_values), key=lambda x: x[1])
    corrected = [0.0] * m

    cum_max = 0.0
    for rank, (orig_idx, p_val) in enumerate(indexed):
        adj_p = min(1.0, p_val * (m - rank))
        cum_max = max(cum_max, adj_p)
        corrected[orig_idx] = cum_max

    return corrected
