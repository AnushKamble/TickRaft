"""
Tests for OOS metrics calculation and multiple testing corrections.
"""

import numpy as np
import pytest
from tickraft_research.evaluation.metrics import compute_oos_metrics, holm_bonferroni_correction


def test_compute_oos_metrics():
    y_train = np.array([10.0, 10.0, 10.0])
    y_true = np.array([12.0, 8.0, 10.0])
    y_pred = np.array([11.0, 9.0, 10.0])

    metrics = compute_oos_metrics(y_true, y_pred, y_train)

    assert metrics["mae"] == pytest.approx(2.0 / 3.0, 1e-6)
    assert metrics["rmse"] == pytest.approx(np.sqrt(2.0 / 3.0), 1e-6)
    assert metrics["sample_size"] == 3
    # SS_tot from train mean (10) = (12-10)^2 + (8-10)^2 + (10-10)^2 = 4 + 4 + 0 = 8
    # SS_res = (12-11)^2 + (8-9)^2 + 0 = 1 + 1 = 2
    # R2 = 1 - 2/8 = 0.75
    assert metrics["oos_r2"] == pytest.approx(0.75, 1e-6)
    assert metrics["ic"] == pytest.approx(1.0, 1e-6)


def test_oos_r2_can_be_negative():
    # If predictions are worse than baseline mean, OOS R2 must be negative
    y_train = np.array([0.0, 0.0, 0.0])
    y_true = np.array([1.0, -1.0, 0.5])
    y_terrible = np.array([100.0, -100.0, 50.0])

    metrics = compute_oos_metrics(y_true, y_terrible, y_train)
    assert metrics["oos_r2"] < 0.0


def test_holm_bonferroni_correction():
    raw_p = [0.01, 0.04, 0.03]
    # Sorted: 0.01 (m=3 -> *3 = 0.03), 0.03 (m=2 -> *2 = 0.06), 0.04 (m=1 -> *1 = 0.04 -> cummax 0.06)
    adj_p = holm_bonferroni_correction(raw_p)

    assert len(adj_p) == 3
    # Adjusted p-values must be >= raw p-values
    for r, a in zip(raw_p, adj_p):
        assert a >= r
        assert a <= 1.0

    assert adj_p[0] == pytest.approx(0.03, 1e-6)
    assert adj_p[2] == pytest.approx(0.06, 1e-6)
