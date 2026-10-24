"""
Tests for Newey-West HAC standard error estimation and lag sensitivity.
"""

import numpy as np
import pytest
from tickraft_research.evaluation.hac import fit_ols_with_hac, lag_sensitivity_analysis


def test_fit_ols_with_hac():
    np.random.seed(42)
    n = 100
    X = np.random.randn(n, 1)
    # y with autoregressive error
    eps = np.zeros(n)
    for i in range(1, n):
        eps[i] = 0.5 * eps[i - 1] + np.random.randn()
    y = 2.0 + 1.5 * X[:, 0] + eps

    res = fit_ols_with_hac(X, y, max_lags=4, feature_names=["flow_signal"])

    assert res["nobs"] == n
    assert "coefficients" in res
    assert "intercept" in res["coefficients"]
    assert "flow_signal" in res["coefficients"]

    coef_info = res["coefficients"]["flow_signal"]
    assert coef_info["coefficient"] > 0  # True coefficient was 1.5
    assert coef_info["std_error_hac"] > 0
    assert 0 <= coef_info["p_value"] <= 1.0


def test_lag_sensitivity_analysis():
    np.random.seed(42)
    n = 80
    X = np.random.randn(n, 1)
    y = 1.0 + 0.8 * X[:, 0] + np.random.randn(n)

    lags = [1, 2, 5]
    sensitivity = lag_sensitivity_analysis(X, y, lags_to_test=lags)

    assert len(sensitivity) == len(lags)
    for entry in sensitivity:
        assert entry["lag"] in lags
        assert "coefficients" in entry
