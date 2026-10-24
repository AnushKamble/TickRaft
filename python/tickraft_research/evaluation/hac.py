"""
Newey-West / HAC robust covariance estimation and lag sensitivity analysis.
"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import statsmodels.api as sm


def fit_ols_with_hac(
    X: np.ndarray,
    y: np.ndarray,
    max_lags: Optional[int] = None,
    feature_names: Optional[List[str]] = None
) -> Dict:
    """
    Fits OLS with Newey-West HAC covariance matrix.
    max_lags defaults to Newey-West rule of thumb: int(4 * (T/100)^(2/9))
    """
    T = len(y)
    if T < 5:
        raise ValueError("Insufficient observations for HAC estimation")

    # Add constant intercept column
    X_with_const = sm.add_constant(X, has_constant='add')

    model = sm.OLS(y, X_with_const)
    results = model.fit()

    if max_lags is None:
        max_lags = max(1, int(4.0 * (float(T) / 100.0) ** (2.0 / 9.0)))

    hac_results = results.get_robustcov_results(cov_type='HAC', maxlags=max_lags)

    names = ["intercept"]
    if feature_names:
        names.extend(feature_names)
    elif X.ndim == 1:
        names.append("feature_0")
    else:
        names.extend([f"feature_{i}" for i in range(X.shape[1])])

    coefs = {}
    for i, name in enumerate(names):
        if i < len(hac_results.params):
            coefs[name] = {
                "coefficient": float(hac_results.params[i]),
                "std_error_hac": float(hac_results.bse[i]),
                "t_stat": float(hac_results.tvalues[i]),
                "p_value": float(hac_results.pvalues[i]),
            }

    return {
        "nobs": int(T),
        "r2_in_sample": float(results.rsquared),
        "max_lags": int(max_lags),
        "coefficients": coefs,
        "aic": float(results.aic),
        "bic": float(results.bic),
    }


def lag_sensitivity_analysis(
    X: np.ndarray,
    y: np.ndarray,
    lags_to_test: List[int] = [1, 2, 5, 10, 20]
) -> List[Dict]:
    """Evaluates how HAC standard errors and p-values change across different lag lengths."""
    sensitivity = []
    T = len(y)
    valid_lags = [lag for lag in lags_to_test if lag < T - 1]
    
    for lag in valid_lags:
        res = fit_ols_with_hac(X, y, max_lags=lag)
        sensitivity.append({
            "lag": lag,
            "coefficients": res["coefficients"]
        })
    return sensitivity
