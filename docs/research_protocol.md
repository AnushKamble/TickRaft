# TickRaft Microstructure Research Protocol

This document specifies the empirical research methodology, feature definitions, target variables, statistical inference, and validation protocols used in TickRaft.

---

## 1. Research Questions & Hypotheses

- **Q1:** Does a documented depth-aware multi-level OFI extension improve out-of-sample mid-price change predictions over Level-1 OFI and Trade-Volume Imbalance (TVI) baselines?
- **Q2:** How do predictive and explanatory metrics vary across forward prediction horizons ($h \in \{1, 5, 10, 30, 60\}$ seconds)?
- **Q3:** Are apparent gains statistically robust after controlling for serial autocorrelation via Newey-West HAC covariance and multi-testing corrections?

---

## 2. Feature Definitions

### 2.1 Trade-Volume Imbalance (TVI)
Uses official LOBSTER event semantics:
- `direction` in LOBSTER denotes the side of the resting limit order (+1 = Buy, -1 = Sell).
- For visible executions (type 4) and hidden executions (type 5), the aggressor who executed against the resting order was on the opposite side:
  $$\text{AggressorSign} = -\text{Direction}$$
  $$\text{TVI}_t = \begin{cases} \text{AggressorSign} \times \text{Size}_t & \text{if EventType} \in \{4, 5\} \\ 0 & \text{otherwise} \end{cases}$$

### 2.2 Level-1 OFI (Cont, Kukanov & Stoikov 2014)
Accounts for both price and size movements at the best quotes:
$$\text{OFI}_t^{(1)} = I_t^{bid} + I_t^{ask}$$
Where:
$$I_t^{bid} = \begin{cases}
q_t^{bid} & \text{if } P_t^{bid} > P_{t-1}^{bid} \\
q_t^{bid} - q_{t-1}^{bid} & \text{if } P_t^{bid} = P_{t-1}^{bid} \\
-q_{t-1}^{bid} & \text{if } P_t^{bid} < P_{t-1}^{bid}
\end{cases}$$

$$I_t^{ask} = \begin{cases}
q_{t-1}^{ask} & \text{if } P_t^{ask} > P_{t-1}^{ask} \\
-(q_t^{ask} - q_{t-1}^{ask}) & \text{if } P_t^{ask} = P_{t-1}^{ask} \\
-q_t^{ask} & \text{if } P_t^{ask} < P_{t-1}^{ask}
\end{cases}$$
Row 0 has no prior state and is left undefined (null).

### 2.3 Depth-Aware Multi-Level OFI Extension
Computes the vector of flow features across levels $k = 1 \dots L$ (primary $L=5$):
$$\mathbf{OFI}_t = [\text{OFI}_t^{(1)}, \text{OFI}_t^{(2)}, \dots, \text{OFI}_t^{(L)}]^T$$
*Note on Rank Shifts:* When a quote at level 1 changes, the price associated with level 2 may shift. This vector is treated as a project-defined depth extension.

---

## 3. Regular Grid Aggregation & Targets

1. **Regular 1-Second Grid:**
   - Flow features ($\text{OFI}, \text{TVI}$) are summed within each integer second interval $[t, t+1)$.
   - Mid-price is the last observed mid-price at bucket end. Empty buckets forward-fill the mid-price and have zero flow sums.
2. **Causal Forward Targets:**
   $$Y_{t+h} = \text{mid}_{t+h} - \text{mid}_t$$
   For horizons $h \in \{1, 5, 10, 30, 60\}$ seconds. Rows near session boundaries where $t+h$ exceeds the available session are purged.

---

## 4. Evaluation & Statistical Inference

1. **Chronological Train/Test Split:**
   - 70% train, 30% holdout test.
   - Never randomly split high-frequency time series rows.
2. **Purged Horizon Embargo:**
   - An embargo period equal to at least the maximum target horizon ($\ge 60$ seconds) is dropped between train and test partitions to guarantee no target overlap leaks into test features.
3. **Newey-West / HAC Robust Covariance:**
   - Standard OLS standard errors underestimate variance due to serial correlation induced by overlapping targets.
   - Fits OLS with Newey-West HAC covariance with bandwidth $L = \text{int}(4 \times (T/100)^{2/9})$ and lag sensitivity analysis.
4. **Multiple Testing Correction:**
   - Holm-Bonferroni step-down procedure controls the family-wise error rate across multiple horizon hypothesis tests.
