# μ Model v3.0 (Synergy) Performance Analysis

## 1. Overview
The training of μ Model v3.0 has been completed with the data leakage issue resolved. The results indicate a realistic model performance, but the Return on Investment (ROI) on the test data (2024) is currently insufficient for profitability.

## 2. Key Metrics
| Metric | Value | Notes |
| :--- | :--- | :--- |
| **Top 1 Accuracy** | **22.19%** | Realistic range (vs 100% leakage). Comparable to standard models. |
| **Top 1 ROI (Test)** | **80.90%** | **Negative**. Significant drop from Validation ROI. |
| **Best ROI (Valid)** | **99.09%** | Achieved in Optuna Trial 41 (2023 data). |
| **Top 5 Recall** | **69.88%** | ~70% of winners are captured in the top 5. |

## 3. Feature Importance Analysis
The top 20 features show a mix of physical attributes, past performance, and the new synergy features.

*   **Dominant Features (Weight)**: `basis_weight_zscore`, `horse_weight_diff_from_avg`, `horse_weight_zscore`, `horse_weight` occupy the top ranks. This suggests the model relies heavily on physical condition relative to the field.
*   **Synergy Features (Validated)**:
    *   `nicks_avg_finish` (Rank 14)
    *   `sire_course_avg_finish` (Rank 16)
    *   `combo_overperform` (Rank 18)
    *   `sire_avg_finish` (Rank 19)
    *   *Conclusion*: The new synergy features are being utilized by the model and are contributing to predictions.
*   **Other Key Features**: `age_zscore`, `bms_avg_finish`, `pace_fit_score` (Rank 10).

## 4. Issues & Hypotheses
### A. Validation-Test Gap (Overfitting or Drift)
*   **Valid ROI (99%) vs Test ROI (81%)**: There is a large discrepancy. The model optimized for 2023 data (Validation) did not generalize well to 2024 (Test).
*   *Hypothesis*: The market efficiency or race trends might have shifted in 2024, or the model is overfitting to specific patterns in the training/valid set.

### B. Feature Redundancy
*   Multiple weight-related features are in the top 5. This might be introducing noise or diluting the impact of other subtle signals like pedigree or synergy.

### C. Naive Betting Strategy
*   The evaluation assumes betting on *every* Top 1 prediction. A profitable model usually requires a **Confidence Threshold** (betting only when the predicted score or probability is high) or an **Odds Threshold** (avoiding over-betting on favorites with low returns).

## 5. Proposed Next Steps (Refinement Plan)

### Phase 1: Betting Strategy Optimization (Immediate)
Instead of retraining immediately, we should analyze if a profitable sub-segment exists.
*   **Action**: Evaluate ROI with a **Confidence Threshold**.
*   **Action**: Evaluate ROI filtering by **Odds** (e.g., avoid < 2.0 or > 50.0).

### Phase 2: Feature Selection & Retraining
*   **Action**: Remove redundant weight features (keep only `zscore` or `diff`).
*   **Action**: Re-run Optuna with a stricter regularization (higher `lambda_l1`, `lambda_l2`) to reduce overfitting.

### Phase 3: Loss Function Adjustment
*   **Action**: The current `target_relevance` is `(gain * 2).astype(int)`. We might need to weigh "winning" more heavily than just the odds return, or use a custom objective function that directly optimizes ROI.

## 6. Conclusion
The v3.0 model is functionally correct (no leakage, features working), but financially immature. The immediate focus should shift from "Feature Implementation" to "Strategy Optimization" and "Model Tuning".
