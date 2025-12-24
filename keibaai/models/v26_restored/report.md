# Model Report: v2.6 Restored

## Summary
- **Version**: v26_restored
- **Created**: 2025-12-20
- **Description**: V15のROI向上特徴量を移植したv2.6復元版
- **Data**: 2014-01-01 〜 2025-10-31

## Multi-Period Results

| Period | Test ROI | Hit Rate | Gap | Notes |
|--------|----------|----------|-----|-------|
| 2022 | **69.1%** | 19.1% | 43.8% | COVID影響期 |
| 2023 | **78.5%** | 19.9% | 32.8% | 最高ROI |
| 2024 | **75.5%** | 20.6% | 32.0% | 通常年 |
| 2025 | **76.3%** | 19.4% | 29.2% | 最新期 |

### Stability Analysis
- **平均ROI**: 74.9% (±4.0%)
- **範囲**: 69.1% ~ 78.5%
- **安定性**: ✗ 未達 (2022年が75%を下回る)

> 📝 2022年のROIが低い原因はCOVID影響期の特殊な市場環境の可能性あり

## V15 Ported Features

1. **horse_c4_gap_avg**: 過去C4馬身差平均 (非NaN: 83.4%)
2. **horse_relative_c4_avg**: 過去相対C4位置平均
3. **post_style_conflict**: 馬番×脚質不適合スコア (非NaN: 83.4%)
4. **race_front_runner_count**: レース内逃げ予備軍数 (平均: 2.83)
5. **front_runner_competition**: 逃げ競合スコア (平均: 2.63)

## Leak Prevention Measures

- ✅ 累積統計: `expanding().mean().shift(1)`
- ✅ 最小レース数要件: 5走
- ✅ 当該レース情報は使用しない

## Changelog

- **2025-12-20**: v26_restored 初期作成, V15特徴量移植, 複数期間テスト完了
