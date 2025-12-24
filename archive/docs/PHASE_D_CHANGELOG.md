# Phase D: ROI向上のための特徴量拡張

**実施日**: 2025-11-22
**目的**: ROI 62.69% → 85~110% への向上
**バージョン**: v2.0 → v2.1

---

## 📊 変更概要

Phase Dでは、**実装済みだが未使用だった高度な特徴量3カテゴリ**を有効化しました。

### 🆕 新規追加特徴量

#### 1. コース適性特徴量 (`generate_course_affinity_features`)
**追加される特徴量（推定20~30個）**:
- `venue_avg_finish`: 競馬場別の平均着順
- `venue_races`: 競馬場別の出走回数
- `venue_avg_odds`: 競馬場別の平均オッズ
- `dist_avg_finish`: 距離カテゴリ別の平均着順
- `dist_races`: 距離カテゴリ別の出走回数
- `surface_avg_finish`: 馬場別（芝/ダート）の平均着順
- `surface_races`: 馬場別の出走回数

**期待効果**:
- 特定の競馬場・距離・馬場が得意な馬を正確に評価
- 「東京芝2400mが得意だが中山は苦手」といったパターンを学習

---

#### 2. レース条件特徴量 (`generate_race_condition_features`)
**追加される特徴量（推定5~10個）**:
- `field_size_category`: フィールドサイズ（small/medium/large/extra_large）
- `race_month`: レース月（1~12）
- `race_season`: 季節（winter/spring/summer/autumn）
- `race_importance`: レース重要度（high/medium/low、賞金ベース）

**期待効果**:
- 少頭数レースでの展開予想の精度向上
- 季節性（夏場の馬体重減少など）を考慮
- 重賞レースと条件戦の違いを学習

---

#### 3. 相対指標 (`calculate_relative_metrics`)
**追加される特徴量（推定10~15個）**:
- `time_deviation`: タイムの偏差値（レース内で50±10）
- `last3f_diff_from_best`: 上がり3Fのベストタイムとの差
- `odds_rank`: オッズの順位（morning_odds使用でデータリーク防止）
- `weight_diff_from_avg`: 斤量の平均との差

**期待効果**:
- レース内での相対的な強さを正確に評価
- 「絶対的には遅いが、そのレースでは速かった」を判定

---

## 📁 変更ファイル

### 1. `keibaai/src/features/feature_engine.py`
```diff
+ # 6. コース適性特徴量 (競馬場別・距離別・馬場別成績)
+ logging.info("コース適性特徴量を生成中...")
+ df = adv_engine.generate_course_affinity_features(df, results_history_df)

+ # 7. レース条件特徴量 (フィールドサイズ・季節性・レース重要度)
+ logging.info("レース条件特徴量を生成中...")
+ df = adv_engine.generate_race_condition_features(df)

+ # 8. 相対指標 (タイム偏差値・上がり3F相対値・オッズ順位)
+ logging.info("レース内相対指標を生成中...")
+ df = adv_engine.calculate_relative_metrics(df)
```

### 2. `keibaai/configs/features.yaml`
```diff
- version: "v2.0"
+ version: "v2.1" # Phase D: コース適性・レース条件・相対指標を追加
```

### 3. `keibaai/configs/models.yaml`
```diff
- feature_version: v1.0
+ feature_version: v2.1  # Phase D: 高度な特徴量を使用
```

---

## 🎯 期待される成果

| 項目 | 変更前 | 変更後（予測） |
|------|--------|----------------|
| **特徴量数** | 160個 | 210~230個 |
| **ROI (2024年)** | 62.69% | 85~110% |
| **的中率** | 不明 | 維持または微増 |
| **予測精度** | ベースライン | +10~20% |

---

## 🚀 次のステップ

### Phase D-2: 特徴量再生成
```bash
python keibaai/src/features/generate_features.py \
  --start_date 2020-01-01 \
  --end_date 2025-10-01
```

### Phase D-3: モデル再学習
```bash
python keibaai/src/models/train_mu_model.py \
  --start_date 2020-01-01 \
  --end_date 2023-12-31 \
  --output_dir keibaai/data/models/mu_v2.1_phase_d
```

### Phase D-4: バックテスト
```bash
python keibaai/src/models/evaluate_model.py \
  --model_dir keibaai/data/models/mu_v2.1_phase_d \
  --start_date 2024-01-01 \
  --end_date 2024-12-31
```

---

## 📝 注意事項

### データリーク防止
- `calculate_relative_metrics`では`morning_odds`を使用し、`win_odds`は使用しない
- レース内相対指標は、レース結果データ（`finish_time_seconds`など）を使わない

### パフォーマンス
- 特徴量数が増加するため、学習時間が10~30%増加する可能性
- メモリ使用量も増加（推定 +20~40%）
- 必要に応じてバッチサイズやサンプリングを調整

---

## 🔬 今後の改善候補（Phase E以降）

1. **アンサンブル比率の最適化**: Ranker:Regressor = 0.5:0.5 を Optuna で最適化
2. **特徴量重要度分析**: 無効な特徴量の削除による過学習防止
3. **σ/νモデルの再学習**: 新特徴量を使った分散・荒れ度予測の向上
4. **評価ロジックの見直し**: `idxmin()` vs `idxmax()` の最適化

---

**実装者**: Claude (Anthropic)
**レビュワー**: @umazkym
**承認日**: 未定
