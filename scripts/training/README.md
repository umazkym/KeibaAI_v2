# 訓練・予測スクリプト（scripts/training/）

**最終更新日**: 2026-05-04

---

## 概要

モデルの訓練、評価、予測に使用するスクリプト群です。
本番で使用するスクリプトのみをルートに配置し、過去の実験スクリプトは `_archive/` に格納しています。

---

## 本番スクリプト

### モデル訓練

| スクリプト | 説明 | 出力先 |
|-----------|------|--------|
| `train_v15.py` | **V15 Binaryモデル訓練** | `keibaai/models/v15/` |
| `train_v16.py` | V16モデル訓練 (jw_rank_in_race等) | `keibaai/models/v16/` |
| `train_v4_4.py` | V4.4 LambdaRankモデル訓練 | `keibaai/models/mu_v4_4/` |
| `train_v15_v44_improved.py` | V15+V4.4 Ensemble統合訓練 | `models/production/` |
| `train_v15_v44_improved_v2.py` | V15+V4.4 Ensemble v2 | `models/production/` |
| `train_sigma_v2.py` | σモデル (不確実性推定) | `keibaai/models/sigma_v2/` |
| `train_nu_v2.py` | νモデル (混沌度推定) | `keibaai/models/nu_v2/` |
| `train_mu_model.py` | μモデル (時間予測) | `keibaai/models/mu_*/` |
| `train_mu_v2_model.py` | μv2モデル訓練 | `keibaai/models/mu_v2/` |

### 推論・評価

| スクリプト | 説明 |
|-----------|------|
| `predict.py` | μ/σ/ν推論パイプライン |
| `predict_bulk.py` | 一括推論 |
| `evaluate_model.py` | モデル評価 (ROI, Hit Rate等) |
| `evaluate_model_advanced.py` | 高度な評価 (セグメント別等) |

### 検証・ツール

| スクリプト | 説明 |
|-----------|------|
| `check_data_leakage.py` | データリーク検出 |
| `walkforward_validation.py` | Walk-forward検証 |
| `walkforward_v15.py` | V15 Walk-forward検証 |
| `feature_selection.py` | 特徴量選択 |

---

## _archive/ について

過去の実験・検証で作成された約260スクリプトが格納されています。
主な系統:
- `train_mu_v*`: μモデルの各バージョン訓練
- `train_v*`: Binary分類モデルの各バージョン訓練
- `validate_*`: 各種検証スクリプト
- `verify_*`: 結果検証スクリプト
- `analyze_*`: 分析スクリプト
- `compare_*`: モデル比較スクリプト
- `ensemble_*`: アンサンブル実験
- `optimize_*`: ハイパーパラメータ最適化

## 命名規則

- `train_{model}.py`: モデル訓練スクリプト
- `predict*.py`: 推論スクリプト
- `evaluate_*.py`: 評価スクリプト
- `walkforward_*.py`: Walk-forward検証
- `validate_*.py`: 検証スクリプト
- `verify_*.py`: 結果確認スクリプト
