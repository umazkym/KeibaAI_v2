# 特徴量エンジニア（Feature Engineers）

**最終更新日**: 2026-05-04

---

## 概要

このディレクトリには、レース予測に使用する特徴量を生成するモジュール群が格納されています。
特徴量エンジニアは**継承チェーン**で構成されており、各バージョンは前バージョンを拡張しています。

---

## ステータス分類

### ✅ 本番使用中（Production）

| ファイル | 役割 | 使用箇所 |
|---------|------|---------|
| `leak_free_feature_engineer_v15.py` | **V15モデルの特徴量生成（66特徴量）** | `train_v15.py`, `models/production/` |
| `leak_free_feature_engineer_v15_fixed.py` | **V4.4 Residualモデル・V16/V22のベースクラス** | `train_v44_regularized.py`, V16/V22の親 |
| `leak_free_feature_engineer_v16.py` | **V16モデルの特徴量生成（jw_rank_in_race等追加）** | `train_v16.py`, `train_v4_4.py` |
| `leak_free_feature_engineer_v22.py` | **V22モデル（保険戦略）** | `train_place_model_b.py`, `validate_v23_hybrid.py` |

### 🔗 依存チェーン（本番が依存、削除不可）

V15は以下のチェーンで構築されています。**これらのファイルを削除するとV15が動作しなくなります。**

```
v2 → v3 → v4 → v5 → v6 → v7 → v8 → v10 → v10_1 → v10_2 → v12 → v13 → v14 → v15
                                                                                    └→ v15_fixed → v16 → v17
                                                                                                  └→ v22
```

| ファイル | 依存先 |
|---------|--------|
| `leak_free_feature_engineer_v2.py` | v3の親（最上位基底クラス） |
| `leak_free_feature_engineer_v3.py` | v4の親 |
| `leak_free_feature_engineer_v4.py` | v5の親 |
| `leak_free_feature_engineer_v5.py` | v6の親 |
| `leak_free_feature_engineer_v6.py` | v7の親 |
| `leak_free_feature_engineer_v7.py` | v8の親, dashboard参照 |
| `leak_free_feature_engineer_v8.py` | v9/v91/v10の親 |
| `leak_free_feature_engineer_v10.py` | v10_1の親 |
| `leak_free_feature_engineer_v10_1.py` | v10_2の親 |
| `leak_free_feature_engineer_v10_2.py` | v11/v11_1/v12の親 |
| `leak_free_feature_engineer_v12.py` | v13の親 |
| `leak_free_feature_engineer_v13.py` | v14の親 |
| `leak_free_feature_engineer_v14.py` | **v15, v15_fixedの親** |

### 📦 アーカイブ候補（本番未使用だが依存チェーン上に無い）

以下は本番モデルから直接参照されておらず、かつ依存チェーン上にもないファイルです。
ただし、過去の実験結果の参照用として残しています。

| ファイル | 備考 |
|---------|------|
| `leak_free_feature_engineer_v9.py` | v8の派生実験 |
| `leak_free_feature_engineer_v91.py` | v8の派生実験 |
| `leak_free_feature_engineer_v11.py` | v10_2の派生実験 |
| `leak_free_feature_engineer_v11_1.py` | v10_2の派生実験 |
| `leak_free_feature_engineer_v15_1.py` | v14の派生実験 |
| `leak_free_feature_engineer_v17.py` | v16の派生（V17モデル実験） |
| `leak_free_feature_engineer_v18.py` | v17の派生実験 |
| `leak_free_feature_engineer_v18_1.py` | v17の派生実験 |
| `leak_free_feature_engineer_v19.py` | v17の派生実験 |
| `leak_free_feature_engineer_v20.py` | v15の派生実験 |
| `leak_free_feature_engineer_v21.py` | v15_fixedの派生実験 |
| `leak_free_feature_engineer_v2_fixed.py` | v2の修正版実験 |
| `leak_free_feature_engineer_v72.py` | v7の派生実験 |
| `leak_free_feature_engineer_v30.py` 〜 `v33.py` | v15ベースの実験シリーズ |

### 📄 その他の特徴量モジュール

| ファイル | 役割 |
|---------|------|
| `feature_engine.py` | 旧特徴量エンジン（FeatureEngineクラス） |
| `leak_free_feature_engineer.py` | リークフリー初期版（v2の前身） |
| `advanced_features.py` | 高度な特徴量（feature_engineから参照） |
| `roi_features.py` | ROI最適化用特徴量（SP値簡易計算含む） |
| `mu_v2_features.py` | μモデル専用特徴量 |
| `comprehensive_features.py` | 包括的特徴量セット |
| `hybrid_features.py` | ハイブリッド特徴量 |
| `condition_features.py` | コンディション特徴量 |
| `context_aware_features.py` | コンテキスト認識特徴量 |
| `pace_features.py` | ペース関連特徴量 |
| `payout_features.py` | 配当関連特徴量 |
| `time_feature_engineer_v1.py` 〜 `v4.py` | 時間予測用特徴量 |
| `time_margin_features.py` | タイム差特徴量 |
| `time_series_features.py` | 時系列特徴量 |
| `track_bias.py` / `track_bias_features.py` | トラックバイアス |
| `report_metrics.py` | レポート用メトリクス |

---

## 命名規則

- `leak_free_feature_engineer_v{N}.py`: リークフリー特徴量エンジニア（Nはバージョン番号）
- バージョン番号は継承順序を示す（大きいほど新しい）
- `_fixed` サフィックス: 特定の問題を修正したバリアント
- `_1` サフィックス: マイナーバリアント
