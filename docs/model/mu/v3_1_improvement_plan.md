# μモデル v3.1 改修計画書 (詳細版)

## 1. はじめに
本ドキュメントは、μモデル v3.0 (Synergy) で確認された過学習の問題を解決し、実運用に耐えうる収益性（ROI > 100%）を確保するための改修計画書です。
第三者開発者が本ドキュメントのみを参照して実装を進められるよう、背景、データ構造、具体的な改修手順を詳細に記述しています。

## 2. 背景と経緯

### 2.1 v3.0 (Synergy) の成果と課題
*   **成果**: Learning to Rank (LGBMRanker) の導入と、シナジー特徴量（騎手×調教師、種牡馬×コース）の実装により、データリークのない状態で学習パイプラインが動作することを確認しました。
*   **課題**:
    *   **過学習**: Validation (2023年) のROIは99%に達しましたが、Test (2024年) では81%に留まりました。
    *   **特徴量の偏り**: 体重関連の特徴量が上位を独占しており、レースの本質的な要因（血統や展開）が十分に活用されていません。

### 2.2 関連ドキュメント
実装にあたっては、以下の既存ドキュメントも参照してください。
*   **[roadmap_v3_synergy.md](../roadmap_v3_synergy.md)**: v3.0の基本コンセプト（Learning to Rank、シナジー特徴量）の定義。
*   **[docs/system/](../system/)**: システム全体の仕様書。特に以下が重要です。
    *   `03_データモデル.md`: データベース構造とカラム定義。
    *   `06_特徴量エンジニアリング.md`: 特徴量生成のロジック。
*   **[parquet_contents_report.md](../../../parquet_contents_report.md)**: 実際のParquetファイルの中身とカラム一覧。

## 3. データ仕様 (Data Specifications)

本モデルで使用する主要なデータソースとカラム定義です。

### 3.1 入力データ (`keibaai/data/parsed/parquet/`)

| ファイル名 | 主要カラム (Key Columns) | 用途 |
| :--- | :--- | :--- |
| **horses_performance_fixed.parquet** | `race_id`, `horse_id`, `race_date`, `finish_position`, `win_odds`, `horse_weight`, `basis_weight` | 学習のメインデータ。過去のレース成績。 |
| **races.parquet** | `race_id`, `venue`, `track_surface`, `distance_m`, `track_condition` | レース環境情報。`horses_performance` にマージして使用。 |
| **pedigrees.parquet** | `horse_id`, `generation`, `ancestor_id` | 血統情報。`sire_id` (父), `damsire_id` (母父) を抽出して使用。 |

### 3.2 ターゲット変数 (Target Variable)
Learning to Rank のための「関連度 (Relevance)」を以下のように定義します。

*   **定義**: `target_relevance = (finish_position == 1) * log(1 + win_odds)`
*   **目的**: 単に勝つだけでなく、「高配当で勝つ」馬を高く評価させるため。
*   **注意**: LightGBM Ranker は整数ラベルを好むため、適宜離散化（例: 0-30のスケール）して入力します。

## 4. 改修戦略 (Improvement Strategy)

v3.1では、以下の3つの柱で過学習を克服します。

### A. 時系列クロスバリデーション (Time-Series Cross-Validation)
**現状**: 2023年のみを検証データとする固定分割。
**改修**: 複数の期間で検証を行う「ローリングウィンドウ方式」に変更します。

| Fold | 学習期間 (Train) | 検証期間 (Valid) | 目的 |
| :--- | :--- | :--- | :--- |
| **Fold 1** | 2020-01 ～ 2021-12 | **2022-01 ～ 2022-12** | 2022年のトレンドへの適合度を確認 |
| **Fold 2** | 2020-01 ～ 2022-12 | **2023-01 ～ 2023-12** | 2023年のトレンドへの適合度を確認 |
| **Fold 3** | 2020-01 ～ 2023-06 | **2023-07 ～ 2023-12** | 直近のトレンドへの適合度を確認 |

*   **実装方針**: `sklearn.model_selection.TimeSeriesSplit` をカスタマイズして実装するか、手動でインデックスを生成してOptunaに渡します。

### B. 厳格な特徴量選抜 (Recursive Feature Elimination)
**現状**: 約170個の特徴量をすべて使用。体重関連が支配的。
**改修**: RFE（再帰的特徴量削減）を用いて、ノイズとなる特徴量を削除します。

1.  **除外対象 (Blacklist)**:
    *   生の `horse_weight`, `basis_weight` (絶対値は馬の個体差が大きく、ノイズになりやすい)。
        *   *残すもの*: `horse_weight_zscore` (相対値), `horse_weight_change` (変化量)。
    *   重要度が著しく低い `past_*` 系の統計量（例: `past_1_passing_order_4_std` など、分散が計算できないもの）。
2.  **選抜プロセス**:
    *   LightGBMで一度学習し、Feature Importance を算出。
    *   下位20%の特徴量を削除して再学習。
    *   これを繰り返し、CVスコアが改善する最適な特徴量セット（目安: 50-80個）を特定する。

### C. 正則化の強化 (Stricter Regularization)
**現状**: モデルが複雑すぎる（過学習）。
**改修**: Optunaの探索空間を以下のように制限します。

```python
params = {
    'num_leaves': trial.suggest_int('num_leaves', 31, 63),       # 以前は256まで探索。小さくして複雑さを抑制。
    'min_child_samples': trial.suggest_int('min_child_samples', 100, 500), # 以前は5から。大きくして「個別の馬」ではなく「パターン」を学習させる。
    'lambda_l1': trial.suggest_float('lambda_l1', 1.0, 10.0, log=True),    # L1正則化の下限を引き上げ。
    'lambda_l2': trial.suggest_float('lambda_l2', 1.0, 10.0, log=True),    # L2正則化の下限を引き上げ。
    'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 0.8), # 特徴量を間引く割合を増やす。
}
```

## 5. 実装手順 (Implementation Steps)

開発者は以下の手順で `scripts/training/train_mu_v3_0_ranker.py` を改修してください。

1.  **特徴量リストの修正**:
    *   `exclude_cols` リストに `horse_weight`, `basis_weight` を追加。
    *   その他、明らかに不要なカラムを除外。
2.  **目的関数 (Objective Function) の書き換え**:
    *   Optunaの `objective` 関数内で、単一の `train/valid` 分割ではなく、上述の **3-Fold Time-Series CV** をループで実行するように変更。
    *   3つのFoldの平均スコア（ROI または NDCG）を `return` するように修正。
3.  **パラメータ探索空間の更新**:
    *   上記の「正則化の強化」に従い、`params` の定義を書き換え。
4.  **実行と検証**:
    *   `python scripts/training/train_mu_v3_0_ranker.py --n_trials 50` を実行。
    *   ログを確認し、Validation ROI と Test ROI の乖離が縮まっていることを確認する。

## 6. 完了条件
*   **汎化性能**: Validation ROI と Test ROI の差が **5ポイント以内** であること。
*   **収益性**: Testデータ (2024年) における Top 1 ROI が **100% を超える** こと（フィルタリング前）。
