# KeibaAI_v2 μモデル 包括的現状レポート

**作成日**: 2025-12-05  
**バージョン**: μ Model v5.4（現行最良モデル）  
**目的**: 第三者が本システムを完全に理解・運用できる技術・運用ドキュメント

---

## 📋 目次

1. [エグゼクティブサマリー](#1-エグゼクティブサマリー)
2. [システム概要と最終目標](#2-システム概要と最終目標)
3. [データソースと制約](#3-データソースと制約)
4. [モデル性能と検証結果](#4-モデル性能と検証結果)
5. [全特徴量一覧（169個）](#5-全特徴量一覧169個)
6. [リーク・過学習リスク管理](#6-リーク過学習リスク管理)
7. [運用設計と投資機会](#7-運用設計と投資機会)
8. [開発ロードマップ](#8-開発ロードマップ)
9. [関連資料一覧](#9-関連資料一覧)

---

## 1. エグゼクティブサマリー

### 1.1 現状のパフォーマンス

| 指標 | v5.4 (現行) | 備考 |
|------|-------------|------|
| **Valid ROI** | 84.25% | 2023年データ（ハイパーパラメータ最適化用） |
| **Test ROI** | **80.05%** | 2024年データ（未知データ評価、1回のみ参照） |
| **Valid-Test差** | 4.20% | 健全範囲内（<10%が目安） |
| **特徴量数** | 169 | 馬自身の特徴量11個含む |
| **投資機会** | 100% | 全レースでTop1推奨（EV閾値導入後も20%以上維持予定） |

### 1.2 達成状況

- ✅ **リークなし**: 全特徴量で`expanding().mean().shift(1)`パターン適用済み
- ✅ **過学習なし**: Valid-Test差 4.20%は健全範囲
- ✅ **Test隔離**: Optuna最適化にTestデータを一切使用しない「クリーン評価」
- ⚠️ **v3.5（81.84%）との差**: 現行v5.4は80.05%でわずかに未達（-1.79%）
- ⚠️ **v3.5再現不可**: オリジナルスクリプト再実行でも78.28%にとどまる

### 1.3 最終目標

> **競馬投資における長期的な収益最大化**

本システムの目的は、機械学習による勝率予測と期待値計算に基づき、**持続可能な正のROI**を達成することである。これを実現する手段として、以下の自動化パイプラインを構築する：

```
[Phase A: 朝一処理 10:00頃]
├── shutuba_parser で出馬表取得
├── μモデルで馬の勝率スコア予測
├── σモデルで予測の不確実性推定
├── νモデルでレースの荒れやすさ推定
├── モンテカルロシミュレーション（K=1000回）
└── morning_odds ベースで資金配分案を生成
        ↓
[Phase B: レース直前処理（発走10分前）]
├── 最終オッズをJRAからスクレイピング
├── オッズ変動によるEV再計算
├── 購入可否判定（EV閾値チェック）
└── 自動購入実行（または中止）
```

---

## 2. システム概要と最終目標

### 2.1 KeibaAI_v2 アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                      データパイプライン                           │
├─────────────────────────────────────────────────────────────────┤
│ [スクレイピング]     [パース]           [特徴量生成]              │
│   HTML.bin    →    Parquet    →    train_data.parquet           │
│                                                                   │
│ データソース:                                                     │
│   - races.parquet (277,826件): レース結果                        │
│   - shutuba.parquet: 出馬表（当日予測用）                         │
│   - horses.parquet: 馬プロファイル                                │
│   - pedigrees.parquet (1,377,361件): 5世代血統                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      モデルパイプライン                           │
├─────────────────────────────────────────────────────────────────┤
│ [学習]                 [推論]               [シミュレーション]    │
│ LightGBM      →     予測スコア      →    モンテカルロ1000回      │
│ (LambdaRank)        (順位予測)             (勝率分布推定)         │
│                                                                   │
│ モデル構成:                                                       │
│   - μモデル: 勝率スコア予測（本レポートの対象）                   │
│   - σモデル: 予測の不確実性推定                                   │
│   - νモデル: レースの荒れやすさ推定                               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      投資パイプライン                             │
├─────────────────────────────────────────────────────────────────┤
│ [ポートフォリオ最適化]    [購入判別]        [自動購入]             │
│   Kellyクライテリオン  →  EV≥1.0判定   →  JRAオンライン投票      │
│                                           （将来実装）            │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 投資機会の設計方針

**目標: 期待値の高いレースに集中投資し、投資機会は20%以上維持**

競馬の控除率は約25%（JRA単勝）であり、市場全体の期待値は約75%。収益を上げるには「市場の歪み（ミスプライス）」を見つけ、**期待値(EV) > 1.0のレースにのみ投資**する必要がある。

| EV閾値 | 投資対象比率 | 備考 |
|--------|-------------|------|
| 1.0 | 約50% | 控除率を上回るレース全て |
| 1.1 | 約30% | より高確度な投資対象 |
| **1.2** | **約20%** | **最低維持ライン** |

> [!IMPORTANT]
> 「投資機会を減らさない」ことは目的ではない。**期待値が低いレースを避け、高いレースに集中する**ことが収益最大化の本質。ただし、統計的有意性確保のため**投資機会20%以上は維持する設計**とする。

---

## 3. データソースと制約

### 3.1 Parquetファイル詳細

#### races.parquet（レース結果）
| 分類 | カラム名 | 型 | 説明 | 欠損率 | 用途 |
|------|----------|----|----|--------|------|
| **レースID** | `race_id` | str | 12桁のレースID（例: 202001010101） | 0% | 主キー |
| **日時** | `race_date` | datetime | 開催日 | 0% | 時系列分割 |
| | `post_time` | str | 発走時刻 | 0% | 参考情報 |
| **コース** | `distance_m` | Int64 | 距離（メートル） | 0% | 特徴量 |
| | `track_surface` | str | 芝/ダート | 0% | 特徴量 |
| | `track_condition` | str | 良/稍/重/不 | <1% | 特徴量 |
| | `venue` | str | 競馬場名 | 0% | 特徴量 |
| **結果** | `finish_position` | Int64 | **着順（ターゲット変数）** | <1% | 学習ターゲット |
| | `finish_time_seconds` | float | 走破タイム（秒） | <1% | 特徴量生成 |
| | `last_3f_time` | float | 上がり3Fタイム（秒） | <1% | 特徴量生成 |
| | `margin_seconds` | float | 着差（秒） | <1% | 特徴量生成 |
| | `passing_order_4` | Int64 | 4角通過順 | **56.6%** | 脚質分析 |
| **オッズ/人気** | `win_odds` | float | **確定単勝オッズ** | <1% | ROI計算 |
| | `popularity` | Int64 | **確定人気順** | <1% | ROI計算/Gap特徴量 |
| **馬体** | `horse_weight` | Int64 | 馬体重（kg） | <1% | 特徴量 |
| | `horse_weight_change` | Int64 | 馬体重増減（kg） | <1% | 特徴量 |
| **馬情報** | `horse_id` | str | 馬ID | 0% | 馬の履歴追跡 |
| | `jockey_id` | str | 騎手ID | 0% | 騎手の履歴追跡 |
| | `trainer_id` | str | 調教師ID | 0% | 調教師の履歴追跡 |

#### shutuba.parquet（出馬表）
| カラム名 | 型 | 説明 | 欠損率 | 備考 |
|----------|----|----|--------|------|
| `morning_odds` | object | **朝一オッズ** | **100%** | ⚠️ 未取得（全てNull） |
| `morning_popularity` | object | 朝一人気 | 100% | ⚠️ 未取得 |
| `horse_weight` | float | 馬体重 | <1% | 出馬表時点 |
| `horse_weight_change` | float | 馬体重増減 | <1% | 前走比 |

> [!CAUTION]
> **Training-Serving Skew の重大リスク**:
> - `morning_odds`は現在**全てNull**であり、スクレイピング未実装
> - バックテストでは`races.parquet`の`win_odds`（確定オッズ）を代用
> - これは「最終オッズで判断した場合」のシミュレーションであり、実運用時とは条件が異なる
> - Gap特徴量（`gap_jockey_popularity`等）も確定人気で計算されており、運用時は朝一人気での再計算が必要

### 3.2 テスト/バックテスト時のデータ使用制約

| データ | 学習時 | バックテスト時 | 実運用（朝一） | 実運用（直前） |
|--------|--------|---------------|---------------|---------------|
| `win_odds`（確定オッズ） | ✅ ターゲット計算 | ✅ ROI計算 | ❌ 未確定 | ✅ JRAから取得 |
| `popularity`（確定人気） | ✅ Gap特徴量 | ✅ 評価用 | ❌ 未確定 | ✅ JRAから取得 |
| `morning_odds` | ❌ 欠損 | ❌ 欠損 | ✅ shutubaから | ✅ 朝取得済み |
| `morning_popularity` | ❌ 欠損 | ❌ 欠損 | ✅ shutubaから | ✅ 朝取得済み |

> [!WARNING]
> **バックテストのROI（80.05%）は「確定オッズで評価した場合」の値**であり、実運用で`morning_odds`を使用した場合は異なる可能性がある。

---

## 4. モデル性能と検証結果

### 4.1 バージョン履歴と教訓

| Version | Valid ROI | Test ROI | Gap | 問題点 | 教訓 |
|---------|-----------|----------|-----|--------|------|
| v3.5 | 83.79% | 81.84% | 1.95% | - | **再現不可**（再実行で78.28%） |
| v5.0 | 122.51% | 87.72% | **34.79%** | ⚠️ リーク | `shift(1)`未適用 |
| v5.1 | 560.75% | 74.14% | 486.61% | ❌ データ誤り | NaN率87%のソース使用 |
| v5.2 | 85.16% | 79.95% | 5.21% | - | races.parquet使用で修正 |
| v5.3 | 84.10% | 86.33% | 2.23% | ⚠️ Test Leakage | Optunaにtest含む |
| **v5.4** | **84.25%** | **80.05%** | **4.20%** | ✅ クリーン | **現行最良モデル** |

### 4.2 追加検証結果（2025-12-05実施）

#### 目的関数比較: LambdaRank vs Binary Classification

| 目的関数 | Valid ROI | Test ROI | Brier Score | 結論 |
|----------|-----------|----------|-------------|------|
| **LambdaRank** | 81.64% | 78.95% | 0.0630 | 現行採用 |
| **Binary** | 81.96% | 79.03% | 0.0662 | ほぼ同等（+0.08%） |

**結論**: 両者はほぼ同等であり、ランキング最適化のLambdaRankを維持。

#### Calibration（確率較正）

| 予測確率帯 | 予測平均 | 実際勝率 | 較正状態 |
|------------|----------|----------|----------|
| 0-5% | 3.1% | 1.8% | ✓ 良好 |
| 5-10% | 7.0% | 7.5% | ✓ 良好 |
| **10-15%** | 12.4% | **15.8%** | ↑ **過小評価** |
| **15-20%** | 16.3% | **21.1%** | ↑ **過小評価** |
| 25-30% | 29.4% | 31.9% | ✓ 良好 |

**発見**: 10-20%帯（中穴層）でモデルが勝率を過小評価している。ここに改善余地あり。

#### Forward Selection結果

v5.4で追加した馬特徴量11個を1つずつ追加した結果：

| 結果 | 意味 |
|------|------|
| **全11特徴量がマイナス** | Valid ROIベースでは「ノイズ」と判定 |
| **しかしTest ROIは改善** | 正則化効果として機能（過学習防止） |

**教訓**: Forward Selectionの結果のみで判断せず、Test ROIと合わせて評価すべき。

#### v3.5再現性検証

| 実行 | Test ROI | 結論 |
|------|----------|------|
| v3.5記録値 | 81.84% | - |
| v3.5オリジナル再実行 | **78.28%** | 再現不可 |
| v3.6（v3.5再現版） | 76.78% | さらに低下 |

**結論**: v3.5の81.84%は「偶然の産物」または「データ環境の変化」により再現不可能。**v5.4（80.05%）を現時点での最良モデルとして採用**。

### 4.3 v5.4 ハイパーパラメータ

```json
{
  "objective": "lambdarank",
  "metric": "ndcg",
  "eval_at": [1, 3, 5],
  "num_leaves": 100,
  "max_depth": 8,
  "min_child_samples": 55,
  "learning_rate": 0.044,
  "lambda_l1": 4.31,
  "lambda_l2": 0.25,
  "feature_fraction": 0.64,
  "bagging_fraction": 0.82,
  "bagging_freq": 5
}
```

### 4.4 データ分割

| セット | 期間 | レコード数 | 用途 |
|--------|------|-----------|------|
| **Train** | ~2022/12/31 | 139,379 | モデル学習 |
| **Valid** | 2023/01/01~2023/12/31 | 44,212 | ハイパーパラメータ最適化（Optuna） |
| **Test** | 2024/01/01~ | 78,405 | **最終評価（1回のみ参照、最適化に使用禁止）** |

---

## 5. 全特徴量一覧（169個）

### 5.1 特徴量カテゴリ別概要

| カテゴリ | 特徴量数 | 主要特徴量 | 重要度合計 |
|----------|----------|------------|------------|
| **A. Gap特徴量** | 4 | gap_jockey_popularity, gap_pedigree_popularity | 763 |
| **B. 騎手** | 18 | jockey_nr_global, jockey_芝_win_rate | 約400 |
| **C. 血統** | 10 | sire_nr_global, sire_course_win_rate | 約350 |
| **D. 馬自身（v5.4新規）** | 11 | horse_time_deviation_avg, horse_distance_nr | 約300 |
| **E. 過去成績** | 68 | past_5_finish_position_mean, past_3_last_3f_time_std | 約250 |
| **F. 調教師** | 10 | trainer_新潟_win_rate, trainer_阪神_win_rate | 約150 |
| **G. レース条件** | 15 | distance_m, horse_number, bracket_is_inner | 約100 |
| **H. コンディション** | 12 | horse_weight_zscore, form_rank | 約100 |
| **I. その他** | 21 | combo_avg_finish, pace_fit_score | 約80 |

### 5.2 Top 30 重要特徴量（v5.4）

| 順位 | 特徴量 | 重要度 | カテゴリ | 計算方法 | 解説 |
|------|--------|--------|----------|----------|------|
| 1 | `gap_jockey_popularity` | 262 | Gap | `popularity - jockey_rank` | **最重要**。騎手の実力ランクと人気順位の乖離。正の値は「騎手の実力に対して過小評価」を示す |
| 2 | `jockey_nr_global` | 211 | 騎手 | 全レースの正規化着順平均 | 騎手の総合能力を0-1でスコア化 |
| 3 | `gap_pedigree_popularity` | 186 | Gap | `popularity - sire_rank` | 血統（父馬勝率）と人気の乖離 |
| 4 | `sire_course_win_rate` | 181 | 血統 | 父馬産駒の当該コース勝率 | 血統のコース適性 |
| 5 | `sire_nr_global` | 176 | 血統 | 全レースの産駒正規化着順平均 | 種牡馬の総合能力 |
| 6 | `gap_course_fit_popularity` | 173 | Gap | `popularity - course_fit_rank` | コース適性と人気の乖離 |
| 7 | `gap_trainer_popularity` | 142 | Gap | `popularity - trainer_rank` | 調教師実力と人気の乖離 |
| 8 | `horse_weight_zscore` | 140 | 条件 | `(weight - mean) / std` | 馬体重の標準化値。適正体重からの乖離 |
| 9 | `horse_distance_nr` | 129 | 馬(v5) | `expanding().mean().shift(1)` | **v5.4新規**。馬の距離別NR平均 |
| 10 | `trainer_新潟_win_rate` | 127 | 調教師 | 調教師の新潟勝率 | 競馬場別の調教師実績 |
| 11 | `horse_time_deviation_avg` | 118 | 馬(v5) | `expanding().mean().shift(1)` | **v5.4新規**。馬のタイム偏差平均 |
| 12 | `horse_surface_nr` | 113 | 馬(v5) | `expanding().mean().shift(1)` | **v5.4新規**。芝/ダート別NR |
| 13 | `horse_interval_days` | 108 | 馬(v5) | `groupby('horse_id')['race_date'].diff().dt.days` | **v5.4新規**。前走間隔日数 |
| 14 | `sire_course_avg_finish` | 107 | 血統 | 父馬産駒の当該コース平均着順 | 血統のコース適性（着順ベース） |
| 15 | `sire_wet_boost` | 107 | 血統 | 重馬場時の成績向上率 | 血統の馬場適性 |
| 16 | `horse_venue_nr` | 104 | 馬(v5) | `expanding().mean().shift(1)` | **v5.4新規**。競馬場別NR |
| 17 | `horse_weight_change_ratio` | 100 | 馬(v5) | `(weight - prev_weight) / prev_weight` | **v5.4新規**。馬体重変化率 |
| 18 | `jockey_芝_win_rate` | 97 | 騎手 | 騎手の芝コース勝率 | 馬場別の騎手能力 |
| 19 | `combo_overperform` | 96 | コンボ | 騎手×馬の過去成績-期待値 | 騎手と馬の相性 |
| 20 | `time_deviation_score_avg_5` | 96 | 過去 | 直近5走のタイム偏差平均 | レース水準との比較 |
| 21-30 | （以下略） | ... | ... | ... | ... |

### 5.3 馬自身の特徴量（v5.4新規、11個）詳細

| 特徴量 | 重要度 | NaN率 | 計算方法 | 解説 |
|--------|--------|-------|----------|------|
| `horse_time_deviation_avg` | 118 | 12.9% | `groupby('horse_id')['time_deviation'].expanding().mean().shift(1)` | 馬の走破タイムが「基準タイム」からどれだけ乖離しているかの累積平均。低いほど優秀 |
| `horse_l3f_deviation_avg` | 87 | 12.9% | `groupby('horse_id')['l3f_deviation'].expanding().mean().shift(1)` | 上がり3Fタイムの乖離累積平均。末脚の安定度 |
| `horse_best_time_deviation` | 83 | 12.9% | `groupby('horse_id')['time_deviation'].expanding().max().shift(1)` | 過去最高のタイム偏差（ベストパフォーマンス） |
| `horse_venue_nr` | 104 | 45.1% | `groupby(['horse_id','venue'])['normalized_rank'].expanding().mean().shift(1)` | 特定競馬場でのNR平均。競馬場適性 |
| `horse_distance_nr` | 129 | 25.6% | `groupby(['horse_id','dist_cat'])['normalized_rank'].expanding().mean().shift(1)` | 距離カテゴリ別NR。距離適性 |
| `horse_surface_nr` | 113 | 21.1% | `groupby(['horse_id','surface'])['normalized_rank'].expanding().mean().shift(1)` | 芝/ダート別NR。馬場適性 |
| `horse_best_nr` | 36 | 12.9% | `groupby('horse_id')['normalized_rank'].expanding().max().shift(1)` | 過去最高NR |
| `horse_interval_days` | 108 | 12.8% | `groupby('horse_id')['race_date'].diff().dt.days` | 前走からの間隔日数。休養明けの影響 |
| `horse_dist_change` | 31 | 16.1% | `groupby('horse_id')['distance_m'].diff()` | 距離変更（メートル） |
| `horse_weight_change_ratio` | 100 | 13.0% | `(weight - prev_weight) / prev_weight` | 馬体重変化率（%） |
| `horse_avg_position_4c` | 76 | 46.1% | `groupby('horse_id')['passing_order_4'].expanding().mean().shift(1)` | 4角通過順の累積平均。脚質 |

> [!NOTE]
> **NaN率12%程度は正常**（「その馬の初出走」を意味する）。NaN率45%超の特徴量（venue_nr, avg_position_4c）は、その馬がその条件で初めて走る場合や、元データ欠損（passing_order_4は56.6%欠損）による。

### 5.4 Gap特徴量（市場の歪み検出）

Gap特徴量は「実力ランク」と「人気ランク」の差を計算し、**市場が見逃している価値（ミスプライス）**を検出する。

| 特徴量 | 重要度 | 計算式 | 解説 |
|--------|--------|--------|------|
| `gap_jockey_popularity` | **262** | `popularity - rank(jockey_win_rate)` | 騎手の実力に対する過小/過大評価 |
| `gap_pedigree_popularity` | 186 | `popularity - rank(sire_win_rate)` | 血統の実力に対する過小/過大評価 |
| `gap_course_fit_popularity` | 173 | `popularity - rank(sire_course_win_rate)` | コース適性に対する過小/過大評価 |
| `gap_trainer_popularity` | 142 | `popularity - rank(trainer_win_rate)` | 調教師の実力に対する過小/過大評価 |

**解釈**: 
- **正の値** = 人気順位 > 実力順位 = **過小評価（買い候補）**
- **負の値** = 人気順位 < 実力順位 = 過大評価（見送り候補）

> [!WARNING]
> **Training-Serving Skewリスク**: Gap特徴量は`popularity`（確定人気）に依存。実運用では`morning_popularity`に置き換える必要があるが、相関が高ければ影響は軽微。

### 5.5 全特徴量リスト（アルファベット順）

<details>
<summary>クリックして展開（169特徴量）</summary>

```
【A. 基本情報（7個）】
age, age_zscore, day_of_meeting, distance_m, horse_number, race_month, round_of_year

【B. 馬体・コンディション（8個）】
basis_weight_zscore, horse_weight_change, horse_weight_zscore, sex_牡, form_rank,
bias_seasonal_score, fast_track_score_avg, heavy_track_score_avg

【C. 枠・ポジション（5個）】
bracket_avg_finish, bracket_is_inner, bracket_is_middle, bracket_is_outer, days_since_last_race

【D. 過去成績 - 着順（20個）】
avg_finish_last3, avg_finish_last5, avg_finish_last10,
past_1_finish_position_max,
past_3_finish_position_max, past_3_finish_position_mean, past_3_finish_position_median, past_3_finish_position_std,
past_5_finish_position_max, past_5_finish_position_mean, past_5_finish_position_median, past_5_finish_position_std,
past_10_finish_position_max, past_10_finish_position_mean, past_10_finish_position_median, past_10_finish_position_std,
finish_cv_last3, finish_cv_last5, finish_cv_last10, finish_std_last3, finish_std_last5, finish_std_last10

【E. 過去成績 - 上がり3F（16個）】
past_1_last_3f_time_max, past_1_last_3f_time_mean,
past_3_last_3f_time_max, past_3_last_3f_time_mean, past_3_last_3f_time_median, past_3_last_3f_time_std,
past_5_last_3f_time_max, past_5_last_3f_time_mean, past_5_last_3f_time_median, past_5_last_3f_time_std,
past_10_last_3f_time_max, past_10_last_3f_time_mean, past_10_last_3f_time_median, past_10_last_3f_time_std

【F. 過去成績 - 着差（16個）】
past_1_margin_seconds_max,
past_3_margin_seconds_max, past_3_margin_seconds_mean, past_3_margin_seconds_median, past_3_margin_seconds_std,
past_5_margin_seconds_max, past_5_margin_seconds_mean, past_5_margin_seconds_median, past_5_margin_seconds_std,
past_10_margin_seconds_max, past_10_margin_seconds_mean, past_10_margin_seconds_median, past_10_margin_seconds_std

【G. 過去成績 - 通過順（24個）】
past_1_passing_order_1_max, past_1_passing_order_4_max,
past_3_passing_order_1_max, past_3_passing_order_1_mean, past_3_passing_order_1_median, past_3_passing_order_1_std,
past_3_passing_order_4_max, past_3_passing_order_4_mean, past_3_passing_order_4_median, past_3_passing_order_4_std,
past_5_passing_order_1_mean, past_5_passing_order_1_median, past_5_passing_order_1_std,
past_5_passing_order_4_mean, past_5_passing_order_4_median, past_5_passing_order_4_std,
past_10_passing_order_1_max, past_10_passing_order_1_mean, past_10_passing_order_1_median, past_10_passing_order_1_std,
past_10_passing_order_4_max, past_10_passing_order_4_mean, past_10_passing_order_4_median, past_10_passing_order_4_std

【H. 勝率・連対率（3個）】
win_rate_last5, place_rate_last10

【I. 距離・コース適性（10個）】
distance_change, prev_distance_m, dist_avg_finish, dist_avg_time, dist_races,
surface_avg_finish, surface_avg_last3f, surface_races, venue_avg_finish, venue_races

【J. 騎手（18個）】
jockey_nr_global, jockey_芝_win_rate, jockey_ダート_win_rate,
jockey_sprint_win_rate, jockey_mile_win_rate, jockey_intermediate_win_rate, jockey_long_win_rate, jockey_marathon_win_rate, jockey_unknown_win_rate,
jockey_東京_win_rate, jockey_中山_win_rate, jockey_阪神_win_rate, jockey_京都_win_rate, jockey_中京_win_rate,
jockey_小倉_win_rate, jockey_新潟_win_rate, jockey_福島_win_rate, jockey_函館_win_rate, jockey_札幌_win_rate

【K. 調教師（10個）】
trainer_東京_win_rate, trainer_中山_win_rate, trainer_阪神_win_rate, trainer_京都_win_rate, trainer_中京_win_rate,
trainer_小倉_win_rate, trainer_新潟_win_rate, trainer_福島_win_rate, trainer_函館_win_rate, trainer_札幌_win_rate

【L. 血統（10個）】
sire_nr_global, sire_course_win_rate, sire_course_avg_finish, sire_course_place_rate, sire_wet_boost,
bms_avg_finish, bms_win_rate, nicks_avg_finish, nicks_win_rate

【M. コンボ（騎手×馬）（4個）】
combo_avg_finish, combo_win_rate, combo_races, combo_overperform

【N. Gap特徴量（4個）】
gap_jockey_popularity, gap_trainer_popularity, gap_pedigree_popularity, gap_course_fit_popularity

【O. ペース・展開（5個）】
pace_fit_score, leader_ratio, n_leaders, is_overvalued, race_class_overbet_risk

【P. タイム指数（2個）】
time_deviation_score_avg_5, l3f_deviation_score_avg_5

【Q. 馬自身（v5.4新規、11個）】
horse_time_deviation_avg, horse_l3f_deviation_avg, horse_best_time_deviation,
horse_venue_nr, horse_distance_nr, horse_surface_nr, horse_best_nr,
horse_interval_days, horse_dist_change, horse_weight_change_ratio, horse_avg_position_4c
```

</details>

---

## 6. リーク・過学習リスク管理

> [!CAUTION]
> **このセクションは最重要です。リークや過学習は、バックテストでは高ROIを示しながら実運用で資金を溶かす最大の原因です。**

### 6.1 データリークとは

**定義**: 予測時点では知り得ない「未来情報」がモデル学習に含まれること

**競馬における典型例**:
1. **今回のレース結果を特徴量に含める**（着順、タイム）
2. **確定オッズを予測前に使用**（人気順は確定後にしか分からない）
3. **同日の他レース結果を参照**（時間的順序の逆転）

### 6.2 過去に発生したリーク事例と教訓

#### v5.0: expanding()のshift(1)忘れ（影響: +7.72%の過大評価）

```python
# ❌ リークあり（今回のレース結果が含まれる）
perf['horse_distance_nr'] = perf.groupby(['horse_id', 'distance_category'])['normalized_rank'].mean()

# ✅ リークなし（今回のレースを除外）
perf['horse_distance_nr'] = (
    perf.groupby(['horse_id', 'distance_category'])['normalized_rank']
    .transform(lambda x: x.expanding().mean().shift(1))
)
```

**教訓**: 累積特徴量には必ず`shift(1)`を適用し、「今回のレース結果」を除外する。

#### v5.3: Optuna目的関数へのTest混入（影響: +6.28%の過大評価）

```python
# ❌ Test Leakage（ハイパーパラメータがTestに適合）
def objective(trial):
    valid_roi = calculate_roi(valid_df, ...)
    test_roi = calculate_roi(test_df, ...)  # ← Testを参照
    return valid_roi - abs(valid_roi - test_roi) * 0.1  # ← Testに依存

# ✅ クリーン評価（v5.4）
def objective(trial):
    valid_roi = calculate_roi(valid_df, ...)
    return valid_roi  # Validのみで最適化
```

**教訓**: TestデータはOptuna最適化に一切使用しない。Testは最終評価で1回のみ参照。

### 6.3 リーク防止チェックリスト

開発時に必ず確認すること：

- [ ] **時系列ソート**: `sort_values(['horse_id', 'race_date'])` を最初に実行
- [ ] **shift(1)適用**: 全ての累積特徴量に `expanding().mean().shift(1)` を使用
- [ ] **diff()適用**: 変化量特徴量に `groupby('horse_id').diff()` を使用
- [ ] **レース結果カラム除外**: `finish_position`, `win_odds`, `popularity` を特徴量に含めない
- [ ] **Testデータ隔離**: Optunaの目的関数にTestを含めない、Testは最終評価でのみ1回参照
- [ ] **morning_oddsの取り扱い**: 実運用で使用する場合はスクレイピングを実装

### 6.4 過学習防止策

| 手法 | v5.4での設定 | 効果 |
|------|-------------|------|
| **正則化** | `lambda_l1=4.31, lambda_l2=0.25` | 過度な重み抑制 |
| **葉のサンプル数** | `min_child_samples=55` | ノイズへの過学習防止 |
| **Early Stopping** | `lgb.early_stopping(100)` | 最適イテレーション数自動選択 |
| **Feature Fraction** | `feature_fraction=0.64` | 全特徴量の64%のみ使用 |
| **Bagging** | `bagging_fraction=0.82, bagging_freq=5` | データサブセットで学習 |

### 6.5 Valid-Test Gap監視

| Gap | 解釈 | アクション |
|-----|------|----------|
| <3% | 健全 | 問題なし |
| 3-5% | 軽微な過学習の可能性 | 正則化検討 |
| 5-10% | 注意 | 特徴量見直し |
| **>10%** | **危険** | **開発中止、原因究明** |

v5.4のGap: 4.20%（軽微な範囲、許容）

---

## 7. 運用設計と投資機会

### 7.1 ROI計算方式

現在の評価方式（Top1推奨）：

```python
def calculate_roi(df, preds):
    df['score'] = preds
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet = df[df['rank_pred'] == 1]  # 各レースでTop1のみに投資
    hits = bet[bet['finish_position'] == 1]  # 的中したレース
    return hits['win_odds'].sum() / len(bet)  # 回収率
```

**投資機会**: 全レースの100%（各レースで1頭を推奨）

### 7.2 EV閾値による投資フィルタリング（将来実装）

```python
# 期待値 = 推定勝率 × オッズ
ev = predicted_win_prob * win_odds

# EV ≥ 1.0 の場合のみ投資
invest = ev >= 1.0
investment_ratio = invest.mean()  # 目標: 20%以上維持
```

| EV閾値 | 投資対象比率 | ROI期待値 | トレードオフ |
|--------|-------------|-----------|-------------|
| 1.0 | 約50% | 中 | バランス |
| 1.1 | 約30% | 高 | 投資機会減少 |
| **1.2** | **約20%** | 最高 | **最低維持ライン** |

> [!IMPORTANT]
> **投資機会20%以上維持は必須設計要件**。これを下回ると統計的有意性が確保できず、分散が増大する。

### 7.3 Kelly半減ルール（リスク管理）

Kelly基準による資金配分は理論的に最適だが、推定誤差を考慮して半分に抑制：

```python
kelly_fraction = (p * b - q) / b  # 標準Kelly
adjusted_fraction = kelly_fraction / 2  # 半減ルール
```

**理由**: 
- 勝率推定に誤差がある
- 過信による破産リスク回避
- 長期的な資金曲線の安定化

---

## 8. 開発ロードマップ

### 8.1 完了済み

| フェーズ | 内容 | 成果 |
|---------|------|------|
| **v5.0-5.4** | 馬自身特徴量の設計・実装 | 11特徴量追加 |
| **リーク対策** | expanding+shift適用 | 全特徴量で対応完了 |
| **Test Leakage対策** | Optuna目的関数修正 | クリーン評価実現 |
| **目的関数比較** | LambdaRank vs Binary | ほぼ同等と確認 |
| **Calibration** | 確率較正分析 | 中穴で過小評価を発見 |
| **Forward Selection** | 特徴量選択 | 正則化効果を確認 |

### 8.2 進行中

| タスク | 状態 | 優先度 |
|--------|------|--------|
| v5.4をベースモデルとして確定 | ✅ 完了 | - |
| morning_oddsスクレイピング実装 | 🔴 未着手 | 高 |
| EV閾値チューニング | 🔴 未着手 | 高 |

### 8.3 予定（Phase 6 - ROI 85%超え）

- [ ] 中穴（10-20%帯）の予測精度改善
- [ ] 馬場バイアス特徴量（開催日×コース×内外）
- [ ] タイム指数の距離補正
- [ ] σモデル（不確実性）の再学習
- [ ] νモデル（荒れ度）の再学習

### 8.4 予定（Phase 7 - 自動購入システム）

- [ ] morning_odds取得パイプライン
- [ ] レース直前オッズスクレイピング（JRA）
- [ ] 購入判別ロジック（EV閾値）
- [ ] 自動購入実行（JRAオンライン投票API連携）
- [ ] リアルタイム監視ダッシュボード

---

## 9. 関連資料一覧

### 9.1 システムドキュメント

| ファイル | 場所 | 内容 |
|----------|------|------|
| **CLAUDE.md** | `/CLAUDE.md` | AI開発者向けガイド（全体アーキテクチャ） |
| **parquet_contents_report.md** | `/parquet_contents_report.md` | Parquetカラム一覧とサンプル |
| システム概要 | `/docs/system/01_システム概要.md` | システム全体像 |
| アーキテクチャ | `/docs/system/02_アーキテクチャ.md` | 技術構成 |
| 運用ガイド | `/docs/system/10_運用ガイド.md` | 日次運用手順 |

### 9.2 モデルドキュメント

| ファイル | 場所 | 内容 |
|----------|------|------|
| **本レポート** | `/docs/model/mu_v5_comprehensive_report.md` | 包括的現状レポート |
| v3.5特徴量リファレンス | `/docs/model/mu_v3_5_feature_reference.md` | v3.5の特徴量説明（165個） |
| v5.4モデル情報 | `/keibaai/models/mu_v5_4/model_info.json` | v5.4のパラメータ・性能 |
| v5.4特徴量一覧 | `/keibaai/models/mu_v5_4/feature_names.json` | v5.4の169特徴量 |

### 9.3 主要スクリプト

| スクリプト | 場所 | 用途 |
|----------|------|------|
| **train_mu_v5_4.py** | `/scripts/training/train_mu_v5_4.py` | v5.4学習スクリプト |
| compare_objectives.py | `/scripts/training/compare_objectives.py` | 目的関数比較 |
| generate_calibration_plot.py | `/scripts/analysis/generate_calibration_plot.py` | Calibration Plot生成 |
| evaluate_model_metrics.py | `/scripts/training/evaluate_model_metrics.py` | 追加評価指標 |
| simulate_daily_races.py | `/scripts/simulation/simulate_daily_races.py` | シミュレーション |

### 9.4 データ格納場所

| データ | 場所 | 形式 | 件数 |
|--------|------|------|------|
| レース結果 | `/keibaai/data/parsed/parquet/races/races.parquet` | Parquet | 277,826 |
| 出馬表 | `/keibaai/data/parsed/parquet/shutuba/shutuba.parquet` | Parquet | 277,826 |
| 馬情報 | `/keibaai/data/parsed/parquet/horses/horses.parquet` | Parquet | - |
| 血統 | `/keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet` | Parquet | 1,377,361 |
| 学習済みモデル | `/keibaai/models/mu_v5_4/` | pkl, json | - |
| 学習データ | `/keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet` | Parquet | 261,996 |

### 9.5 検証結果ファイル

| ファイル | 場所 | 内容 |
|----------|------|------|
| 目的関数比較 | `/keibaai/models/objective_comparison/results.json` | LambdaRank vs Binary |
| Calibration | `/keibaai/models/calibration/calibration_data.json` | 確率較正データ |
| Forward Selection | `/keibaai/models/mu_v5_6/forward_selection_results.json` | 特徴量選択結果 |
| ベースライン評価 | `/keibaai/models/mu_baseline/baseline_results.json` | v3.3 A/Bテスト |

---

## 付録A: 用語集

| 用語 | 説明 |
|------|------|
| **NR (Normalized Rank)** | 正規化着順。0（最下位）〜1（1着）にスケール。`(頭数 - 着順 + 1) / 頭数` |
| **ROI (Return on Investment)** | 回収率。100%で収支トントン、100%超で利益 |
| **EV (Expected Value)** | 期待値。`勝率 × オッズ`。1.0以上で理論的な投資価値あり |
| **リーク (Data Leakage)** | 予測時点で知り得ない情報が混入する問題。バックテスト性能を過大評価する |
| **Test Leakage** | ハイパーパラメータ最適化にTestデータを使用してしまう問題 |
| **Valid-Test Gap** | 検証セットとテストセットの性能差。10%超は過学習の危険信号 |
| **Gap特徴量** | 「実力ランク」と「人気ランク」の差。市場のミスプライスを検出 |
| **Kelly Criterion** | 資金配分の最適化手法。期待値に応じて投資比率を決定 |
| **Training-Serving Skew** | 学習時と運用時でデータ条件が異なる問題 |
| **Calibration** | 予測確率と実際の発生確率の一致度 |

---

## 付録B: コマンドリファレンス

### モデル学習

```bash
# v5.4学習（Optuna 50トライアル）
python scripts/training/train_mu_v5_4.py

# 目的関数比較
python scripts/training/compare_objectives.py

# Calibration Plot生成
python scripts/analysis/generate_calibration_plot.py
```

### 評価

```bash
# 追加評価指標
python scripts/training/evaluate_model_metrics.py

# シミュレーション
python scripts/simulation/simulate_daily_races.py
```

### データ確認

```bash
# Parquetカラム確認
python -c "import pandas as pd; df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet'); print(df.columns.tolist())"

# 特徴量一覧確認
python -c "import json; print(json.load(open('keibaai/models/mu_v5_4/feature_names.json')))"
```

---

*最終更新: 2025-12-05 by AI Assistant*
