# μモデル v5.4 システム現状レポート

**作成日**: 2025-12-05
**バージョン**: μ Model v5.4 (クリーン評価版)
**目的**: 第三者への包括的な技術・運用ドキュメント

---

## 📋 目次

1. [エグゼクティブサマリー](#1-エグゼクティブサマリー)
2. [システム概要と最終目標](#2-システム概要と最終目標)
3. [現状のモデル性能](#3-現状のモデル性能)
4. [全特徴量一覧（169個）](#4-全特徴量一覧169個)
5. [データソースとパイプライン](#5-データソースとパイプライン)
6. [リーク・過学習リスク管理](#6-リーク過学習リスク管理)
7. [運用上の注意事項](#7-運用上の注意事項)
8. [開発ロードマップ](#8-開発ロードマップ)
9. [関連資料一覧](#9-関連資料一覧)

---

## 1. エグゼクティブサマリー

### 現状のパフォーマンス

| 指標 | v5.4 (現行) | v3.5 (ベースライン) | 備考 |
|------|-------------|---------------------|------|
| **Valid ROI** | 84.25% | 83.79% | 2023年データ（検証用） |
| **Test ROI** | 80.05% | 81.84% | 2024年データ（未知データ評価） |
| **Valid-Test差** | 4.20% | 1.95% | 健全範囲内（<10%が目安） |
| **特徴量数** | 169 | 157 | +12個（馬自身の特徴量） |

### 達成状況

- ✅ **リークなし**：全特徴量で`expanding().mean().shift(1)`パターン適用済み
- ✅ **過学習なし**：Valid-Test差 4.20%は健全範囲
- ✅ **投資機会維持**：全レースの100%に投資可能（Top1推奨方式）
- ❌ **v3.5超え未達**：Test ROI 80.05% < 81.84%（▲1.79%）

### 最終目標

**競馬投資における収益最大化**

本システムの目的は、機械学習による勝率予測と期待値計算に基づき、**長期的な収益を最大化**することである。

これを実現するための手段として、以下のパイプラインを構築する：
- **朝一資金配分**: `morning_odds`で期待値を計算し、Kelly基準で投資配分を決定
- **レース直前購入判別**: 最終オッズで期待値を再計算し、投資可否を判断
- **自動購入システム**: JRAオンライン投票で自動執行

---

## 2. システム概要と最終目標

### 2.1 KeibaAI_v2 とは

競馬AI予測・最適投資システム。以下のパイプラインで構成される：

```
[スクレイピング] → [パース] → [特徴量生成] → [モデル学習]
        ↓                          ↓               ↓
     HTML.bin              Parquet         LightGBM
                                               ↓
[シミュレーション] ← [予測] ← [推論]
        ↓
[ポートフォリオ最適化（Kellyクライテリオン）]
        ↓
[購入判別・自動購入]（将来実装）
```

### 2.2 最終目標アーキテクチャ

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

> [!IMPORTANT]
> **テスト/バックテスト時の注意**：
> `morning_odds`はレース当日朝にしか取得できないため、バックテストでは`races.parquet`に格納された確定オッズ(`win_odds`)を代用する。
> これは「最終オッズで資金配分した場合」のシミュレーションとなり、実運用とは若干異なる条件となる。

### 2.3 投資機会の設計方針

**収益最大化のため、期待値が高いレースに集中投資**

競馬の控除率は約25%（JRA単勝）であり、市場全体の期待値は約75%。収益を上げるには「市場の歪み（ミスプライス）」を見つけ、**期待値(EV) > 1.0のレースにのみ投資**する必要がある。

**投資機会の目安**：
- **EV閾値 1.0**: 投資対象レースは全体の約50%程度
- **EV閾値 1.1**: 投資対象レースは全体の約30%程度
- **EV閾値 1.2**: 投資対象レースは全体の約20%程度

> [!NOTE]
> 「投資機会を減らさない」ことは目的ではない。
> **期待値が低いレースを避け、高いレースに集中する**ことが収益最大化の本質。

---

## 3. 現状のモデル性能

### 3.1 バージョン履歴

| Version | Valid ROI | Test ROI | Gap | 備考 |
|---------|-----------|----------|-----|------|
| v3.5 | 83.79% | 81.84% | 1.95% | ベースライン（リークなし確認済み） |
| **v5.0** | 122.51% | 87.72% | **34.79%** | ⚠️ リーク発覚（expanding未適用） |
| v5.1 | 560.75% | 74.14% | 486.61% | ❌ データソース誤り（NaN率87%） |
| v5.2 | 85.16% | 79.95% | 5.21% | races.parquet使用に修正 |
| **v5.3** | 84.10% | 86.33% | 2.23% | ⚠️ Test Leakage発覚（Optunaにtest含む） |
| **v5.4** | 84.25% | **80.05%** | 4.20% | ✅ クリーン評価版（最終） |

### 3.2 v5.4 ハイパーパラメータ

```json
{
  "objective": "lambdarank",
  "metric": "ndcg",
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

### 3.3 データ分割

| セット | 期間 | レコード数 | 用途 |
|--------|------|-----------|------|
| Train | ~2022/12/31 | 139,379 | モデル学習 |
| Valid | 2023/01/01~2023/12/31 | 44,212 | ハイパーパラメータ最適化 |
| Test | 2024/01/01~ | 78,405 | 最終評価（一度のみ参照） |

---

## 4. 全特徴量一覧（169個）

### 4.1 特徴量カテゴリ別概要

| カテゴリ | 特徴量数 | 主要特徴量 |
|----------|----------|------------|
| **A. 馬自身（v5.4新規）** | 11 | horse_time_deviation_avg, horse_distance_nr, horse_interval_days |
| **B. 騎手・調教師** | 28 | jockey_nr_global, jockey_芝_win_rate, trainer_新潟_win_rate |
| **C. 血統** | 8 | sire_nr_global, sire_course_win_rate, bms_avg_finish |
| **D. 過去成績** | 68 | past_1_finish_position_max, past_5_last_3f_time_mean |
| **E. レース条件** | 15 | distance_m, horse_number, bracket_is_inner |
| **F. コンディション** | 12 | horse_weight_zscore, age_zscore, form_rank |
| **G. Gap特徴量** | 4 | gap_jockey_popularity, gap_pedigree_popularity |
| **H. その他** | 23 | combo_avg_finish, pace_fit_score |

### 4.2 Top 20 重要度（v5.4）

| 順位 | 特徴量 | 重要度 | カテゴリ | 説明 |
|------|--------|--------|----------|------|
| 1 | `gap_jockey_popularity` | 262 | Gap | 騎手実力と人気の乖離 |
| 2 | `jockey_nr_global` | 211 | 騎手 | 騎手の総合正規化着順（NR算出） |
| 3 | `gap_pedigree_popularity` | 186 | Gap | 血統実力と人気の乖離 |
| 4 | `sire_course_win_rate` | 181 | 血統 | 種牡馬のコース別勝率 |
| 5 | `sire_nr_global` | 176 | 血統 | 種牡馬の総合NR |
| 6 | `gap_course_fit_popularity` | 173 | Gap | コース適性と人気の乖離 |
| 7 | `gap_trainer_popularity` | 142 | Gap | 調教師実力と人気の乖離 |
| 8 | `horse_weight_zscore` | 140 | 条件 | 馬体重の標準化値 |
| **9** | **`horse_distance_nr`** | **129** | **馬(v5)** | **馬の距離別NR平均** |
| 10 | `trainer_新潟_win_rate` | 127 | 調教師 | 調教師の新潟勝率 |
| **11** | **`horse_time_deviation_avg`** | **118** | **馬(v5)** | **馬のタイム偏差平均** |
| **12** | **`horse_surface_nr`** | **113** | **馬(v5)** | **馬の芝/ダート別NR** |
| **13** | **`horse_interval_days`** | **108** | **馬(v5)** | **前走からの間隔日数** |
| 14 | `sire_course_avg_finish` | 107 | 血統 | 種牡馬のコース別平均着順 |
| 15 | `sire_wet_boost` | 107 | 血統 | 重馬場での種牡馬ブースト |
| **16** | **`horse_venue_nr`** | **104** | **馬(v5)** | **馬の会場別NR** |
| **17** | **`horse_weight_change_ratio`** | **100** | **馬(v5)** | **馬体重変化率** |
| 18 | `jockey_芝_win_rate` | 97 | 騎手 | 騎手の芝勝率 |
| 19 | `combo_overperform` | 96 | コンボ | 騎手×馬の過去相性 |
| 20 | `time_deviation_score_avg_5` | 96 | 過去 | 直近5走タイム偏差 |

### 4.3 馬自身の特徴量（v5.4新規、11個）

| 特徴量 | 重要度 | NaN率 | 説明 | 計算方法 |
|--------|--------|-------|------|----------|
| `horse_time_deviation_avg` | 118 | 12.9% | タイム偏差平均 | `expanding().mean().shift(1)` |
| `horse_l3f_deviation_avg` | 87 | 12.9% | 上がり3F偏差平均 | `expanding().mean().shift(1)` |
| `horse_best_time_deviation` | 83 | 12.9% | 過去最高タイム偏差 | `expanding().max().shift(1)` |
| `horse_venue_nr` | 104 | 45.1% | 会場別NR平均 | `groupby(['horse_id','venue']).expanding().mean().shift(1)` |
| `horse_distance_nr` | 129 | 25.6% | 距離カテゴリ別NR | `groupby(['horse_id','dist_cat']).expanding().mean().shift(1)` |
| `horse_surface_nr` | 113 | 21.1% | 芝/ダート別NR | `groupby(['horse_id','surface']).expanding().mean().shift(1)` |
| `horse_best_nr` | 36 | 12.9% | 過去最高NR | `expanding().max().shift(1)` |
| `horse_interval_days` | 108 | 12.8% | 前走間隔 | `groupby('horse_id')['race_date'].diff().dt.days` |
| `horse_dist_change` | 31 | 16.1% | 距離変更 | `groupby('horse_id')['distance_m'].diff()` |
| `horse_weight_change_ratio` | 100 | 13.0% | 馬体重変化率 | `(weight - prev_weight) / prev_weight` |
| `horse_avg_position_4c` | 76 | 46.1% | 4角通過順平均 | `expanding().mean().shift(1)` |

> [!NOTE]
> **NaN率について**：NaN率12%程度は「その馬の初出走」を意味し、これは正常な欠損です。
> NaN率45%超の特徴量（venue_nr, avg_position_4c）は、その馬がその条件で初めて走る場合や、元データ欠損によります。

### 4.4 全特徴量一覧（アルファベット順）

```
A. 基本情報（7個）
  - age: 馬齢
  - age_zscore: 馬齢の標準化
  - day_of_meeting: 開催日
  - distance_m: 距離（メートル）
  - horse_number: 馬番
  - race_month: レース月
  - round_of_year: 年内開催回次

B. 馬体・コンディション（8個）
  - basis_weight_zscore: 斤量の標準化
  - horse_weight_change: 馬体重変化
  - horse_weight_zscore: 馬体重の標準化
  - sex_牡: 性別（牡馬フラグ）
  - form_rank: 調子ランク
  - bias_seasonal_score: 季節バイアススコア
  - fast_track_score_avg: 良馬場スコア平均
  - heavy_track_score_avg: 重馬場スコア平均

C. 枠・ポジション（5個）
  - bracket_avg_finish: 枠番別平均着順
  - bracket_is_inner: 内枠フラグ
  - bracket_is_middle: 中枠フラグ
  - bracket_is_outer: 外枠フラグ
  - days_since_last_race: 前走からの日数

D. 過去成績 - 着順（20個）
  - avg_finish_last3/5/10: 直近3/5/10走平均着順
  - past_{1,3,5,10}_finish_position_{max,mean,median,std}: 過去走着順統計

E. 過去成績 - 上がり3F（16個）
  - past_{1,3,5,10}_last_3f_time_{max,mean,median,std}: 過去走上がり3F統計

F. 過去成績 - 着差（16個）
  - past_{1,3,5,10}_margin_seconds_{max,mean,median,std}: 過去走着差統計

G. 過去成績 - 通過順（16個）
  - past_{1,3,5,10}_passing_order_{1,4}_{max,mean,median,std}: 過去走通過順統計

H. 勝率・連対率（5個）
  - win_rate_last5: 直近5走勝率
  - place_rate_last10: 直近10走連対率
  - finish_cv_last3/5/10: 着順変動係数
  - finish_std_last3/5/10: 着順標準偏差

I. 距離・コース適性（10個）
  - distance_change: 距離変更
  - prev_distance_m: 前走距離
  - dist_avg_finish: 距離別平均着順
  - dist_avg_time: 距離別平均タイム
  - dist_races: 距離別出走回数
  - surface_avg_finish: 芝/ダート別平均着順
  - surface_avg_last3f: 芝/ダート別上がり3F
  - surface_races: 芝/ダート別出走回数
  - venue_avg_finish: 会場別平均着順
  - venue_races: 会場別出走回数

J. 騎手（18個）
  - jockey_nr_global: 騎手総合NR
  - jockey_{芝,ダート}_win_rate: 芝/ダート勝率
  - jockey_{sprint,mile,intermediate,long,marathon,unknown}_win_rate: 距離別勝率
  - jockey_{東京,中山,阪神,京都,中京,小倉,新潟,福島,函館,札幌}_win_rate: 会場別勝率

K. 調教師（9個）
  - trainer_{東京,中山,阪神,京都,中京,小倉,新潟,福島,函館,札幌}_win_rate: 会場別勝率

L. 血統（8個）
  - sire_nr_global: 種牡馬総合NR
  - sire_course_win_rate: 種牡馬コース別勝率
  - sire_course_avg_finish: 種牡馬コース別平均着順
  - sire_course_place_rate: 種牡馬コース別連対率
  - sire_wet_boost: 種牡馬重馬場ブースト
  - bms_avg_finish: 母父平均着順
  - bms_win_rate: 母父勝率
  - nicks_avg_finish: ニックス平均着順
  - nicks_win_rate: ニックス勝率

M. コンボ（騎手×馬）（4個）
  - combo_avg_finish: 騎手×馬の平均着順
  - combo_win_rate: 騎手×馬の勝率
  - combo_races: 騎手×馬の出走回数
  - combo_overperform: 騎手×馬の過剰パフォーマンス

N. Gap特徴量（4個）
  - gap_jockey_popularity: 騎手実力と人気の乖離
  - gap_trainer_popularity: 調教師実力と人気の乖離
  - gap_pedigree_popularity: 血統実力と人気の乖離
  - gap_course_fit_popularity: コース適性と人気の乖離

O. ペース・展開（5個）
  - pace_fit_score: ペース適性スコア
  - leader_ratio: 逃げ馬比率
  - n_leaders: 逃げ馬数
  - is_overvalued: 過大評価フラグ
  - race_class_overbet_risk: クラス別過剰人気リスク

P. タイム指数（2個）
  - time_deviation_score_avg_5: 直近5走タイム偏差
  - l3f_deviation_score_avg_5: 直近5走上がり偏差

Q. 馬自身の特徴量【v5.4新規】（11個）
  - horse_time_deviation_avg: タイム偏差平均
  - horse_l3f_deviation_avg: 上がり偏差平均
  - horse_best_time_deviation: 最高タイム偏差
  - horse_venue_nr: 会場別NR
  - horse_distance_nr: 距離別NR
  - horse_surface_nr: 芝/ダート別NR
  - horse_best_nr: 過去最高NR
  - horse_interval_days: 前走間隔
  - horse_dist_change: 距離変更
  - horse_weight_change_ratio: 馬体重変化率
  - horse_avg_position_4c: 4角通過順平均
```

---

## 5. データソースとパイプライン

### 5.1 Parquetファイル一覧

| ファイル | レコード数 | 主要カラム | 用途 |
|----------|-----------|-----------|------|
| `races/races.parquet` | 277,826 | race_id, finish_position, win_odds, finish_time_seconds, last_3f_time, passing_order_4, horse_weight | レース結果（学習・評価） |
| `shutuba/shutuba.parquet` | - | morning_odds, morning_popularity, horse_weight | 出馬表（当日予測用） |
| `horses/horses.parquet` | - | horse_id, birth_date, trainer_id | 馬プロファイル |
| `pedigrees/pedigrees.parquet` | 1,377,361 | horse_id, ancestor_id, generation | 血統（5世代） |

### 5.2 主要カラム詳細（races.parquet）

> [!IMPORTANT]
> **テスト/バックテスト時の注意**：
> `morning_odds`は`shutuba.parquet`にのみ存在し、過去データでは欠損が多い。
> バックテストでは`races.parquet`の`win_odds`（確定オッズ）を代用する。

| カラム | 型 | 説明 | 欠損率 |
|--------|----|----|--------|
| `race_id` | str | レースID（例: 202001010101） | 0% |
| `race_date` | datetime | 開催日 | 0% |
| `finish_position` | Int64 | 着順 | <1% |
| `win_odds` | float | 確定単勝オッズ | <1% |
| `popularity` | Int64 | 人気順 | <1% |
| `finish_time_seconds` | float | 走破タイム（秒） | <1% |
| `last_3f_time` | float | 上がり3Fタイム | <1% |
| `passing_order_4` | Int64 | 4角通過順 | 56.6% |
| `horse_weight` | Int64 | 馬体重 | <1% |
| `horse_weight_change` | Int64 | 馬体重増減 | <1% |
| `distance_m` | Int64 | 距離（メートル） | 0% |
| `track_surface` | str | 芝/ダート | 0% |
| `track_condition` | str | 馬場状態（良/稍/重/不） | <1% |
| `venue` | str | 開催場（例: 東京1回1日目） | 0% |

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

### 6.2 v5.0で発生したリーク事例

**問題**: `expanding().mean()` を使用したが `shift(1)` を忘れた

```python
# ❌ リークあり（今回のレース結果が含まれる）
perf['horse_distance_nr'] = perf.groupby(['horse_id', 'distance_category'])['normalized_rank'].mean()

# ✅ リークなし（今回のレースを除外）
perf['horse_distance_nr'] = (
    perf.groupby(['horse_id', 'distance_category'])['normalized_rank']
    .transform(lambda x: x.expanding().mean().shift(1))
)
```

**結果**: Test ROI 87.72% → 実際は80%程度（+7.72%の過大評価）

### 6.3 v5.3で発生したTest Leakage事例

**問題**: Optunaの目的関数にTestデータを含めた

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

**結果**: Test ROI 86.33% → 実際は80.05%（+6.28%の過大評価）

### 6.4 リーク防止チェックリスト

開発時に必ず確認すること：

1. [ ] **時系列ソート**: `sort_values(['horse_id', 'race_date'])` を最初に実行
2. [ ] **shift(1)適用**: 全ての累積特徴量に `expanding().mean().shift(1)` を使用
3. [ ] **diff()適用**: 変化量特徴量に `groupby('horse_id').diff()` を使用
4. [ ] **レース結果カラム除外**: `finish_position`, `win_odds`, `popularity` を特徴量に含めない
5. [ ] **Testデータ隔離**: Optunaの目的関数にTestを含めない、Testは最終評価でのみ1回参照

### 6.5 過学習防止策

1. **正則化パラメータ**: `min_child_samples=55`, `lambda_l1=4.3`, `lambda_l2=0.25`
2. **Early Stopping**: `lgb.early_stopping(100)`
3. **Feature Fraction**: `feature_fraction=0.64`（全特徴量の64%のみ使用）
4. **Valid-Test Gap監視**: 10%以上の乖離は危険信号

---

## 7. 運用上の注意事項

### 7.1 オッズデータの使用制約

| 項目 | 学習時 | 実運用（朝一） | 実運用（直前） |
|------|--------|---------------|---------------|
| morning_odds | ❌ 使用不可（欠損多） | ✅ 取得可能 | ✅ 取得可能 |
| 最終オッズ | ✅ races.parquetから | ❌ 未確定 | ✅ JRAスクレイピング |
| 人気順 | ✅ racesから | ❌ 未確定 | ✅ JRAスクレイピング |

> [!WARNING]
> **バックテスト時の制約**：
> 学習・評価には `races.parquet` の確定オッズ（`win_odds`）を使用。
> これは「最終オッズで判断した場合」の結果であり、
> 実運用で`morning_odds`を使用する場合とは条件が異なる。

### 7.2 ROI計算方式

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

### 7.3 将来の投資フィルタリング

EV閾値による絞り込みを導入する場合：

```python
# 期待値 = 推定勝率 × オッズ
ev = predicted_win_prob * win_odds

# EV ≥ 1.0 の場合のみ投資
invest = ev >= 1.0
investment_ratio = invest.mean()  # 目標: 20%以上維持
```

---

## 8. 開発ロードマップ

### 8.1 完了済み（Phase 5）

- [x] 馬自身の特徴量11個の設計・実装
- [x] データリーク検出・修正（expanding+shift）
- [x] Test Leakage検出・修正（Optuna目的関数）
- [x] クリーン評価（v5.4）完了

### 8.2 進行中（Phase 5.5 - v3.5超え）

- [ ] 正則化パラメータの更なる調整
- [ ] Optuna探索空間の拡大（100トライアル）
- [ ] 特徴量相互作用の追加

### 8.3 予定（Phase 6 - ROI 100%超え）

- [ ] タイム指数の距離補正（短距離と長距離の重み付け）
- [ ] 馬場状態×血統の相互作用特徴量
- [ ] σモデル（不確実性）の再学習
- [ ] νモデル（荒れ度）の再学習

### 8.4 予定（Phase 7 - 自動購入システム）

- [ ] morning_odds取得パイプライン
- [ ] レース直前オッズスクレイピング（JRA）
- [ ] 購入判別ロジック（EV閾値）
- [ ] 自動購入実行（JRAオンライン投票API連携）

---

## 9. 関連資料一覧

### 9.1 システムドキュメント

| ファイル | 場所 | 内容 |
|----------|------|------|
| CLAUDE.md | `/CLAUDE.md` | AI開発者向けガイド（全体アーキテクチャ） |
| parquet_contents_report.md | `/parquet_contents_report.md` | Parquetカラム一覧 |
| システム概要 | `/docs/system/01_システム概要.md` | システム全体像 |
| アーキテクチャ | `/docs/system/02_アーキテクチャ.md` | 技術構成 |
| 運用ガイド | `/docs/system/10_運用ガイド.md` | 日次運用手順 |

### 9.2 モデルドキュメント

| ファイル | 場所 | 内容 |
|----------|------|------|
| v3.5特徴量リファレンス | `/docs/model/mu_v3_5_feature_reference.md` | v3.5の特徴量説明 |
| v5.4モデル情報 | `/keibaai/models/mu_v5_4/model_info.json` | v5.4のパラメータ・性能 |
| v5.4特徴量一覧 | `/keibaai/models/mu_v5_4/feature_names.json` | v5.4の169特徴量 |

### 9.3 スクリプト一覧

| スクリプト | 場所 | 用途 |
|----------|------|------|
| train_mu_v5_4.py | `/scripts/training/train_mu_v5_4.py` | v5.4学習スクリプト |
| evaluate_model.py | `/scripts/training/evaluate_model.py` | モデル評価 |
| simulate_daily_races.py | `/scripts/simulation/simulate_daily_races.py` | シミュレーション |
| optimize_daily_races.py | `/scripts/optimization/optimize_daily_races.py` | 資金配分最適化 |

### 9.4 データ格納場所

| データ | 場所 | 形式 |
|--------|------|------|
| レース結果 | `/keibaai/data/parsed/parquet/races/` | Parquet |
| 出馬表 | `/keibaai/data/parsed/parquet/shutuba/` | Parquet |
| 馬情報 | `/keibaai/data/parsed/parquet/horses/` | Parquet |
| 血統 | `/keibaai/data/parsed/parquet/pedigrees/` | Parquet |
| 学習済みモデル | `/keibaai/models/mu_v5_4/` | pkl, json |

---

## 付録: 用語集

| 用語 | 説明 |
|------|------|
| **NR (Normalized Rank)** | 正規化着順。0（最下位）〜1（1着）にスケール |
| **ROI (Return on Investment)** | 回収率。100%で収支トントン |
| **EV (Expected Value)** | 期待値。勝率×オッズ。1.0以上で投資価値あり |
| **リーク (Data Leakage)** | 予測時点で知り得ない情報が混入する問題 |
| **Valid-Test Gap** | 検証セットとテストセットの性能差。10%超は危険 |
| **Kelly Criterion** | 資金配分の最適化手法。期待値に応じて投資比率を決定 |

---

*最終更新: 2025-12-05 by AI Assistant*
