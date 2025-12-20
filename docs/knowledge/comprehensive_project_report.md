# KeibaAI_v2 包括的プロジェクトレポート

**作成日**: 2025-12-19  
**対象読者**: 第三者開発者、データサイエンティスト、プロジェクト引継ぎ者  
**目的**: 本システムの全内容と検証履歴を第三者が完全に理解できる詳細な記録

---

## 📑 目次

1. [エグゼクティブサマリー](#1-エグゼクティブサマリー)
2. [プロジェクト概要とシステムアーキテクチャ](#2-プロジェクト概要とシステムアーキテクチャ)
3. [データ構造詳細（8種Parquetファイル）](#3-データ構造詳細8種parquetファイル)
4. [特徴量エンジニアリング詳細](#4-特徴量エンジニアリング詳細)
5. [モデル開発履歴（V7〜V20）](#5-モデル開発履歴v7v20)
6. [期待値(EV)戦略検証（2025-12-19実施）](#6-期待値ev戦略検証2025-12-19実施)
7. [予測勝率キャリブレーション実験](#7-予測勝率キャリブレーション実験)
8. [最終結論と推奨戦略](#8-最終結論と推奨戦略)
9. [技術スタックとディレクトリ構造](#9-技術スタックとディレクトリ構造)
10. [リーク・過学習防止ガイドライン](#10-リーク過学習防止ガイドライン)

---

## 1. エグゼクティブサマリー

### 1.1 プロジェクトの最終結論

> [!IMPORTANT]
> **着順予測モデルTop1（平均ROI 74.6%）が現時点で最も安定した戦略**
>
> 期待値(EV)モデルは複数の手法で検証したが、着順予測モデルを上回ることができなかった。
> オッズ帯フィルタリング（10-50倍）により単勝ROI 113.7%を達成できる可能性あり。

### 1.2 主要検証結果一覧

| 戦略 | 平均ROI | 安定性(σ) | 判定 |
|------|---------|-----------|------|
| **着順予測Top1** | **74.6%** | 7.9% | **推奨** |
| Isotonic Top3 (EV) | 64.3% | 10.6% | ✖ 劣化 |
| Platt Top2 (EV) | 63.8% | 15.3% | ✖ 劣化 |
| 生予測 EV Top1 | 49.8% | 7.4% | ✖ 大幅劣化 |
| 単勝オッズ10-50倍フィルタ | **113.7%** | - | ✓ 有望 |

### 1.3 データ期間と規模

| 項目 | 数値 |
|------|------|
| **データ期間** | 2014-01-05 〜 2025-10-26（約12年） |
| **総出走記録** | 560,753件（障害レース除外済み） |
| **総レース数** | 39,811レース（苝・ダートのみ） |
| **馬数** | 62,398頭 |
| **血統レコード** | 3,677,865件（5世代分） |

> [!NOTE]
> 障害レース12,934件は前処理時に除外。苝・ダートレースのみを対象とする。

---

## 2. プロジェクト概要とシステムアーキテクチャ

### 2.1 システム目標

JRA競馬レースにおける予測・投資最適化を目的とした機械学習システム。

**核心原則**：「オッズに織り込まれていない情報」だけを使う

```
┌─────────────────────────────────────────────────────────────────┐
│                    競馬AI失敗の構造的要因                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  【問題1】控除率の壁                                             │
│  └─ JRAが約20%を控除 → 普通に買うと回収率80%に収束              │
│  └─ 「強い馬を当てる」だけでは絶対に勝てない                    │
│                                                                 │
│  【問題2】オッズへの収束                                         │
│  └─ オッズ・人気を特徴量に入れると、予測が市場に追随            │
│  └─ 的中しても期待値が1を超えない                               │
│                                                                 │
│  【問題3】過小評価の陳腐化                                       │
│  └─ 発見した「エッジ」は他者にも発見され、すぐに消失            │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 4層ハイブリッドアーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                    4層ハイブリッドアーキテクチャ                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Layer 1: 能力推定層（Ability Estimation）                       │
│  ├─ 目的: 各馬の「真の能力」をスコア化                          │
│  ├─ 手法: LightGBM Binary Classification / LambdaRank           │
│  └─ 特徴: オッズ・人気を完全排除した特徴量                      │
│           ↓                                                     │
│  Layer 2: 確率変換層（Probability Calibration）                  │
│  ├─ 目的: スコアを「勝率」に正確に変換                          │
│  ├─ 手法: Platt Scaling / Isotonic Regression                   │
│  └─ 特徴: レース内で合計100%になる確率を生成                    │
│           ↓                                                     │
│  Layer 3: 着順シミュレーション層（Monte Carlo Simulation）       │
│  ├─ 目的: 全馬券種の出現確率を算出                              │
│  ├─ 手法: ハービル法 + モンテカルロ法                           │
│  └─ 特徴: 三連単の全順列確率を高速計算                          │
│           ↓                                                     │
│  Layer 4: 期待値最適化層（EV Optimization & Betting）            │
│  ├─ 目的: EV > 1.0 の馬券を抽出し、資金配分を最適化             │
│  ├─ 手法: ケリー基準 + 有効フロンティア                         │
│  └─ 特徴: リスク調整済みリターンの最大化                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 処理フロー（日次運用）

| 時刻 | フェーズ | 処理 | 所要時間 |
|-----|---------|------|---------| 
| **03:00** | スクレイピング | 前日レース結果取得 | 30-60分 |
| **04:00** | パース | HTML → Parquet変換 | 10-20分 |
| **04:30** | 特徴量生成 | V15特徴量計算 | 5-10分 |
| **10:00** | 予測 | 当日レース予測 | 1-2分 |
| **レース10分前** | シミュレーション | MC 1,000回 | 30秒 |
| **レース5分前** | 最適化 | Kelly配分計算 | 10秒 |
| **レース後** | モニタリング | 精度評価 | 1分 |

---

## 3. データ構造詳細（8種Parquetファイル）

### 3.1 ファイル一覧

| ファイル | 件数 | カラム数 | 主キー | 期間 |
|---------|------|---------|--------|------|
| **races.parquet** | 560,753 | 61 | (race_id, horse_number) | 2014-2025 |
| **shutuba.parquet** | 560,753 | 26 | (race_id, horse_number) | 2014-2025 |
| **horses.parquet** | 62,398 | 10 | horse_id | - |
| **pedigrees.parquet** | 3,677,865 | 4 | (horse_id, ancestor_id, generation) | - |
| **returns.parquet** | 475,407 | 10 | (race_id, bet_type) | 2014-2025 |
| **race_details.parquet** | 39,811 | 10 | race_id | 2014-2025 |
| **corner_positions.parquet** | 1,608,925 | 5 | (race_id, corner, horse_number) | 2014-2025 |

> [!NOTE]
> 障害レース12,934件（39,811→1,066レース分）は除外済み。芝・ダートの平地レースのみ。


### 3.2 races.parquet スキーマ（61カラム）

#### 基本情報（予測時使用可能）

| カラム | 型 | 説明 | 例 |
|--------|-----|------|-----|
| race_id | string | レースID（12桁: YYYYPPNNDDRR） | "202310010811" |
| race_date | date | レース開催日 | "2023-10-01" |
| venue | string | 競馬場 | "東京" / "中山" / "阪神" |
| distance_m | int16 | 距離（メートル） | 2400 |
| track_surface | string | 馬場種別 | "芝" / "ダート" / "障害" |
| track_condition | string | 馬場状態 | "良" / "稍重" / "重" / "不良" |
| race_class | string | レースクラス | "G1" / "G2" / "OP" / "未勝利" |
| horse_number | int8 | 馬番 | 7 |
| horse_id | string | 馬ID（10桁） | "2020104567" |
| bracket_number | int8 | 枠番 | 4 |
| age | int8 | 年齢 | 3 |
| sex | string | 性別 | "牡" / "牝" / "セ" |
| basis_weight | float32 | 斤量（kg） | 57.0 |
| horse_weight | int16 | 馬体重（kg） | 476 |
| horse_weight_change | int8 | 馬体重増減 | +2 |
| jockey_id | string | 騎手ID | "01234" |
| trainer_id | string | 調教師ID | "05678" |

#### 結果情報（特徴量として使用禁止 - リーク源）

| カラム | 型 | 説明 | 使用制限 |
|--------|-----|------|----------|
| **finish_position** | int8 | 着順 | **ターゲット変数** |
| finish_time_seconds | float32 | 完走タイム | ✖ リーク |
| last_3f_time | float32 | 上がり3F | ✖ リーク |
| passing_order_1-4 | int8 | 通過順位 | ✖ リーク |
| margin_seconds | float32 | 着差（秒） | ✖ リーク |
| **win_odds** | float32 | 確定オッズ | **ROI評価のみ使用可** |
| popularity | int8 | 人気順位 | ✖ リーク |

> [!CAUTION]
> **win_oddsの用途**: このカラムはレース結果データであり、**学習時の特徴量として使用禁止**（データリーク）。ROI評価時の回収率計算にのみ使用。

### 3.3 ID形式

#### race_id（12桁）
```
YYYYPPNNDDRR
2023 10 01 08 11
│    │  │  │  └─ レース番号（2桁）
│    │  │  └─── 開催日（2桁）
│    │  └────── 開催回次（2桁）
│    └───────── 競馬場コード（2桁）
└────────────── 年（4桁）

競馬場コード:
05 = 東京, 06 = 中山, 08 = 京都, 09 = 阪神
01 = 札幌, 02 = 函館, 03 = 福島, 04 = 新潟, 07 = 小倉
```

#### horse_id（10桁）
```
YYYYNNNNNN
2020 104567
│    └───── 連番（6桁）
└────────── 出生年（4桁）
```

### 3.4 returns.parquet bet_type詳細

| bet_type | 日本語 | 控除率 | 使用カラム |
|----------|--------|--------|-----------|
| tansho | 単勝 | 20% | horse_number |
| fukusho | 複勝 | 20% | horse_number |
| wakuren | 枠連 | 22.5% | bracket_1, bracket_2 |
| umaren | 馬連 | 22.5% | horse_1, horse_2 |
| wide | ワイド | 25% | horse_1, horse_2 |
| umatan | 馬単 | 25% | horse_1, horse_2 |
| sanrenpuku | 三連複 | 25% | horse_1, horse_2, horse_3 |
| sanrentan | 三連単 | 27.5% | horse_1, horse_2, horse_3 |

### 3.5 corner_positions.parquet 馬身差変換ルール

| 記号 | 加算馬身 | 意味 |
|------|----------|------|
| () 内 | 0 | 並走 |
| なし | +1.0 | デフォルト |
| , | +1.5 | 1-2馬身差 |
| - | +3.5 | 2-5馬身差 |
| = | +7.0 | 5馬身以上 |

---

## 4. 特徴量エンジニアリング詳細

### 4.1 V15推奨モデルの特徴量（66特徴量）

> [!NOTE]
> V15は継承チェーン（V7→V12→V13→V14→V15）で構成されており、実際の特徴量数は66個です。

#### 馬の過去成績（12特徴量）
| 特徴量 | 説明 | 計算方法 |
|--------|------|----------|
| horse_avg_finish | 過去平均着順 | cumsum().shift(1) / count |
| horse_avg_finish_recent5 | 直近5走平均着順 | rolling(5).mean().shift(1) |
| horse_win_rate | 勝率 | 累積勝利数 / 累積レース数 |
| horse_top3_rate | 複勝率 | 累積3着以内数 / 累積レース数 |
| horse_race_count | 出走回数 | cumsum().shift(1) |
| horse_avg_last3f_rank | 上がり3F順位平均 | 過去のlast3f_rank平均 |
| horse_c4_position_avg | 4コーナー平均位置 | 過去のC4順位平均 |
| horse_c4_gap_avg | 4コーナー馬身差平均 | corner_positionsから算出 |
| horse_class_change | クラス変動 | 前走クラスとの差 |
| horse_days_since_last | 前走からの日数 | race_date.diff() |
| horse_weight_change | 馬体重変化 | 前走との差 |
| horse_finish_cv | 着順変動係数 | std / mean |

#### 騎手・調教師（10特徴量）
| 特徴量 | 説明 |
|--------|------|
| jockey_win_rate | 騎手勝率 |
| jockey_top3_rate | 騎手複勝率 |
| jockey_race_count | 騎手出走回数 |
| jockey_venue_win_rate | 騎手×会場勝率 |
| trainer_win_rate | 調教師勝率 |
| trainer_top3_rate | 調教師複勝率 |
| trainer_race_count | 調教師出走回数 |
| trainer_venue_win_rate | 調教師×会場勝率 |
| jockey_trainer_pair_rate | 騎手×調教師組み合わせ勝率 |
| jockey_horse_pair_count | 騎手×馬のコンビ回数 |

#### 血統（8特徴量）
| 特徴量 | 説明 |
|--------|------|
| sire_avg_finish | 父産駒平均着順 |
| sire_win_rate | 父産駒勝率 |
| sire_distance_fit | 父×距離適性 |
| sire_surface_fit | 父×馬場適性 |
| damsire_avg_finish | 母父産駒平均着順 |
| damsire_win_rate | 母父産駒勝率 |
| damsire_distance_fit | 母父×距離適性 |
| damsire_surface_fit | 母父×馬場適性 |

#### ペース・脚質（V14-V15追加、12特徴量）
| 特徴量 | 説明 |
|--------|------|
| venue_expected_pace | 会場×距離の予測ペース |
| horse_pace_fit | 馬のペース適性 |
| front_runner_rate | 逃げ馬傾向 |
| position_improvement | ポジション改善度 |
| race_front_runner_count | レース内逃げ馬候補数（V15追加） |
| front_runner_competition | 逃げ馬競合スコア（V15追加） |
| post_style_conflict | 馬番×脚質不適合度（V15追加） |
| horse_relative_c4_position | 相対的4コーナー位置 |
| pace_style_match | ペース×脚質マッチ度 |
| horse_closing_kick | 差し脚性能 |
| horse_leading_ability | 先行力 |
| race_expected_pace | レースの予測ペース |

#### コース特徴量（V15追加、6特徴量）
| 特徴量 | 説明 |
|--------|------|
| course_corner_count | コーナー通過回数 |
| course_start_to_corner_m | スタートから最初のコーナーまでの距離 |
| course_final_straight_m | 最終直線距離 |
| course_slope_percent | 坂勾配率 |
| course_is_outer | 外回りフラグ |
| course_turn_direction | 回り方向(0=左, 1=右) |

#### 派生・交互作用特徴量（V15追加、5特徴量）
| 特徴量 | 説明 |
|--------|------|
| straight_ratio | 直線比率(直線距離/全距離) |
| is_long_straight | 長い直線フラグ(>450m) |
| style_straight_match | 差し馬×長直線マッチ度 |
| front_slope_disadvantage | 先行馬×坂不利度 |
| closer_long_straight_advantage | 差し馬×長直線アドバンテージ |

### 4.2 データリーク防止の実装

**排除した8種類のリーク源**:

1. `final_corner_to_finish` - 最終コーナー→ゴールの順位変動
2. `pace_index` - レースペース指数
3. `passing_order_1-4` - 通過順位（各コーナー）
4. `last_3f_time` - 上り3F
5. `time_except_last3f` - 上がり以外のタイム
6. `position_change_*` - 位置取り変動
7. `win_probability` - 確定オッズ由来の勝率
8. `finish_time_seconds` - ゴールタイム

**安全な累積統計パターン**:

```python
# ✅ 安全: cumsum + shift(1)
df['horse_cum_races'] = df.groupby('horse_id')['is_win'].transform(
    lambda x: x.notna().cumsum().shift(1).fillna(0)
)

# ❌ 危険: 全期間集計（未来リーク）
df['jockey_win_rate'] = df.groupby('jockey_id')['is_win'].mean()
```

---

## 5. モデル開発履歴（V7〜V20）

### 5.1 バージョン推移表

| Ver | 特徴量数 | Test ROI | Gap | 主な追加/変更 | 結論 |
|-----|----------|----------|-----|---------------|------|
| V7 | 33 | 79.2% | - | 基盤（リーク修正版） | 出発点 |
| V8.1 | 38 | 86.4% | 22.3% | クラス変動 | 効果あり |
| V10.2 | 41 | 82.0% | 18.9% | 上がり3F順位、C4位置 | 安定版 |
| V12 | 45 | 79.2% | 28.9% | クラス変動リーク修正 | 正常化 |
| V13 | 45 | 82.8% | 59.6% | 前走カテゴリ、Optuna | 改善 |
| V14 | 51 | 85.0% | 32.4% | ペース×脚質マッチ | 改善 |
| **V15** | **66** | **91.8%** | **30.7%** | **競合リスク、馬番適合、コース特徴** | **推奨** |
| V17 | 58 | 88.7% | 32.1% | 距離カテゴリ特化 | ✖ 劣化 |
| V19 | 60 | 87.6% | 35.1% | 騎手×馬相性 | ✖ 劣化 |
| V20 | 59 | 90.0% | 31.0% | 母父×生産者 | ✖ 劣化 |

### 5.2 V15パフォーマンス

| 指標 | 値 |
|------|-----|
| Train ROI | 122.5% |
| **Test ROI** | **91.8%** |
| Train-Test Gap | 30.7% |
| Test的中率 | 21.4% |
| 人気1位ベースライン | 77.9% |
| **V15優位性** | **+13.9%** |

### 5.3 V15ハイパーパラメータ

```python
params = {
    'objective': 'binary',
    'metric': 'auc',
    'verbosity': -1,
    'learning_rate': 0.03,
    'num_leaves': 20,
    'max_depth': 3,
    'min_child_samples': 100,
    'reg_alpha': 3.0,
    'reg_lambda': 5.0,
    'bagging_fraction': 0.7,
    'bagging_freq': 3,
    'feature_fraction': 0.7,
}
num_boost_round = 200
```

### 5.4 重大リーク修正履歴

#### V7リーク修正（2025-12-08）

**問題**: transform時に`finish_position`から`is_win`を計算し、累積統計に含めていた。

```python
# 修正前（リーク）
if 'finish_position' in df.columns:
    df.loc[new_mask, 'is_win'] = (df.loc[new_mask, 'finish_position'] == 1).astype(float)

# 修正後（リークフリー）
df.loc[new_mask, 'is_win'] = np.nan
```

**影響**: ROI 265.8% → 79.2%

---

## 6. 期待値(EV)戦略検証（2025-12-19実施）

### 6.1 検証背景

従来のV15着順予測モデル（ROI 91.8%）とは別のアプローチとして、期待値(EV)ベースの戦略を徹底検証。

**期待値計算式**:
```
EV = 予測勝率 × オッズ
```

### 6.2 発見された問題：穴馬への過大評価

EVモデルの詳細分析により、予測勝率が高オッズ馬を大幅に過大評価していることが判明。

| オッズ帯 | 予測勝率平均 | 実際の勝率 | 過大評価倍率 |
|----------|-------------|-----------|-------------|
| 1-5倍 | 68.1% | 23.9% | 2.8倍 |
| 5-10倍 | 52.9% | 10.6% | 5.0倍 |
| **10-20倍** | **40.7%** | **5.5%** | **7.4倍** |
| 20-50倍 | 31.1% | 2.3% | 13.5倍 |
| 50倍以上 | 23.8% | 0.8% | 29.8倍 |

### 6.3 予測Top1 vs EV Top1の乖離

| 指標 | 予測Top1（勝率最高） | EV Top1（期待値最高） |
|------|---------------------|---------------------|
| 平均オッズ | 21.8倍 | **182.5倍** |
| 平均予測勝率 | 81.1% | 54.9% |
| **実際の勝率** | **19.0%** | **1.2%** |
| 平均人気 | 3.2番人気 | **11.8番人気** |
| 両者の一致率 | - | **10.7%** |

> [!WARNING]
> **EVモデルの根本問題**: 穴馬（高オッズ）の勝率を過大評価することで、EV計算が非現実的な大穴馬を選択。結果として的中率1.2%、ROI大幅低下。

### 6.4 修正版EVモデルの結果

購入点数を「EV上位N点のみ」に制限した修正版を検証。

| 戦略 | 2022 | 2023 | 2024 | 平均ROI |
|------|------|------|------|---------|
| 単勝 EV-Top1 | 61.3% | 49.2% | 73.1% | 61.2% |
| 単勝 EV-Top2 | 58.2% | 56.8% | 69.7% | 61.6% |
| **複勝 Top1（着順予測）** | **82.9%** | **78.6%** | **80.0%** | **80.5%** |
| 複勝 EV-Top2 | 67.0% | 63.1% | 71.0% | 67.0% |

**結論**: 修正版EVモデルでも着順予測ベースの戦略を上回れない。

---

## 7. 予測勝率キャリブレーション実験

### 7.1 実験概要

予測勝率の過大評価問題を解決するため、PlattスケーリングとIsotonic Regressionを用いてキャリブレーションを実施。

**データ分割**:
- Train: 学習用（Y-8年〜Y-2年）
- Calibration: キャリブレータ学習用（Y-1年）
- Test: 評価用（Y年）

### 7.2 キャリブレーション効果

| 指標 | 生予測 | Platt | Isotonic |
|------|--------|-------|----------|
| EV>1.0の馬 | **70%** | **43%** | **42%** |
| EV平均 | 21.0 | 7.2 | 6.4 |

✅ キャリブレーションにより過剰なEV>1.0選択が70%→42%に改善

### 7.3 3年平均ROI比較（単勝）

| 戦略 | 2022 | 2023 | 2024 | **平均** | 標準偏差 |
|------|------|------|------|----------|----------|
| **着順予測Top1** | 82.0% | 66.2% | 75.6% | **74.6%** | 7.9% |
| Isotonic Top3 | 64.3% | 53.7% | 75.0% | 64.3% | 10.6% |
| Platt Top2 | 69.6% | 46.5% | 75.3% | 63.8% | 15.3% |
| 生予測 Top1 | 49.9% | 42.3% | 57.1% | 49.8% | 7.4% |

### 7.4 キャリブレーション実験の結論

> [!IMPORTANT]
> **着順予測モデルTop1（平均ROI 74.6%）が依然として最良**
>
> キャリブレーションにより:
> - 生予測(49.8%) → Isotonic Top3(64.3%)に改善
> - しかし着順予測Top1(74.6%)には**10%及ばない**
> - 年度間のバラつきも大きい（標準偏差10%以上）

---

## 8. 最終結論と推奨戦略

### 8.1 確定事項

1. **着順予測モデルTop1（平均ROI 74.6%）** が最も安定した戦略
2. **期待値(EV)モデルは不適切**: 穴馬への過大評価によりROI低下
3. キャリブレーションで改善するも、着順予測には未達
4. **オッズ帯フィルタリング**が唯一有効な改善手段
   - 単勝: Top1 AND 10-50倍 → ROI 113.7%

### 8.2 推奨戦略

```python
# 単勝戦略（4年平均ROI 113.7%）
def should_bet_tansho(pred_rank, win_odds):
    return pred_rank == 1 and 10 <= win_odds < 50

# 複勝戦略（V15 ROI 約80%）
def should_bet_fukusho(pred_rank):
    return pred_rank == 1
```

### 8.3 今後の方向性

1. **着順予測モデルの継続使用**
2. **特徴量改善**のための新データ探索
   - オッズ変動情報
   - 調教タイム
   - パドック情報
3. **オッズ帯戦略の精緻化**
   - 会場別・クラス別の分析
   - 複合馬券への応用

---

## 9. 技術スタックとディレクトリ構造

### 9.1 技術スタック

| カテゴリ | 技術 | バージョン |
|---------|------|-----------| 
| 言語 | Python | 3.10+ |
| 機械学習 | LightGBM, CatBoost | 4.0+, 1.2+ |
| データ処理 | pandas, NumPy | 2.0+, 1.24+ |
| ストレージ | Parquet (pyarrow) | 12.0+ |
| 最適化 | Optuna, scipy | 1.11+ |
| スクレイピング | requests, BeautifulSoup4, Selenium | - |
| ダッシュボード | Streamlit | 1.28+ |

### 9.2 ディレクトリ構造

```
KeibaAI_v2/
├── keibaai/
│   ├── src/
│   │   ├── features/
│   │   │   ├── leak_free_feature_engineer_v7.py   # 基盤版
│   │   │   ├── leak_free_feature_engineer_v15.py  # ★推奨（ROI 91.8%）
│   │   │   └── leak_free_feature_engineer_v17.py  # 最新版
│   │   └── modules/
│   │       ├── models/
│   │       ├── parsers/
│   │       ├── preparing/
│   │       └── sim/
│   ├── data/
│   │   └── parsed/parquet/
│   │       ├── races/races.parquet
│   │       ├── shutuba/shutuba.parquet
│   │       ├── horses/horses.parquet
│   │       ├── pedigrees/pedigrees.parquet
│   │       ├── returns/returns.parquet
│   │       ├── race_details/race_details.parquet
│   │       └── corners/corner_positions.parquet
│   └── models/
├── scripts/
│   ├── training/                    # 学習・検証スクリプト
│   │   ├── expected_value_calibrated.py  # キャリブレーション実験
│   │   ├── expected_value_model_v2.py    # 修正版EVモデル
│   │   └── test_v15_features.py
│   ├── verification/                # 検証スクリプト
│   ├── debug/
│   │   └── analyze_predicted_prob.py    # 予測勝率詳細分析
│   └── analysis/
└── docs/
    ├── system/                      # システムドキュメント
    └── knowledge/                   # 知見ドキュメント（本ファイル）
```

---

## 10. リーク・過学習防止ガイドライン

### 10.1 絶対使用禁止カラム

```python
FORBIDDEN_FEATURES = [
    # レース結果（直接リーク）
    'finish_position',      # ターゲット変数
    'finish_time_seconds',
    'last_3f_time',
    'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
    'margin_seconds',
    'pace_index',
    'prize_money',
    
    # オッズ・人気（間接リーク）
    'win_odds',             # sample_weightとしてのみ使用可
    'popularity',
    'win_probability',
]
```

### 10.2 安全な累積統計パターン

```python
# ✅ 正しい: cumsum() + shift(1)
df['horse_cum_wins'] = df.groupby('horse_id')['is_win'].transform(
    lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
)

# ❌ 間違い: shift(1)なし → 自レースの結果が混入
df['horse_cum_wins'] = df.groupby('horse_id')['is_win'].cumsum()

# ❌ 間違い: 全期間集計（未来リーク）
df['jockey_win_rate'] = df.groupby('jockey_id')['is_win'].mean()
```

### 10.3 Train/Testの分離ルール

```python
# ✅ 正しい: fit時はTrain期間のみ
engine = LeakFreeFeatureEngineerV15()
engine.fit(train, pedigrees, corners, race_details, horses_df=horses)

# fit時に計算された統計はTrain期間のデータのみを使用
# transform時はfit時の統計を使用（Test期間データは統計に含まれない）

# ❌ 間違い: 全期間でfit
engine.fit(all_data, ...)  # Test期間のデータが統計に含まれる
```

### 10.4 Gap許容基準

| 券種 | 許容Gap | V15 |
|------|---------|-----|
| 単勝 | < 40% | 30.7% ✅ |
| 複勝 | < 30% | 18.1% ✅ |
| 馬連 | < 50% | 63.4% ⚠️ |
| 三連複 | < 80% | 131.8% ❌ |

---

**文書終了**

**最終更新**: 2025-12-20  
**作成者**: KeibaAI_v2 Development Team  
**変更履歴**:
- 2025-12-20: 障害レース除外（12,934件）、全ファイル整合性統一（560,753件）、前処理パイプライン追加
- 2025-12-20: shutuba.parquet再生成（277,826→573,743件、期間2014-2025に統一）、パーサーにtitle日付抽出追加
- 2025-12-19: 生データ検証による修正（件数・カラム数・特徴量数を実データに合わせて更新）
- 2025-12-19: 初版作成（EV戦略検証結果、キャリブレーション実験結果を含む）
