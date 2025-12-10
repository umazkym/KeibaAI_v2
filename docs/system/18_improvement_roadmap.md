# KeibaAI_v2 今後の改修方針書

**作成日**: 2025-12-10  
**対象読者**: 第三者開発者、データサイエンティスト  
**目的**: 券種ごとに最適化されたモデル戦略による本質的なROI改善

---

## 📑 目次

1. [現状分析と課題](#1-現状分析と課題)
2. [改修の基本方針](#2-改修の基本方針)
3. [券種別モデル戦略](#3-券種別モデル戦略)
4. [単勝・複勝モデルの本質的改善](#4-単勝複勝モデルの本質的改善)
5. [複合馬券専用モデルの開発](#5-複合馬券専用モデルの開発)
6. [利用可能データからの特徴量開発](#6-利用可能データからの特徴量開発)
7. [リーク・過学習・過適合の防止策](#7-リーク過学習過適合の防止策)
8. [実装ロードマップ](#8-実装ロードマップ)
9. [データ仕様リファレンス](#9-データ仕様リファレンス)

---

## 1. 現状分析と課題

### 1.1 現行モデル（V15）のパフォーマンス

| 指標 | Train | Test | Gap |
|------|-------|------|-----|
| 単勝ROI | 122.5% | **91.8%** | 30.7% |
| 的中率 | 31.6% | 21.4% | - |
| 特徴量数 | 55 | - | - |
| モデル | LightGBM Binary Classification | - | - |

### 1.2 券種別ROI分析結果（2025年Test期間）

| 券種 | ROI | 的中率 | Train-Test Gap | 判定 |
|------|-----|--------|----------------|------|
| 馬単 | 94.7% | 3.9% | 60.1% | ⚠️ 過学習 |
| **単勝** | **91.5%** | 21.3% | 30.4% | ✅ 推奨 |
| **複勝** | **83.7%** | 47.1% | 18.1% | ✅ 安定 |
| 馬連 | 79.4% | 7.8% | 63.4% | ⚠️ 過学習 |
| ワイド | 75.3% | 17.1% | 51.2% | ⚠️ 過学習 |
| 三連単 | 63.5% | 0.9% | 178.2% | ❌ 深刻 |
| 三連複 | 45.2% | 3.8% | 131.8% | ❌ 深刻 |

### 1.3 本質的な課題

```
┌─────────────────────────────────────────────────────────────────┐
│              券種別パフォーマンス差の根本原因                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  【原因1】モデルが1着予測に最適化されている                       │
│  └─ V15は Binary Classification (is_win) で学習                │
│  └─ 2着・3着の予測精度が低い（Top3完全一致 3.8%）               │
│                                                                 │
│  【原因2】複合馬券は「順序」の予測精度が必要                      │
│  └─ 馬単: 1-2着の順序が必要                                    │
│  └─ 三連単: 1-2-3着の順序が必要                                │
│  └─ 現モデルは「1着か否か」のみを予測                          │
│                                                                 │
│  【原因3】複合馬券はTrain期間に過適合しやすい                    │
│  └─ 組み合わせ数が多く、偶然のパターンを学習                   │
│  └─ Gap 60-180% は深刻な過学習を示唆                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 改修の基本方針

### 2.1 核心原則：券種ごとにモデルを最適化する

> [!IMPORTANT]
> **閾値設定によるROI向上（EV > 1.0 のみにベットする等）は表面的な改善であり、本質的なモデル改善ではない。**
> 購入数を減らして見かけ上のROIを上げるのではなく、**予測精度そのものを向上させる**ことを目指す。

### 2.2 券種別の改修アプローチ

| 券種カテゴリ | 現状 | 課題 | 改修方針 |
|------------|------|------|---------|
| **単勝・複勝** | ROI 83-91% | あと10%不足 | ラベル・特徴量の洗練 |
| **2頭組合せ** | ROI 75-94% | Gap 50-60% | 過学習対策 + 2着専用モデル |
| **3頭組合せ** | ROI 45-63% | Gap 130-180% | 根本的再設計が必要 |

### 2.3 優先順位の明確化

1. **オッズ・人気を特徴量に使用しない**（リーク）
2. **着順・タイム・上がり3Fを特徴量に使用しない**（リーク）
3. **まず本質的なモデル改善を行う**（EV閾値戦略は後のフェーズで実施）

> [!NOTE]
> EV閾値による購入数絞り込みは有効な戦略だが、それは予測精度を上げた**後に**実施する。
> 予測精度が低いままEV閾値を設けても、根本的な改善にはならない。

---

## 3. 券種別モデル戦略

### 3.1 戦略概要

```
┌─────────────────────────────────────────────────────────────────┐
│                    券種別モデル設計                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  【モデルA】単勝専用モデル                                       │
│  ├─ 目的変数: is_win (1着か否か)                               │
│  ├─ 手法: Binary Classification (AUC最適化)                    │
│  └─ 正則化: max_depth=3, num_leaves=20                         │
│                                                                 │
│  【モデルB】複勝専用モデル                                       │
│  ├─ 目的変数: is_place (3着以内か否か)                         │
│  ├─ 手法: Binary Classification                                │
│  └─ 特徴: 複勝向けラベルで再学習                               │
│                                                                 │
│  【モデルC】ランキングモデル（馬連・馬単・ワイド用）              │
│  ├─ 目的変数: 関連度 [1着=5, 2着=4, 3着=3, 4-5着=1, 他=0]       │
│  ├─ 手法: LambdaRank (NDCG最適化)                             │
│  └─ 正則化: max_depth=2, num_leaves=4（強化）                  │
│                                                                 │
│  【モデルD】三連系モデル（三連複・三連単用）                      │
│  ├─ 手法: アンサンブル(モデルA + モデルC)                      │
│  └─ または: Monte Carlo シミュレーション                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 券種とモデルのマッピング

| 券種 | 使用モデル | 予測対象 | 備考 |
|------|-----------|---------|------|
| 単勝 | モデルA | 1着の馬 | 現行V15を基盤 |
| 複勝 | モデルB | 3着以内の馬 | ラベル変更で再学習 |
| 馬連 | モデルC | Top2の組合せ | 順不同 |
| 馬単 | モデルC | Top1→Top2 | 順序あり |
| ワイド | モデルC | Top2のいずれか3着以内 | 緩い条件 |
| 三連複 | モデルD | Top3の組合せ | アンサンブル |
| 三連単 | モデルD | Top1→Top2→Top3 | 最も困難 |

---

## 4. 単勝・複勝モデルの本質的改善

### 4.1 単勝モデル（モデルA）の改善

#### 課題
- 現状Test ROI 91.5%（目標100%まであと8.5%）
- 的中率21.3%は妥当だが、的中時の平均配当が控除率を上回っていない

#### 改善施策1: ラベルの重み付け学習

**概念**: 高配当の1着を重視して学習させる

```python
# 重み計算（学習時のみ。特徴量として使用禁止）
# 確定オッズ(win_odds)を使用するが、特徴量ではなくsample_weight
train_df['sample_weight'] = np.log1p(train_df['win_odds'])

# LightGBM学習
train_ds = lgb.Dataset(
    X_train, 
    y_train,
    weight=train_df['sample_weight']
)
model = lgb.train(params, train_ds, num_boost_round=200)
```

> [!WARNING]
> `win_odds`は**sample_weight**としてのみ使用。特徴量としては絶対に使用禁止。

#### 改善施策2: 特徴量の重要度分析と選別

```python
# V15特徴量の重要度を確認し、低重要度を除外
importance = model.feature_importance(importance_type='gain')
low_importance = [col for col, imp in zip(feature_cols, importance) if imp < threshold]

# 低重要度特徴量を除外して再学習
# → 過学習を軽減しつつ予測精度を維持
```

#### 改善施策3: ハイパーパラメータ最適化（ROI基準）

```python
import optuna

def objective(trial):
    params = {
        'max_depth': trial.suggest_int('max_depth', 2, 5),
        'num_leaves': trial.suggest_int('num_leaves', 4, 31),
        'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.1, 10.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.1, 10.0),
    }
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # 評価指標: Test ROI（AUCではない）
    test_roi = calculate_roi(valid_df, model)
    return test_roi

study = optuna.create_study(direction='maximize')
study.optimize(objective, n_trials=100)
```

### 4.2 複勝モデル（モデルB）の開発

#### 現状の問題
- 単勝モデル（1着予測）の出力を複勝に流用している
- 複勝は「3着以内」であり、2着・3着馬も対象

#### 改善施策: 複勝専用モデルの構築

```python
# ラベル変更: 3着以内を正例とする
y_place = (train_df['finish_position'] <= 3).astype(int)

# Binary Classification
params = {
    'objective': 'binary',
    'metric': 'auc',
    'max_depth': 3,
    'num_leaves': 15,
    'min_child_samples': 100,
}

train_ds = lgb.Dataset(X_train, y_place)
place_model = lgb.train(params, train_ds, num_boost_round=200)
```

**期待効果**:
- 複勝のROI向上（現状83.7% → 目標90%+）
- 2着・3着馬の予測精度向上

---

## 5. 複合馬券専用モデルの開発

### 5.1 課題分析

| 券種 | 必要な予測 | 現モデルの問題 |
|------|-----------|---------------|
| 馬連 | Top2（順不同） | 2着予測精度が低い |
| 馬単 | Top1→Top2（順序） | 順序精度が不十分 |
| ワイド | Top2のいずれかが3着以内 | モデルBで代用可能か |
| 三連複 | Top3（順不同） | 3着予測精度が極めて低い |
| 三連単 | Top1→Top2→Top3（順序） | 事実上ランダムに近い |

### 5.2 モデルC: LambdaRankによる順位学習

```python
# 関連度ラベル設計
def create_relevance_label(finish_pos):
    """
    1着: 5点（最重要）
    2着: 4点
    3着: 3点
    4-5着: 1点（掲示板入り）
    6着以下: 0点
    """
    if finish_pos == 1: return 5
    elif finish_pos == 2: return 4
    elif finish_pos == 3: return 3
    elif finish_pos <= 5: return 1
    else: return 0

train_df['relevance'] = train_df['finish_position'].apply(create_relevance_label)

# グループ（レース単位）
groups = train_df.groupby('race_id').size().values

# LambdaRankモデル
model = lgb.LGBMRanker(
    objective='lambdarank',
    metric='ndcg',
    max_depth=2,          # かなり浅い木（過学習対策）
    num_leaves=4,         # 少ない葉
    min_child_samples=200,
    reg_alpha=10.0,
    reg_lambda=15.0,
    subsample=0.4,
    colsample_bytree=0.4,
)
model.fit(X_train, train_df['relevance'], group=groups)
```

### 5.3 過学習対策（複合馬券向け）

**問題**: Train-Test Gap が 50-180% と深刻

**対策1: 極端な正則化**

```python
# 過学習を抑制するパラメータ
params = {
    'max_depth': 2,           # 最も浅い木
    'num_leaves': 4,          # 最小限の葉
    'min_child_samples': 300, # 大きなサンプル数要件
    'subsample': 0.3,         # 低いサンプリング率
    'colsample_bytree': 0.3,  # 低い特徴量サンプリング
    'reg_alpha': 15.0,        # 強いL1正則化
    'reg_lambda': 20.0,       # 強いL2正則化
}
```

**対策2: 特徴量削減**

```python
# 重要度上位N個のみを使用
top_n = 20  # 55特徴量から20個に削減
important_features = importance_df.head(top_n)['feature'].tolist()
X_train_reduced = X_train[important_features]
```

**対策3: 時系列交差検証**

```python
from sklearn.model_selection import TimeSeriesSplit

tscv = TimeSeriesSplit(n_splits=5)
for train_idx, val_idx in tscv.split(X):
    # 過去データでtrain、未来データでvalidate
    model.fit(X[train_idx], y[train_idx])
    roi = evaluate(model, X[val_idx], y[val_idx])
```

### 5.4 三連系モデル（モデルD）の検討

> [!CAUTION]
> **三連系（三連複・三連単）は現状ROI 45-63%であり、100%達成は困難。**
> Top3完全一致率3.8%は、ランダム予測（約0.1%）より高いが、控除率25-27.5%を上回るには不十分。

**推奨アプローチ**: 
1. **短期**: 三連系は「見送り」または「少額実験」に留める
2. **中期**: モデルA + モデルCのアンサンブル
3. **長期**: μ/σ/νモデルによるMonte Carloシミュレーション

---

## 6. 利用可能データからの特徴量開発

### 6.1 データ利用可能性の確認結果

| データソース | カラム | 実データ状況 | 利用可否 |
|-------------|--------|------------|---------|
| shutuba.parquet | morning_odds | **全件NULL** | ❌ 不可 |
| shutuba.parquet | career_stats | **全件NULL** | ❌ 不可 |
| shutuba.parquet | last_5_finishes | **全件NULL** | ❌ 不可 |
| race_details.parquet | first_half/second_half | 19,427件（有効） | ✅ 可能 |
| corner_positions.parquet | gap_from_leader | 811,415件（有効） | ✅ 可能 |
| horses.parquet | breeder_name | 有効 | △ 効果薄（V20で検証済み） |

> [!WARNING]
> **shutuba.parquetのmorning_odds, career_stats, last_5_finishesは全件NULLであり、特徴量として使用不可能。**
> parquet_contents_report.mdにはカラムとして記載されているが、実データは存在しない。

### 6.2 実際に活用可能な特徴量開発

#### 6.2.1 race_details.parquet: 予測ペース特徴量

**データ構造**:
- `first_half`: 前半3Fタイム（秒）
- `second_half`: 後半3Fタイム（秒）
- 19,427レース分のデータ

**特徴量案**:

```python
# レースの予測ペース（会場×距離×馬場ごとの平均）
pace_stats = race_details.merge(races[['race_id', 'venue', 'distance_m', 'track_condition']])
venue_pace = pace_stats.groupby(['venue', 'distance_category', 'track_condition'])[['first_half', 'second_half']].mean()

# ペース傾向（前傾/後傾）
df['expected_pace_type'] = df.apply(lambda x: venue_pace.loc[(x['venue'], x['dist_cat'], x['condition']), 'first_half'] - venue_pace.loc[...], axis=1)
```

**リークフリー実装**:
- 予測対象レースより前のデータのみで統計計算
- `.shift(1)` 不要（レース単位の統計なので）

#### 6.2.2 corner_positions.parquet: コーナーポジション特徴量

**データ構造**:
- 811,415件（レース×馬×コーナー）
- `gap_from_leader`: 先頭からの馬身差（平均5.9馬身）

**V15で既に実装済みの特徴量**:
- `horse_c4_gap_avg`: 4コーナー馬身差平均
- `front_runner_competition`: 逃げ馬競合数

**追加特徴量候補**:

```python
# 馬ごとのコーナーポジション安定性
corners_by_horse = corners.groupby(['horse_id', 'race_id'])['position'].mean()
df['horse_position_std'] = df.groupby('horse_id')['c4_position'].transform(
    lambda x: x.rolling(5).std().shift(1)  # 過去5走の安定性
)

# 先行力スコア（過去レースでの平均ポジション）
df['horse_front_tendency'] = df.groupby('horse_id')['c4_position'].transform(
    lambda x: x.expanding().mean().shift(1)
)
```

### 6.3 horses.parquet: 馬マスタ情報

**利用可能カラム**:
- `breeder_name`: 生産者名（ノーザンファーム等）
- `producing_area`: 産地（浦河町、日高町等）
- `birth_date`: 生年月日

**V20実験結果**: 生産者別勝率（breeder_win_rate）は**効果なし**と判明（V15より劣化）

**活用可能性が残る特徴量**:

```python
# 産地×距離適性（産地ごとの距離カテゴリ別平均着順）
area_dist_perf = races.merge(horses[['horse_id', 'producing_area']])
area_dist_stats = area_dist_perf.groupby(['producing_area', 'distance_category'])['finish_position'].mean()

# 月齢（レース時点での月齢）
df['horse_age_months'] = (df['race_date'] - df['birth_date']).dt.days / 30
```

---

## 7. リーク・過学習・過適合の防止策

### 7.1 データリーク防止チェックリスト

```markdown
□ 1. オッズ・人気を特徴量に含めていないか
□ 2. 着順（finish_position）を直接参照していないか
□ 3. 上がり3F（last_3f_time）を参照していないか
□ 4. 通過順位（passing_order_1-4）を参照していないか
□ 5. 累積統計に .shift(1) を適用しているか
□ 6. Test期間のデータがTrain統計に混入していないか
□ 7. 同日の他レース結果を使用していないか
```

### 7.2 安全な計算パターン

```python
# ✅ 安全: cumsum() + shift(1)
df['horse_cum_wins'] = df.groupby('horse_id')['is_win'].transform(
    lambda x: x.fillna(0).cumsum().shift(1).fillna(0)
)

# ✅ 安全: Train期間固定の統計
train_stats = train_df.groupby('sire_id')['is_win'].mean()
df['sire_win_rate_fixed'] = df['sire_id'].map(train_stats)

# ❌ 危険: 全期間集計
df['jockey_win_rate'] = df.groupby('jockey_id')['is_win'].mean()
```

### 7.3 券種別の許容Gap基準

| 券種 | 許容Gap | 理由 |
|------|---------|------|
| 単勝 | < 40% | 現状30.7%で許容範囲 |
| 複勝 | < 30% | 高い的中率のため厳しく |
| 馬連・馬単 | < 50% | 組み合わせ数が多いため緩め |
| 三連複・三連単 | < 80% | 現状130-180%は受け入れ不可 |

---

## 8. 実装ロードマップ

### 8.1 Phase 1（1-2週間）: 単勝・複勝の本質的改善

| # | タスク | 成果物 |
|---|--------|--------|
| 1 | 複勝専用モデル（モデルB）の開発 | train_place_model.py |
| 2 | 単勝モデルのオッズ加重学習 | train_tansho_weighted.py |
| 3 | ハイパーパラメータ最適化（ROI基準） | optuna_roi_tuning.py |
| 4 | 結果比較・ドキュメント化 | docs/reports/ |

**成功基準**:
- 単勝Test ROI: 91.8% → 95%+
- 複勝Test ROI: 83.7% → 88%+
- Gap維持: < 40%

### 8.2 Phase 2（2-4週間）: 複合馬券モデルの開発

| # | タスク | 成果物 |
|---|--------|--------|
| 5 | LambdaRankモデル（モデルC）の開発 | train_ranker_model.py |
| 6 | 過学習対策の強化 | 正則化パラメータ最適化 |
| 7 | 馬連・馬単・ワイドROI検証 | multi_bet_roi_v16.py |
| 8 | 特徴量削減実験 | feature_selection.py |

**成功基準**:
- 馬連Test ROI: 79.4% → 85%+
- Gap: 63.4% → < 50%

### 8.3 Phase 3（1-2ヶ月）: 特徴量発掘と統合

| # | タスク | 成果物 |
|---|--------|--------|
| 9 | 予測ペース特徴量の開発 | race_details活用 |
| 10 | コーナー特徴量の拡張 | corner_positions活用 |
| 11 | 特徴量有効性検証 | 1つずつ追加・検証 |
| 12 | V16特徴量エンジニアの完成 | leak_free_feature_engineer_v16.py |

### 8.4 Phase 4（モデル改善完了後）: EV閾値戦略の実装

**前提条件**: Phase 1-3でモデルの予測精度が向上した後に実施

| # | タスク | 成果物 |
|---|--------|--------|
| 13 | 確率キャリブレーション | Isotonic Regression |
| 14 | EV計算ロジック実装 | ev_calculator.py |
| 15 | 閾値最適化実験 | ev_threshold: 1.0, 1.1, 1.2, 1.3 |
| 16 | バックテスト検証 | ev_strategy_backtest.py |

**EV計算式**:
```python
# 期待値 = 予測確率 × オッズ × 払戻率
EV = predicted_prob * win_odds * 0.80  # 単勝払戻率80%

# EV > 閾値 の馬のみにベット
if EV > threshold:
    bet(horse)
```

**期待効果**:
- 購入数は減少するが、ROIが大幅向上
- Phase 1-3で予測精度が向上していれば、EV > 1.0 の馬の割合が増加

---

## 9. データ仕様リファレンス

### 9.1 主要Parquetファイル

| ファイル | 件数 | 主キー | 備考 |
|---------|------|--------|------|
| races.parquet | 277,826 | (race_id, horse_number) | メイン |
| shutuba.parquet | 277,826 | (race_id, horse_number) | ※朝オッズ等はNULL |
| horses.parquet | ~35,000 | horse_id | 馬マスタ |
| pedigrees.parquet | 2,043,151 | (horse_id, ancestor_id) | 血統 |
| returns.parquet | 240,333 | (race_id, bet_type) | 払戻 |
| race_details.parquet | 20,157 | race_id | ペース |
| corner_positions.parquet | 811,415 | (race_id, corner, horse_number) | 馬身差 |

### 9.2 使用禁止カラム（リーク源）

```python
FORBIDDEN_FEATURES = [
    'finish_position',      # ターゲット
    'finish_time_seconds',  # 結果
    'last_3f_time',         # 結果
    'passing_order_1-4',    # 結果
    'margin_seconds',       # 結果
    'win_odds',             # 確定オッズ
    'popularity',           # 確定人気
    'pace_index',           # 結果
    'prize_money',          # 結果
]
```

### 9.3 shutuba.parquet 実データ状況

| カラム | 非NULL件数 | 備考 |
|--------|-----------|------|
| race_id～trainer_id | 277,826 | ✅ 有効 |
| horse_weight | 276,837 | ✅ 有効 |
| morning_odds | **0** | ❌ 全件NULL |
| morning_popularity | **0** | ❌ 全件NULL |
| career_stats | **0** | ❌ 全件NULL |
| last_5_finishes | **0** | ❌ 全件NULL |

---

## 補足: V7→V15の進化履歴

| バージョン | 特徴量数 | Test ROI | 主な変更 |
|-----------|----------|----------|---------|
| V7 | 33 | 79.2% | 基盤（リーク修正版） |
| V8.1 | 38 | 86.4% | クラス変動追加 |
| V10.2 | 41 | 82.0% | 安定版 |
| V14 | 51 | 85.0% | ペース×脚質 |
| **V15** | **55** | **91.8%** | **競合リスク×ポジショニング（推奨）** |
| V17-V20 | 58-60 | 87-90% | ✖ 全て劣化 |

> [!TIP]
> **V17-V20の教訓**: 新特徴量の追加は必ずしもROI向上に繋がらない。
> 効果がなければ即時ロールバックする姿勢が重要。

---

**文書終了**

**最終更新**: 2025-12-10  
**作成者**: KeibaAI_v2 Development Team

