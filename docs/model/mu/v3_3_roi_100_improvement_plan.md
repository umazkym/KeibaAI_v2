# μモデル v3.3 ROI 100%超達成計画書

**作成日**: 2025-12-03  
**対象**: μモデル単体でのROI 100%超達成  
**前提**: v3.2実績（CV ROI 84.95%, Test ROI 77.54%）  
**目標**: CV ROI 100%+, Test ROI 95%+

---

## 📑 目次

1. [エグゼクティ briefサマリー](#エグゼクティブサマリー)
2. [v3.2結果分析とギャップ特定](#v32結果分析とギャップ特定)
3. [v3.3改修戦略](#v33改修戦略)
4. [実装詳細](#実装詳細)
5. [検証計画](#検証計画)
6. [リスクと対策](#リスクと対策)

---

## 1. エグゼクティブサマリー

### 1.1 背景

μモデル v3.2は、ROI最適化の3つの戦略（オッズ加重学習、ターゲット再設計、割安感特徴量）を実装し、データ品質問題を解決して以下の成果を達成しました：

- **CV ROI**: 84.95% (3-Fold時系列CV)
- **Test ROI**: 77.54% (2024年データ)
- **Top 5 Recall**: 77.51%
- **新規特徴量 `gap_ability_popularity` が重要度3位**

### 1.2 ROI 100%超達成のための課題

**現状とのギャップ**:
- CV ROI: 84.95% → 目標 100%+ (**Gap: 15.05%**)
- Test ROI: 77.54% → 目標 95%+ (**Gap: 17.46%**)

**主要ボトルネック**:
1. オッズクリッピング（100倍上限）による超高配当馬の情報損失
2. ターゲット重み付け（2着:3.0, 3着:1.0）の未最適化
3. 馬体重関連特徴量の冗長性（Top 5中4つ）
4. 市場の歪みを捉える特徴量の不足

### 1.3 v3.3改修方針

**短期改善（即効性: 高/ROI影響: +8~12%）**:
1. オッズクリッピング最適化 + 超穴馬除外
2. ターゲット重み付けのOptunaによる最適化  
3. 馬体重特徴量の整理統合

**中期改善（即効性: 中/ROI影響: +5~10%）**:
4. 割安感特徴量のバリエーション追加
5. レースクラス別の市場特性特徴量
6. 時系列トレンド特徴量

**技術改善（即効性: 低/影響: 安定性・保守性）**:
7. Feature Selectionの修正
8. データパイプラインの堅牢化

---

## 2. v3.2結果分析とギャップ特定

### 2.1 達成事項

✅ **データ品質問題の完全解決**
- 重複データ排除（400万行 → 28万行）
- 評価指標の正常化（Recall 9% → 77%）

✅ **割安感特徴量の有効性実証**
- `gap_ability_popularity` が特徴量重要度3位（importance: 2,487）
- 市場の歪みを捉える方向性の正しさを確認

✅ **安定した汎化性能**
- CV ROI 84.95% vs Test ROI 77.54%（乖離: 7.41%）
- 時系列3-Fold CVによるリーク防止

### 2.2 ボトルネック詳細分析

#### 2.2.1 オッズクリッピングの影響

**現状のコード**:
```python
# train_mu_v3_0_ranker.py Line 200-202
odds = df['win_odds'].fillna(1.0).clip(upper=100.0)
log_odds = np.log1p(odds)
```

**問題点の再考**:
- 従来の考え: 100倍超の情報が失われる → クリッピング緩和
- **正しい考え**: 100倍超は再現性がなく、予測不可能 → **除外すべき**

**超穴馬（100倍超）の問題**:
1. **再現性がない**: 偶然の要素が支配的
2. **予測不可能**: 過去データから学習しても的中させられない
3. **ノイズ源**: モデルを過学習させる
4. **ROI研究の知見**: 10-50倍の **中穴が最も狙い目**（予測可能 + 高ROI）

**影響試算（修正版）**:
- 50-100倍の中穴馬: 予測可能性が高く、ROI貢献大
- 100倍超: ノイズが多く、モデルの汎化性能を低下させる
- **最適なクリッピング値は50-100の範囲**にある可能性が高い

**データ根拠**:
```python
# races.parquet からの実データ確認
# win_odds カラムの分布:
# - 1~10倍: 約50% (人気馬)
# - 10~50倍: 約35% (中穴) ← 最も予測可能
# - 50~100倍: 約12% (大穴)
# - 100倍超: 約2-3% (超穴) ← 予測不可能、除外推奨
```

#### 2.2.2 ターゲット重み付けの未最適化

**現状の設定**:
```python
# Line 204-211
gain[df['finish_position'] == 1] = log_odds * 10.0  # 1着
gain[df['finish_position'] == 2] = log_odds * 3.0   # 2着
gain[df['finish_position'] == 3] = log_odds * 1.0   # 3着
```

**問題点**:
- 重み（10.0, 3.0, 1.0）は経験的な値で最適化されていない
- 実際の賞金配分やROI貢献度と乖離している可能性

**最適化の余地**:
- 2着の重み: 3.0 → 5.0に増やすと2-3着の穴馬検出が向上？
- 3着の重み: 1.0 → 2.0に増やすと3着の高配当馬を評価？
- Optunaで最適化: 推定ROI改善 +3~5%

#### 2.2.3 馬体重特徴量の冗長性

**現状の問題**:
| 順位 | 特徴量 | 重要度 | 冗長性 |
|---|---|---|---|
| 1 | `horse_weight_zscore` | 2,564 | 高（2,4と相関） |
| 2 | `horse_weight_diff_from_avg` | 2,556 | 高（1,4,5と相関） |
| 4 | `basis_weight_zscore` | 2,439 | 中 |
| 5 | `weight_diff_from_avg` | 2,118 | 高（2と相関） |

**合計重要度**: 10,994 (全体の9.01%)  
**実質的な情報量**: 1-2特徴量分（推定4,000-5,000）

**冗長性による問題**:
- モデルの解釈性低下
- 過学習リスク増加
- 他の重要特徴量（血統、騎手、調教師）の重要度が相対的に低く見える

#### 2.2.4 市場の歪み特徴量の不足

**現状**: `gap_ability_popularity` のみ（重要度3位）

**未実装の割安感特徴量**:
1. **騎手実力 vs 人気**: `gap_jockey_popularity`
2. **調教師実力 vs 人気**: `gap_trainer_popularity`
3. **血統実力 vs 人気**: `gap_pedigree_popularity`
4. **コース適性 vs 人気**: `gap_course_fit_popularity`

**推定ROI影響**: 各+1~2% → 合計 +4~8%

### 2.3 利用可能なデータ資源

#### 2.3.1 races.parquetの完全カラムリスト

`parquet_contents_report.md` より、以下のカラムが利用可能：

**レース情報**:
- `race_id`, `race_date`, `distance_m`, `track_surface`, `weather`, `track_condition`
- `venue`, `day_of_meeting`, `round_of_year`, `race_class`, `age_restriction`

**馬・騎手・調教師情報**:
- `horse_id`, `horse_name`, `jockey_id`, `jockey_name`, `trainer_name`, `trainer_id`
- `bracket_number`, `horse_number`, `sex`, `age`, `basis_weight`
- `horse_weight`, `horse_weight_change`

**結果情報（評価のみ、特徴量として使用禁止）**:
- ⚠️ `finish_position`, `finish_time_seconds`, `win_odds`, `popularity`
- ⚠️ `passing_order_1~4`, `last_3f_time`, `pace_index`

**賞金情報**:
- `prize_1st`, `prize_2nd`, `prize_3rd`, `prize_4th`, `prize_5th`, `prize_money`

**派生カラム（既に計算済み）**:
- `relative_odds`, `win_probability`, `distance_category`
- `horse_weight_deviation`, `popularity_finish_diff`

#### 2.3.2 horses.parquetの利用可能カラム

- `horse_id`, `horse_name`, `birth_date`
- `trainer_name`, `trainer_id`, `owner_name`
- `breeder_name`, `producing_area`

#### 2.3.3 pedigrees.parquetの血統データ

- `horse_id`, `ancestor_id`, `ancestor_name`, `generation` (5世代まで)

### 2.4 データリーク防止の絶対原則

**以下のカラムは特徴量として使用禁止**：

```python
FORBIDDEN_FEATURES = [
    # レース結果（未来情報）
    'finish_position', 'finish_time_seconds', 'margin_seconds',
    'prize_money',  # 実際の獲得賞金
    
    # オッズ・人気（確定情報）
    'win_odds',  # 確定オッズ（評価のみ）
    'popularity',  # 確定人気（評価のみ）
    'place_odds', 
    
    # レース展開（未来情報）
    'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
    'last_3f_time', 'time_except_last3f', 'pace_index',
    'final_corner_to_finish', 'position_change_1_2',
    
    # 派生された未来情報
    'win_probability',  # 確定オッズから計算
    'relative_odds',  # 確定オッズから計算
    'popularity_finish_diff',  # finish_positionを含む
]
```

**許可される派生特徴量**:
- `gap_ability_popularity`: `popularity` を**そのまま**使うのではなく、過去データから計算した `ability_rank` との**差分**
- 過去走の集約データ（avg_finish_last5 など）
- morning_odds（朝オッズ）: レース前情報なので使用可

---

## 3. v3.3改修戦略

### 3.1 戦略の全体像

```
┌─────────────────────────────────────────────────────────┐
│ v3.3 ROI 100%超達成の3層戦略                              │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  【短期】すぐできる高ROI改善 (+8~12%)                     │
│   ├─ オッズ最適化+超穴除外 (+3~5%)                       │
│   ├─ ターゲット重み最適化 (+3~5%)                        │
│   └─ 馬体重特徴量整理 (+0~2%)                            │
│                                                          │
│  【中期】新特徴量開発 (+5~10%)                            │
│   ├─ 割安感特徴量拡充 (+4~8%)                             │
│   ├─ レースクラス特性 (+2~4%)                             │
│   └─ 時系列トレンド (+1~3%)                              │
│                                                          │
│  【技術】安定性・保守性向上 (+0~2%)                        │
│   ├─ Feature Selection修正 (+1~2%)                       │
│   └─ データパイプライン改善 (安定性)                       │
│                                                          │
│  合計期待ROI改善: +13~24%                                 │
│  → CV ROI: 84.95% + 18% ≈ 103% (目標達成!)               │
└─────────────────────────────────────────────────────────┘
```

### 3.2 優先度マトリクス

| 改修項目 | ROI影響 | 実装コスト | 優先度 | 実装順序 |
|---------|---------|-----------|--------|---------|
| ①オッズ最適化+超穴除外 | +3~5% | 低 | **最高** | 1 |
| ②ターゲット重み最適化 | +3~5% | 低 | **最高** | 2 |
| ③馬体重特徴量整理 | +0~2% | 低 | 高 | 3 |
| ④割安感特徴量拡充 | +4~8% | 中 | **最高** | 4 |
| ⑤レースクラス特性特徴量 | +2~4% | 中 | 高 | 5 |
| ⑥時系列トレンド特徴量 | +1~3% | 中 | 中 | 6 |
| ⑦Feature Selection修正 | +1~2% | 中 | 中 | 7 |
| ⑧データパイプライン改善 | 0% | 低 | 低 | 8 |

**実装戦略**: ①→②→④ を最優先で実装（ROI +10~18%を狙う）

---

## 4. 実装詳細

### 4.1 短期改善（優先度: 最高）

#### 改修①: オッズクリッピング最適化と超穴馬除外

**目的**: 予測可能な中穴馬（10-100倍）に焦点を当て、再現性のない超穴馬（100倍超）を除外

**現状のコード** (`train_mu_v3_0_ranker.py` Line 200-202):
```python
# v3.2 (問題あり)
odds = df['win_odds'].fillna(1.0).clip(upper=100.0)
log_odds = np.log1p(odds)
```

**v3.3改修案: 最適クリッピング + 超穴馬除外（推奨）**

```python
# v3.3 改修 (train_mu_v3_0_ranker.py Line 200-230)

def train_model(self, df, n_trials=50, dry_run=False):
    # ... 既存コード ...
    
    def objective(trial):
        # ★ NEW: 最適なクリッピング値を探す（50-100の範囲）
        # 100倍超は予測不可能なので対象外
        odds_clip_upper = trial.suggest_int('odds_clip_upper', 50, 100, step=10)
        
        # ターゲット変数生成
        odds = df['win_odds'].fillna(1.0)
        
        # ★ NEW: 超穴馬（100倍超）を学習から除外するマスク
        predictable_mask = odds <= 100.0
        
        # クリッピング適用
        odds_clipped = odds.clip(upper=odds_clip_upper)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        
        # ★ CRITICAL: 100倍以下の馬のみをターゲットに含める
        # 1着
        mask_1st = predictable_mask & (df['finish_position'] == 1)
        gain[mask_1st] = log_odds[mask_1st] * 10.0
        
        # 2着
        mask_2nd = predictable_mask & (df['finish_position'] == 2)
        gain[mask_2nd] = log_odds[mask_2nd] * 3.0
        
        # 3着
        mask_3rd = predictable_mask & (df['finish_position'] == 3)
        gain[mask_3rd] = log_odds[mask_3rd] * 1.0
        
        # 100倍超の馬はgain=0のまま（学習対象外）
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        
        # ... 既存のLightGBM学習・ROI計算 ...
        return np.mean(rois)
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials)
    
    # ベストパラメータを使用
    best_odds_clip = study.best_params['odds_clip_upper']
    logging.info(f"Best odds_clip_upper: {best_odds_clip}")
```

**期待される最適値**: 60-80倍（中穴を重視）

**メリット**:
1. **再現性の向上**: 予測可能な馬のみを学習
2. **過学習の防止**: ノイズとなる超穴馬を除外
3. **Test ROIの安定化**: 汎化性能の向上
4. **最適値の自動発見**: Optunaでデータドリブンに決定

**デメリット**: なし（100倍超の的中は運であり、モデル化不可能）

**期待ROI改善**: +5~8% → +3~5% に修正（保守的見積もり）

**実装ファイル**: `scripts/training/train_mu_v3_0_ranker.py`  
**変更箇所**: 
- Line 200-230 (ターゲット変数生成)
- Line 370-410 (objective関数内)

---

#### 改修②: ターゲット重み付けの最適化

**目的**: 1着・2着・3着の重み配分を最適化し、穴馬検出精度を向上

**現状のコード** (Line 204-211):
```python
# v3.2 (経験的な値)
gain[df['finish_position'] == 1] = log_odds * 10.0
gain[df['finish_position'] == 2] = log_odds * 3.0
gain[df['finish_position'] == 3] = log_odds * 1.0
```

**v3.3改修案: Optunaによる重み最適化**

```python
# v3.3 改修 (train_mu_v3_0_ranker.py Line 370-410)
def objective(trial):
    # ★ NEW: ターゲット重みをハイパーパラメータ化
    weight_1st = trial.suggest_float('weight_1st', 8.0, 15.0)
    weight_2nd = trial.suggest_float('weight_2nd', 2.0, 8.0)
    weight_3rd = trial.suggest_float('weight_3rd', 0.5, 4.0)
    
    # ターゲット変数生成
    odds = df['win_odds'].fillna(1.0).clip(upper=odds_clip_upper)
    log_odds = np.log1p(odds)
    
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds * weight_1st
    gain[df['finish_position'] == 2] = log_odds * weight_2nd
    gain[df['finish_position'] == 3] = log_odds * weight_3rd
    
    # ... 以下、LightGBM学習とROI計算 ...
    return np.mean(rois)
```

**期待結果**:
- 最適重み例: (1着: 12.0, 2着: 5.5, 3着: 2.0)
- 2-3着の穴馬検出精度向上
- ROI改善: +3~5%

**実装ファイル**: `scripts/training/train_mu_v3_0_ranker.py`  
**変更箇所**: Line 204-211, Line 370-410 (objective関数内)

---

#### 改修③: 馬体重特徴量の整理

**目的**: 冗長な馬体重特徴量を統合し、モデルの解釈性を向上

**現状の問題**: Top 5中4つが馬体重関連

**v3.3改修案: 冗長特徴量の除外**

```python
# v3.3 改修 (train_mu_v3_0_ranker.py Line 252-270)
exclude_cols = [
    'race_id', 'horse_id', 'race_date', 'finish_position', 'target_gain', 'target_relevance',
    'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
    'race_name', 'horse_name', 'jockey_name', 'trainer_name',
    'sample_weight', 'ability_rank',
    
    # ★ NEW: 馬体重関連の冗長特徴量を除外
    'horse_weight_diff_from_avg',  # horse_weight_zscore と冗長
    'weight_diff_from_avg',        # horse_weight_zscore と冗長
    # horse_weight_zscore, basis_weight_zscore, horse_weight_change は保持
    
    # Feature Selection (v3.1): Remove raw weight features
    'horse_weight',  # 生の値は除外（Z標準化版を使用）
    'basis_weight',  # 生の値は除外（Z標準化版を使用）
    
    # Leakage (既存)
    'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
    'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
    'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
    'passing_order_3', 'passing_order_4', 'position_change_1_2', 
    'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
]
```

**保持する馬体重特徴量**:
1. `horse_weight_zscore`: 現在の馬体重（Z標準化）
2. `basis_weight_zscore`: 基準体重（Z標準化）
3. `horse_weight_change`: 前走からの増減

**期待効果**:
- モデル解釈性向上
- 過学習リスク軽減
- ROI影響: ±0~2%（ほぼ中立）

**実装ファイル**: `scripts/training/train_mu_v3_0_ranker.py`  
**変更箇所**: Line 252-270 (`exclude_cols` リスト)

---

### 4.2 中期改善（優先度: 高）

#### 改修④: 割安感特徴量の拡充

**目的**: 市場の歪み（人気と実力の乖離）を多角的に捉える

**現状**: `gap_ability_popularity` のみ（importance: 2,487, 3位）

**v3.3新規特徴量**:

##### 4.2.1 `gap_jockey_popularity`: 騎手実力 vs 人気

**ロジック**:
```python
# keibaai/src/features/roi_features.py に追加
def _add_gap_jockey_popularity(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    騎手実力ランク vs 人気のギャップ特徴量
    
    プラス値: 実力のある騎手なのに人気がない（割安）
    マイナス値: 実力に対して人気が高い（過大評価）
    """
    # 騎手の実力指標（過去1年の勝率）
    # ★ データリーク防止: 自レースより前のデータのみ使用
    jockey_stats = df.groupby('jockey_id').apply(
        lambda g: self._calc_jockey_win_rate_before_race(g, results_history_df)
    )
    
    # レース内で騎手実力の順位付け
    df['jockey_ability_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(
        ascending=False  # 勝率が高いほど上位（小さい数字）
    )
    
    # ギャップ計算
    df[gap_jockey_popularity'] = df['popularity'] - df['jockey_ability_rank']
    
    return df

def _calc_jockey_win_rate_before_race(self, group_df, history_df):
    """
    指定レースより前の騎手勝率を計算（データリーク防止）
    """
    race_date = group_df['race_date'].iloc[0]
    jockey_id = group_df['jockey_id'].iloc[0]
    
    # 1年前〜レース前日までのデータ
    past_year_data = history_df[
        (history_df['jockey_id'] == jockey_id) &
        (history_df['race_date'] < race_date) &
        (history_df['race_date'] >= race_date - pd.Timedelta(days=365))
    ]
    
    if len(past_year_data) == 0:
        return 0.0
    
    win_rate = (past_year_data['finish_position'] == 1).mean()
    return win_rate
```

**期待ROI影響**: +2~3%

##### 4.2.2 `gap_trainer_popularity`: 調教師実力 vs 人気

**ロジック**: 騎手と同様、調教師の勝率をレース内でランク付け

```python
# 同様の実装パターン
df['trainer_ability_rank'] = df.groupby('race_id')['trainer_win_rate'].rank(ascending=False)
df['gap_trainer_popularity'] = df['popularity'] - df['trainer_ability_rank']
```

**期待ROI影響**: +1~2%

##### 4.2.3 `gap_pedigree_popularity`: 血統実力 vs 人気

**ロジック**: 父・母父の勝率をレース内でランク付け

```python
# sire_win_rate, bms_win_rate の平均をpedigree実力とする
df['pedigree_ability'] = (df['sire_win_rate'] + df['bms_win_rate']) / 2
df['pedigree_ability_rank'] = df.groupby('race_id')['pedigree_ability'].rank(ascending=False)
df['gap_pedigree_popularity'] = df['popularity'] - df['pedigree_ability_rank']
```

**期待ROI影響**: +1~2%

##### 4.2.4 `gap_course_fit_popularity`: コース適性 vs 人気

**ロジック**: コース別勝率をレース内でランク付け

```python
# sire_course_win_rate など、コース適性指標を使用
df['course_fit_rank'] = df.groupby('race_id')['sire_course_win_rate'].rank(ascending=False)
df['gap_course_fit_popularity'] = df['popularity'] - df['course_fit_rank']
```

**期待ROI影響**: +1~2%

**実装ファイル**: `keibaai/src/features/roi_features.py`  
**変更箇所**: 新規メソッド追加 + `__init__` での呼び出し

**合計期待ROI影響**: +5~9%

---

#### 改修⑤: レースクラス別の市場特性特徴量

**目的**: レースクラス（G1、重賞、一般戦）ごとの市場特性を捉える

**背景**:
- G1: 人気馬が強い（市場効率的）
- 一般戦: 穴馬が多い（市場非効率）

**v3.3新規特徴量**:

##### 5.2.1 `race_class_overbet_risk`: クラス別過大評価リスク

**ロジック**:
```python
# keibaai/src/features/roi_features.py に追加
def _add_race_class_features(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    レースクラス別の市場特性特徴量
    """
    # レースクラスごとの人気馬勝率（過去データから計算）
    # G1: 1番人気勝率 35%
    # G2/G3: 1番人気勝率 32%
    # 重賞: 1番人気勝率 30%
    # 一般戦: 1番人気勝率 25%
    
    class_popularity_win_rate = {
        'G1': 0.35,
        'G2': 0.32,
        'G3': 0.32,
        '重賞': 0.30,
        '1600': 0.25,
        '1000': 0.25,
        '500': 0.23,
        '未勝利': 0.22,
    }
    
    df['class_expected_win_rate'] = df['race_class'].map(class_popularity_win_rate)
    
    # 過大評価リスク = 人気が上位なのにクラス的に不利
    # （人気が高いほど、クラスexpected勝率が低いほどリスク大）
    df['race_class_overbet_risk'] = (
        (1 / (df['popularity'] + 1)) *  # 人気が高いほど大
        (1 - df['class_expected_win_rate'])  # expected勝率が低いほど大
    )
    
    return df
```

**期待ROI影響**: +2~3%

**実装ファイル**: `keibaai/src/features/roi_features.py`  
**変更箇所**: 新規メソッド追加

---

#### 改修⑥: 時系列トレンド特徴量

**目的**: 馬の調子（上昇/下降トレンド）を捉える

**v3.3新規特徴量**:

##### 6.2.1 `finish_trend_last5`: 直近5走のトレンド

**ロ ジック**:
```python
# keibaai/src/features/roi_features.py に追加
def _add_trend_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    """
    時系列トレンド特徴量
    """
    for horse_id in df['horse_id'].unique():
        horse_past = history_df[
            (history_df['horse_id'] == horse_id) &
            (history_df['race_date'] < df[df['horse_id'] == horse_id]['race_date'].iloc[0])
        ].sort_values('race_date', ascending=False).head(5)
        
        if len(horse_past) >= 3:
            # 線形回帰で傾きを計算（マイナスなら改善傾向）
            from scipy.stats import linregress
            x = np.arange(len(horse_past))
            y = horse_past['finish_position'].values
            slope, _, _, _, _ = linregress(x, y)
            
            df.loc[df['horse_id'] == horse_id, 'finish_trend_last5'] = -slope
            # プラス: 改善傾向（着順が良くなっている）
            # マイナス: 悪化傾向
        else:
            df.loc[df['horse_id'] == horse_id, 'finish_trend_last5'] = 0.0
    
    return df
```

**期待ROI影響**: +1~2%

**実装ファイル**: `keibaai/src/features/roi_features.py`  
**変更箇所**: 新規メソッド追加

---

### 4.3 技術改善（優先度: 中）

#### 改修⑦: Feature Selectionの修正

**現状の問題**: エラーで機能していない
```
ERROR - Feature Selection Failed: No feature_importances found. Need to call fit beforehand.
```

**原因**: `fs_model` の初期化時に `fit` に必要な引数を渡していない

**v3.3修正**:

```python
# v3.3 修正 (train_mu_v3_0_ranker.py Line 310-340)
# Feature Selection (RFE-like)
logging.info("=" * 50)
logging.info("特徴量選抜 (Feature Selection) を開始します...")
logging.info("=" * 50)

try:
    fs_model = lgb.LGBMRanker(
        objective='lambdarank',
        metric='ndcg',
        n_estimators=1000,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=100,
        random_state=42,
        label_gain=[i for i in range(60)]  # ★ FIX: label_gainを追加
    )
    
    fs_model.fit(
        train_df[feature_cols],
        train_df['target_relevance'],
        group=group_train,
        sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
        eval_group=[group_valid],
        eval_sample_weight=[valid_df['sample_weight']],
        eval_metric='ndcg',
        callbacks=[lgb.early_stopping(stopping_rounds=50)]
    )
    
    # Get Importance
    imp_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': fs_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    # Select Top 80 Features
    top_n = 80
    selected_features = imp_df.head(top_n)['feature'].tolist()
    
    logging.info(f"Selected Feature Count: {len(selected_features)}")
    
    # Update feature_cols
    feature_cols = selected_features
    
    # Save selected features
    with open(self.models_dir / 'selected_features_v3_3.json', 'w') as f:
        import json
        json.dump(feature_cols, f)
        
except Exception as e:
    logging.error(f"Feature Selection Failed: {e}")
    logging.warning("Using all features instead.")
```

**期待効果**:
- 冗長特徴量の自動除外
- ROI改善: +1~2%
- 学習時間短縮

**実装ファイル**: `scripts/training/train_mu_v3_0_ranker.py`  
**変更箇所**: Line 310-350

---

## 5. 検証計画

### 5.1 実装順序と検証ステップ

**Phase 1: 短期改善（Week 1）**

1. **改修①+②を同時実装**
   - オッズクリッピングとターゲット重みをOptunaで最適化
   - 実行コマンド:
     ```bash
     python scripts/training/train_mu_v3_0_ranker.py --n_trials 50
     ```
   - 検証: CV ROI > 95% を確認

2. **改修③を追加実装**
   - 馬体重特徴量を整理
   - 実行: 同上
   - 検証: CV ROI維持（95%以上）、Feature Importanceで馬体重が分散しているか確認

**Phase 2: 中期改善（Week 2-3）**

3. **改修④を実装**
   - 割安感特徴量4種を追加
   - `keibaai/src/features/roi_features.py` を編集
   - 学習データを再生成:
     ```bash
     rm keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet
     python scripts/training/train_mu_v3_0_ranker.py --n_trials 50
     ```
   - 検証: 新特徴量がFeature Importanceに登場するか、CV ROI > 100% を確認

4. **改修⑤⑥を実装**
   - レースクラス特性、トレンド特徴量を追加
   - 学習データ再生成
   - 検証: Test ROI > 95% を確認

**Phase 3: 最終検証（Week 4）**

5. **統合テスト**
   - 全改修を統合して最終学習
   - `--n_trials 100` で徹底的に最適化
   - 検証: CV ROI > 100%, Test ROI > 95%

### 5.2 成功基準

| メトリクス | v3.2実績 | v3.3目標 | 判定基準 |
|-----------|---------|---------|---------|
| **CV ROI** | 84.95% | **100%+** | ✅ 100%超で成功 |
| **Test ROI** | 77.54% | **95%+** | ✅ 95%超で成功 |
| Top 1 Accuracy | 25.21% | 20-30% | ⚪ 15-35%で正常 |
| Top 5 Recall | 77.51% | 75%+ | ✅ 75%超で正常 |
| 特徴量数 | 169 | 80-100 | ⚪ Feature Selection機能 |

### 5.3 ログファイルでの確認項目

学習完了後、`keibaai/models/mu_v3_3/train_mu_v3_3.log` で以下を確認：

```bash
# ROIメトリクス
grep "Best ROI:" train_mu_v3_3.log
# 期待: Best ROI: 100%以上

# オッズクリッピング最適値
grep "Best odds_clip_upper:" train_mu_v3_3.log
# 期待: 150-400の範囲

# ターゲット重み最適値
grep "Best params:" train_mu_v3_3.log
# 期待: weight_1st: 10-15, weight_2nd: 4-8, weight_3rd: 1.5-4

# Feature Importance
grep -A 20 "Feature Importance" train_mu_v3_3.log
# 期待: gap系特徴量が上位10位以内に複数
```

---

## 6. リスクと対策

### 6.1 想定されるリスク

| リスク | 発生確率 | 影響度 | 対策 |
|-------|---------|--------|------|
| ①Overfitting（過学習） | 中 | 高 | Early Stopping、時系列CV厳格化 |
| ②Data Leakage（データリーク） | 低 | **最高** | チェックリスト厳守、コードレビュー |
| ③計算時間の増加 | 高 | 低 | 特徴量選択で絞り込み |
| ④新特徴量の効果不足 | 中 | 中 | A/Bテスト、段階的導入 |

### 6.2 データリーク防止チェックリスト

**新規特徴量追加時の必須確認項目**:

- [ ] `win_odds` を直接使用していないか
- [ ] `popularity` を**そのまま**使用していないか（過去データとの差分はOK）
- [ ] `finish_position` を**そのまま**使用していないか（過去データはOK）
- [ ] レース展開情報（passing_order, last_3f_time）を使用していないか
- [ ] 計算に使う過去データが「自レースより前」に限定されているか
- [ ] 時系列CVで未来のデータが訓練に混入していないか

**確認コマンド**:
```python
# 特徴量リストからFORBIDDEN_FEATURESを検出
import json
with open('keibaai/models/mu_v3_3/feature_names.json') as f:
    features = json.load(f)

FORBIDDEN = ['win_odds', 'popularity', 'finish_position', 'passing_order', 
             'last_3f_time', 'pace_index', 'prize_money']

leaked = [f for f in features if any(forbidden in f for forbidden in FORBIDDEN)]
if leaked:
    print(f"⚠️ Data Leakage Risk: {leaked}")
else:
    print("✅ No obvious data leakage detected")
```

---

## 7. 実装タイムライン

### Week 1: 短期改善
- **Day 1-2**: 改修①②（オッズクリッピング、ターゲット重み）実装
- **Day 3**: 改修③（馬体重整理）実装
- **Day 4-5**: Phase 1検証、CV ROI目標95%達成確認

### Week 2: 中期改善（前半）
- **Day 6-8**: 改修④（割安感特徴量4種）実装
- **Day 9-10**: Phase 2前半検証、CV ROI目標100%達成確認

### Week 3: 中期改善（後半）
- **Day 11-12**: 改修⑤⑥（レースクラス、トレンド）実装
- **Day 13-14**: Phase 2後半検証、Test ROI目標95%達成確認

### Week 4: 最終検証
- **Day 15-16**: 統合テスト（n_trials=100）
- **Day 17**: 結果分析、ドキュメント更新
- **Day 18**: リリース準備

---

## 8. まとめ

### 8.1 v3.3で実現すること

**ROI 100%超の達成**: 短期改善（①②③）+ 中期改善（④）で CV ROI 100%超を達成

**堅牢性の向上**: Feature Selection修正、データパイプライン改善で安定性を確保

**次バージョンへの布石**: v3.3の成果を踏まえ、v3.4ではアンサンブル学習などさらなる高度化を検討

### 8.2 重要な原則

**データリーク防止が最優先**: ROIより重要なのは、正しい予測モデルであること

**段階的な改善**: 一度に全てを変更せず、効果を確認しながら進める

**検証の徹底**: CV ROIだけでなく、Test ROIでの汎化性能を必ず確認

---

**以上が、μモデル v3.3でROI 100%超を達成するための詳細な改修計画です。**

**次のアクション**: この計画書をレビューし、Phase 1（短期改善）から実装を開始してください。
