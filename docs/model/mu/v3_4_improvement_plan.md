# μモデル v3.4 改修計画書

**作成日**: 2025-12-04
**対象**: μモデル単体でのROI 100%超達成
**現状**: v3.3実績（Test ROI 80.69%, Top 5 Recall 77.30%）
**目標**: Test ROI 100%+

---

## 📑 目次

1. [エグゼクティブサマリー](#エグゼクティブサマリー)
2. [v3.3での到達点と残課題](#v33での到達点と残課題)
3. [v3.4改修戦略](#v34改修戦略)
4. [5つの改修項目の実装詳細](#5つの改修項目の実装詳細)
5. [検証計画](#検証計画)
6. [σ/νモデルとの役割分担](#σνモデルとの役割分担)

---

## 1. エグゼクティブサマリー

### 1.1 ROI推移と現状

| バージョン | CV ROI | Test ROI | 主な改修内容 |
|-----------|--------|----------|--------------|
| v3.0 | - | 78.71% | ベースライン（LambdaRank） |
| v3.1 | - | 79.92% | オッズ加重学習 |
| v3.2 | 84.95% | 80.20% | Gap Features追加 |
| v3.3 | - | 80.69% | Feature Selection (202→161個) |
| v3.4（目標） | - | 100%+ | 本計画の対象 |

### 1.2 ROI 100%達成に必要な改善幅

- 現状: 80.69%
- 目標: 100%+
- 必要ギャップ: **+19.31%**

### 1.3 改修方針の全体像

```
┌──────────────────────────────────────────────────────────────┐
│ v3.4 ROI 100%超達成 5層戦略                                   │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  【改修①】アンサンブル学習（期待ROI: +5~8%）                   │
│   └─ 複数ハイパーパラメータモデルの加重平均                    │
│                                                               │
│  【改修②】ベッティング戦略の最適化（期待ROI: +5~10%）           │
│   └─ Kelly Criterion / EV閾値による賭け対象選別              │
│                                                               │
│  【改修③】潜在的リーク特徴量の清掃（期待ROI: +2~5%）            │
│   └─ popularity依存特徴量の見直し                            │
│                                                               │
│  【改修④】時系列CVの強化（期待ROI: +2~3%）                     │
│   └─ Purged K-Fold の導入                                    │
│                                                               │
│  【改修⑤】レースフィルタリング（期待ROI: +3~5%）               │
│   └─ 予測困難レースの除外                                     │
│                                                               │
│  合計期待ROI改善: +17~31%                                     │
│  → Test ROI: 80.69% + 20% ≈ 100%+（目標達成!）               │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. v3.3での到達点と残課題

### 2.1 v3.3で実装済みの改修

- ✅ オッズクリッピング最適化: odds_clip_upper=90で最適化
- ✅ ターゲット重み最適化: weight_1st=12.74, weight_2nd=6.73, weight_3rd=3.69
- ✅ 馬体重特徴量整理: 冗長特徴量の削除
- ✅ Gap Features拡張: 5種類実装済み（いずれもTop 50入り）
- ✅ Feature Selection: 202 → 161特徴量に削減

### 2.2 v3.3で有効だった特徴量 (Top 10)

| 順位 | 特徴量 | 重要度 | カテゴリ |
|------|--------|--------|----------|
| 1 | horse_weight_zscore | 3,413 | 馬体重 |
| 2 | basis_weight_zscore | 2,869 | 斤量 |
| 3 | sire_wet_boost | 1,968 | 血統 |
| 4 | gap_ability_popularity | 1,867 | 割安感 |
| 5 | bracket_avg_finish | 1,723 | 枠順 |

---

## 3. v3.4改修戦略

### 3.1 優先度マトリクス

| 改修項目 | ROI影響 | 実装コスト | リスク | 優先度 |
|---------|---------|-----------|--------|--------|
| ①アンサンブル学習 | +5~8% | 中 | 低 | **最高** |
| ②ベッティング戦略 | +5~10% | 低 | 低 | **最高** |
| ③リーク清掃 | +2~5% | 中 | 中 | 高 |
| ④時系列CV強化 | +2~3% | 中 | 低 | 中 |
| ⑤レースフィルタリング | +3~5% | 低 | 中 | 高 |

### 3.2 実装順序

- **Week 1**: ①②を並行実装（ROI +10~18%を狙う）
- **Week 2**: ③⑤を実装（ROI +5~10%を狙う）
- **Week 3**: ④を実装 + 統合テスト

---

## 4. 実装詳細

### 改修①: アンサンブル学習（期待ROI +5~8%）

#### 4.1.1 目的

異なるハイパーパラメータを持つ複数モデルの予測を組み合わせ、汎化性能を向上。

#### 4.1.2 実装方法

```python
# scripts/training/train_mu_v3_4_ensemble.py
class EnsembleMuTrainer:
    def __init__(self):
        self.models = []
        self.weights = []
    
    def train(self, df, n_trials=30):
        # モデル1: 保守的（high regularization）
        params_1 = {
            'num_leaves': 31,
            'lambda_l1': 5.0,
            'lambda_l2': 5.0,
            'min_child_samples': 500,
            'learning_rate': 0.01
        }
        
        # モデル2: 標準
        params_2 = {
            'num_leaves': 63,
            'lambda_l1': 1.0,
            'lambda_l2': 1.0,
            'min_child_samples': 200,
            'learning_rate': 0.05
        }
        
        # モデル3: 積極的（low regularization）
        params_3 = {
            'num_leaves': 127,
            'lambda_l1': 0.1,
            'lambda_l2': 0.1,
            'min_child_samples': 100,
            'learning_rate': 0.1
        }
        
        for params in [params_1, params_2, params_3]:
            model = lgb.LGBMRanker(**params, ...)
            model.fit(...)
            self.models.append(model)
        
        # 重み最適化（Optuna）
        self.weights = self._optimize_weights(valid_df)
    
    def predict(self, X):
        preds = [m.predict(X) for m in self.models]
        return np.average(preds, axis=0, weights=self.weights)
```

#### 4.1.3 検証基準

- アンサンブルROI > 単一モデルROI + 3%

---

### 改修②: ベッティング戦略の最適化（期待ROI +5~10%）

#### 4.2.1 目的

予測スコアだけでなく、期待値（EV）に基づいて賭け対象を選別。

#### 4.2.2 EV計算式

```python
# EV = P(win) × odds - 1
# P(win)はモデル予測のソフトマックス変換で推定
def calculate_ev(df, score_col='score', odds_col='win_odds'):
    # レース内でソフトマックスで確率変換
    df['exp_score'] = np.exp(df[score_col])
    df['win_prob'] = df.groupby('race_id')['exp_score'].transform(
        lambda x: x / x.sum()
    )
    
    # EV計算
    df['ev'] = df['win_prob'] * df[odds_col] - 1
    return df
```

#### 4.2.3 賭け戦略

```python
def select_bets(df, ev_threshold=0.0, min_odds=3.0, max_odds=50.0):
    """
    EVとオッズ範囲で賭け対象をフィルタリング
    
    - EV > 0: 期待値プラスの馬のみ
    - オッズ3~50倍: 予測可能な中穴ゾーン
    """
    bet_candidates = df[
        (df['ev'] > ev_threshold) &
        (df['win_odds'] >= min_odds) &
        (df['win_odds'] <= max_odds)
    ]
    
    # レースごとにTop 1を選択
    bets = bet_candidates.loc[
        bet_candidates.groupby('race_id')['score'].idxmax()
    ]
    
    return bets
```

#### 4.2.4 検証基準

- 選別後ROI > 全レースROI + 10%
- 賭けレース数 > 全体の30%（選別しすぎない）

---

### 改修③: 潜在的リーク特徴量の清掃（期待ROI +2~5%）

#### 4.3.1 問題

Gap Features（5種）がすべてpopularityを参照：

```
gap_ability_popularity = popularity - ability_rank
```

popularityは確定人気であり、運用時の「発走直前人気」とは時間差がある。

#### 4.3.2 解決策

**Option A: morning_popularityへの差し替え**

```python
# 朝オッズ由来の人気を使用
df['gap_ability_popularity'] = df['morning_popularity'] - df['ability_rank']
```

**Option B: オッズランクからの再計算**

```python
# morning_oddsから人気順を計算
df['implied_popularity'] = df.groupby('race_id')['morning_odds'].rank(method='min')
df['gap_ability_popularity'] = df['implied_popularity'] - df['ability_rank']
```

#### 4.3.3 注意点

- morning_odds/morning_popularityは一部欠損あり（shutuba.parquet参照）
- 欠損時はpopularityにフォールバック

---

### 改修④: 時系列CVの強化（期待ROI +2~3%）

#### 4.4.1 問題

現行の時系列3-Fold CV:

```
Fold 1: Train 2020~2021    Valid 2022~2022
Fold 2: Train 2020~2022    Valid 2023~2023
Fold 3: Train 2020~2023.06 Valid 2023.07~2023.12
```

問題点: 訓練データと検証データの境界日付近でリーク可能性。

#### 4.4.2 解決策: Purged K-Fold

```python
def purged_time_series_split(df, n_folds=3, purge_days=30):
    """
    訓練と検証の間にギャップ（purge期間）を設ける
    """
    folds = []
    
    splits = [
        ('2020-01-01', '2021-12-01', '2022-01-01', '2023-01-01'),
        ('2020-01-01', '2022-12-01', '2023-01-01', '2024-01-01'),
        ('2020-01-01', '2023-06-01', '2023-07-01', '2024-01-01'),
    ]
    
    for train_start, train_end, valid_start, valid_end in splits:
        train_mask = (df['race_date'] >= train_start) & (df['race_date'] < train_end)
        valid_mask = (df['race_date'] >= valid_start) & (df['race_date'] < valid_end)
        folds.append((train_mask, valid_mask))
    
    return folds
```

---

### 改修⑤: レースフィルタリング（期待ROI +3~5%）

#### 4.5.1 目的

予測が困難で損失リスクが高いレースを除外。

#### 4.5.2 除外基準

```python
def filter_predictable_races(df):
    """
    予測可能なレースのみを対象に
    """
    # 除外条件
    excluded_races = df[
        # 少頭数（5頭以下）: ランダム性が高い
        (df['head_count'] <= 5) |
        
        # 超多頭数（17頭以上）: 展開読みが困難
        (df['head_count'] >= 17) |
        
        # 新馬戦: 過去データなし
        (df['race_class'] == '新馬') |
        
        # 障害レース: 異なる競技
        (df['track_surface'] == '障害')
    ]['race_id'].unique()
    
    return df[~df['race_id'].isin(excluded_races)]
```

#### 4.5.3 期待効果

- 予測精度向上（ノイズ削減）
- ROI向上（高リスクレース回避）

---

## 5. 検証計画

### 5.1 実装ステップ

| Phase | 改修項目 | 期間 | 成功基準 |
|-------|---------|------|---------|
| 1 | ①②ベッティング戦略+アンサンブル | Week 1 | ROI > 90% |
| 2 | ⑤レースフィルタリング | Week 1 | ROI > 95% |
| 3 | ③リーク清掃 | Week 2 | ROI維持 |
| 4 | ④Purged K-Fold | Week 2 | ROI維持 |
| 5 | 統合テスト | Week 3 | ROI > 100% |

### 5.2 成功基準（最終）

| メトリクス | v3.3実績 | v3.4目標 |
|-----------|---------|---------|
| **Test ROI** | 80.69% | **100%+** |
| 賭けレース数 | 100% | 50~80% |
| Top 5 Recall | 77.30% | 75%+ |

---

## 6. σ/νモデルとの役割分担

> [!IMPORTANT]
> 本計画はμモデル単体の改修に限定。σ/νモデルの改修は別計画で行う。

### 6.1 μモデルの役割（本計画）

| 機能 | 内容 |
|------|------|
| 順位予測 | LambdaRankによる相対順位スコア |
| 単勝ベット選択 | Top 1馬の特定 |
| ROI最大化 | オッズ加重学習 + EV戦略 |

### 6.2 σ/νモデルの役割（別計画）

| 機能 | 内容 | 本計画との関係 |
|------|------|---------------|
| σ（確信度） | 予測の不確実性推定 | μの残差から学習（μ改修後に再学習） |
| ν（混沌度） | レース展開の予測困難性 | μの残差から学習（μ改修後に再学習） |
| 馬連/三連複 | 複数馬の組み合わせ予測 | μ/σ/ν統合後に実装 |
| Kelly配分 | 資金配分最適化 | μ/σ/ν統合後に実装 |

### 6.3 開発順序

1. **μモデル v3.4（本計画）** → ROI 100%+ を目指す
2. σモデル再学習 → μv3.4の残差ベース
3. νモデル再学習 → リーク排除版
4. 統合システム → 馬連/Kelly配分

---

## 付録: 参照ドキュメント

- [03_データモデル.md](file:///c:/Users/zk-ht/Keiba/Keiba_AI_v2/docs/system/03_データモデル.md): Parquetスキーマ定義
- [06_特徴量エンジニアリング.md](file:///c:/Users/zk-ht/Keiba/Keiba_AI_v2/docs/system/06_特徴量エンジニアリング.md): 特徴量生成ロジック
- [07_機械学習モデル.md](file:///c:/Users/zk-ht/Keiba/Keiba_AI_v2/docs/system/07_機械学習モデル.md): LightGBM設定
- [parquet_contents_report.md](file:///c:/Users/zk-ht/Keiba/Keiba_AI_v2/parquet_contents_report.md): データ構造定義

---

**作成日**: 2025-12-04
**最終更新**: 2025-12-04
