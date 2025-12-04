# μモデル v3.0 (Synergy) 実装仕様書

本ドキュメントは、μモデルを「的中率重視（v2.8）」から「機会創出重視（v3.0）」へ転換し、システム全体のROIを最大化するための詳細な実装ガイドです。

## 1. 目的とコンセプト
- **目的**: σ・νモデル（後段のフィルタ）に、より多くの「高配当の種（穴馬）」を供給すること。
- **コンセプト**: **Learning to Rank (ランキング学習)** の導入。
    - 従来の「1着になる確率」ではなく、「1着になった時のリターン（期待値）」を学習し、高配当馬を上位にランク付けする。

## 2. 実装詳細

### 2.1 データ準備 (`scripts/training/train_mu_v3_0_ranker.py`)

#### ラベル定義 (Label Engineering)
単純な0/1ラベルではなく、オッズを加味した「ゲイン（報酬）」をラベルとします。

```python
# ターゲット変数の作成
# 1着の場合は log(1 + 単勝オッズ) をゲインとする。それ以外は0。
# logをとることで、単勝万馬券などの外れ値による勾配爆発を防ぐ。
df['target_gain'] = (df['finish_position'] == 1).astype(int) * np.log1p(df['win_odds'])
```

#### グループ化 (Grouping)
ランキング学習には「クエリ（レースID）」ごとのグループ情報が必要です。

```python
# race_id でソートしておくことが必須
df = df.sort_values('race_id')
group = df.groupby('race_id').size().to_list()
```

### 2.2 特徴量エンジニアリング (`keibaai/src/features/roi_features.py`)

`ROIFeatureEngine` クラスに以下のメソッドを追加し、市場評価（オッズ・人気）と実力値のギャップを捉える特徴量を生成します。

#### (1) Gap Features (乖離特徴量)
市場の過小評価を見抜くための指標。

```python
def add_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
    # 前提: dfには 'popularity' (人気順), 'pace_index' (スピード指数相当) が存在
    
    # 1. スピード指数順位と人気順位のギャップ
    # プラスが大きいほど「実力の割に人気がない（おいしい）」
    # pace_indexが高いほど速いと仮定
    df['speed_rank'] = df.groupby('race_id')['pace_index'].rank(ascending=False)
    df['gap_speed_popularity'] = df['popularity'] - df['speed_rank']
    
    # 2. 騎手・調教師の実力と人気のギャップ
    # win_rateが高いのに人気がない馬を探す
    df['jockey_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(ascending=False)
    df['gap_jockey_popularity'] = df['popularity'] - df['jockey_rank']
    
    return df
```

#### (2) Anti-Trend Features (逆張り特徴量)
「危険な人気馬」を検知し、スコアを下げるための指標。

```python
def add_anti_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
    # 1. 過剰人気フラグ
    # 「騎手はルメール（人気）だが、馬の近走成績は悪い」場合など
    # 近走成績ランク（past_5_finish_position_meanの順位）と人気順位の乖離
    df['form_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True)
    df['is_overvalued'] = (df['popularity'] == 1) & (df['form_rank'] > 3)
    
    return df
```

### 2.3 モデル学習設定 (LightGBM Ranker)

`LGBMRanker` を使用します。

```python
from lightgbm import LGBMRanker

model = LGBMRanker(
    objective='lambdarank',
    metric='ndcg',
    eval_at=[1, 3, 5],  # Top 1, 3, 5 のランキング精度を評価
    learning_rate=0.05,
    n_estimators=1000,
    importance_type='gain',
    random_state=42
)

model.fit(
    X_train, 
    y_train,      # target_gain を使用
    group=group_train,
    eval_set=[(X_valid, y_valid)],
    eval_group=[group_valid],
    early_stopping_rounds=50
)
```

## 3. システム統合 (`docs/system` 準拠)

### 3.1 推論パイプライン (`keibaai/src/inference/predictor.py`)
v3.0モデルの出力は「確率」ではなく「スコア（ランク）」になります。

1.  **μモデル (v3.0)**: 全出走馬のスコアを算出。
2.  **候補生成**: スコア上位 **5頭** を「推奨候補」として抽出。
3.  **σ・νモデル連携**:
    - 抽出された5頭に対してのみ、σ（確信度）とν（期待値）を計算。
    - これにより、計算コストを削減しつつ、v2.8では切り捨てられていた「3番手評価の穴馬」などを拾うことが可能になる。

## 4. 期待される効果とKPI

| 指標 | v2.8 (現状) | v3.0 (目標) | 理由 |
| :--- | :--- | :--- | :--- |
| **Top 1 的中率** | 26.9% | 20.0% | 人気馬をあえて下げるため低下する。 |
| **Top 1 平均オッズ** | 9.8倍 | **15.0倍+** | 穴馬がTop 1に来る頻度が増える。 |
| **Top 5 Recall** | N/A | **90.0%+** | 「勝ち馬」をTop 5に含める能力（取りこぼし防止）。 |
| **システムROI** | 79.4% | **100.0%+** | σ・νモデルが「高配当の原石」を選別できるようになるため。 |

## 5. 開発ステップ

1.  **特徴量追加**: `keibaai/src/features/roi_features.py` に `Gap Features` を実装。
2.  **学習スクリプト作成**: `scripts/training/train_mu_v3_0_ranker.py` を作成。
    - `lambdarank` と `target_gain` を実装。
3.  **検証**: 2024年データでバックテスト。
    - 評価指標は `NDCG@5` と `Top 1 ROI`。
