# σ/νモデル統合によるROI改善計画書

**作成日**: 2025-12-04
**対象**: μモデルv3.3 + σ/νモデル統合による高確信度レース選別
**目標**: ROI 100%+ 達成

---

## 📑 目次

1. [エグゼクティブサマリー](#エグゼクティブサマリー)
2. [現状分析](#現状分析)
3. [σ/νモデルの役割と理論](#σνモデルの役割と理論)
4. [データ構造と特徴量](#データ構造と特徴量)
5. [σモデル再学習手順](#σモデル再学習手順)
6. [νモデル再学習手順（リーク排除版）](#νモデル再学習手順)
7. [μ/σ/ν統合手順](#μσν統合手順)
8. [実装コード詳細](#実装コード詳細)
9. [検証計画](#検証計画)
10. [タイムライン](#タイムライン)

---

## 1. エグゼクティブサマリー

### 1.1 なぜσ/νモデル統合が必要か

μモデル単体での限界：
- **v3.3 Test ROI: 80.69%**（目標100%に未達）
- ハイパーパラメータ再最適化後も**78.13%**に留まる
- **特徴量の追加なしでは天井がある**

σ/νモデル統合のメリット：
- **σモデル**: 予測の「確信度」を評価 → 確信度が高いレースのみ選別
- **νモデル**: レースの「荒れやすさ」を評価 → 堅いレースに集中投資

### 1.2 戦略の全体像

```
┌─────────────────────────────────────────────────────────────┐
│  μモデル（v3.3）                                            │
│  - 各馬の相対的な強さスコアを予測                            │
│  - ROI: 80.69%（全レース投資時）                            │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐       ┌───────────────────────┐
│  σモデル          │       │  νモデル              │
│  馬ごとの不確実性 │       │  レースの混沌度       │
│  σ_i = f(馬特徴) │       │  ν = g(レース特徴)   │
└─────────┬─────────┘       └──────────┬────────────┘
          │                            │
          └──────────┬─────────────────┘
                     │
                     ▼
          ┌─────────────────────┐
          │  レース選別フィルタ │
          │  σ < 閾値 AND ν > 閾値 │
          │  → 確信度高 & 堅いレース │
          └─────────┬───────────┘
                    │
                    ▼
          ┌─────────────────────┐
          │  選別後ROI目標      │
          │  100%+ 達成         │
          │  (投資レース数: 30-50%) │
          └─────────────────────┘
```

---

## 2. 現状分析

### 2.1 利用可能なデータソース

**Parquetファイル構造**:

| ファイル | パス | 用途 |
|---------|------|------|
| horses.parquet | keibaai/data/parsed/parquet/horses/ | 馬プロファイル |
| pedigrees.parquet | keibaai/data/parsed/parquet/pedigrees/ | 血統データ |
| races.parquet | keibaai/data/parsed/parquet/races/ | レース結果（学習用ターゲット含む） |
| shutuba.parquet | keibaai/data/parsed/parquet/shutuba/ | 出馬表（予測時の入力） |

### 2.2 races.parquetの主要カラム

```
【識別子】
- race_id: レース識別子（例: 202001010101）
- horse_id: 馬識別子

【ターゲット変数（学習時のみ使用）】
- finish_position: 着順（1, 2, 3, ...）
- finish_time_seconds: 完走時間（秒）
- win_odds: 確定単勝オッズ

【レース特徴量（νモデル用）】
- distance_m: 距離（m）
- track_surface: 馬場（芝/ダート）
- track_condition: 馬場状態（良/稀重/重/不良）
- weather: 天候
- venue: 開催場所
- race_class: クラス（G1, G2, 未勝利 等）
- age_restriction: 年齢制限

【馬特徴量（σモデル用）】
- sex_age: 性別・年齢
- basis_weight: 斤量
- horse_weight: 馬体重
- jockey_id: 騎手ID
- trainer_id: 調教師ID
```

### 2.3 既存σ/νモデルの問題点

**σモデル** (`keibaai/src/modules/models/sigma_estimator.py`):
- ✅ 実装は健全
- μモデルの残差から学習
- **問題**: μモデルがv2.x系のため、v3.3の残差で再学習が必要

**νモデル** (`keibaai/src/modules/models/nu_estimator.py`):
- ⚠️ **データリーク検出済み**
- 過去バージョンで`avg_win_odds`, `std_win_odds`が特徴量に含まれていた
- これらは最終確定オッズであり、予測時には使用不可
- **対策**: リーク排除版を再実装

---

## 3. σ/νモデルの役割と理論

### 3.1 t分布によるモデリング

各馬iの完走時間をt分布でモデル化:

```
T_i ~ t_ν(μ_i, σ_i²)

パラメータ:
  μ_i: 馬iの期待完走時間（μモデルで予測）
  σ_i: 馬iの不確実性（σモデルで予測）
  ν:   レース全体の混沌度（νモデルで予測）
```

### 3.2 パラメータの解釈

| パラメータ | 意味 | 低い値 | 高い値 |
|-----------|------|--------|--------|
| **σ（sigma）** | 馬固有の不確実性 | 安定した馬 | 調子にムラがある馬 |
| **ν（nu）** | レースの混沌度 | 荒れやすいレース | 堅いレース |

### 3.3 レース選別戦略

**選別条件**:
```python
# 確信度が高く、堅いレースのみ選別
selected_races = df[
    (df['sigma_pred'] < sigma_threshold) &  # 不確実性が低い
    (df['nu_pred'] > nu_threshold)           # レースが堅い
]
```

**期待効果**:
- 全レース投資: ROI 80%
- 選別後投資: ROI 100%+（投資機会は減少）

---

## 4. データ構造と特徴量

### 4.1 σモデルの特徴量

**馬固有の不確実性に影響する要因**:

| カテゴリ | 特徴量 | 計算方法 | リーク判定 |
|---------|--------|----------|-----------|
| **過去成績の安定性** | past_N_finish_position_std | 直近N走の着順標準偏差 | ✅ 安全（.shift(1)適用） |
| | past_N_finish_time_std | 直近N走のタイム標準偏差 | ✅ 安全 |
| **経験値** | career_starts | 通算出走数 | ✅ 安全 |
| | career_wins | 通算勝利数 | ✅ 安全 |
| **馬の状態** | days_since_last_race | 前走からの日数 | ✅ 安全 |
| | horse_weight_change | 馬体重増減 | ✅ 安全 |
| **騎手安定性** | jockey_recent_win_rate | 騎手の直近勝率 | ✅ 安全 |

### 4.2 νモデルの特徴量（リーク排除版）

**レース混沌度に影響する要因**:

| カテゴリ | 特徴量 | 計算方法 | リーク判定 |
|---------|--------|----------|-----------|
| **レース構造** | head_count | 出走頭数 | ✅ 安全 |
| | distance_m | 距離 | ✅ 安全 |
| | track_surface | 芝/ダート | ✅ 安全 |
| | track_condition | 馬場状態 | ✅ 安全 |
| **クラス** | race_class | クラス（エンコード済み） | ✅ 安全 |
| | age_restriction | 年齢制限 | ✅ 安全 |
| **条件** | venue | 開催場所 | ✅ 安全 |
| | weather | 天候 | ✅ 安全 |

❌ **使用禁止**:
- `avg_win_odds`: 確定オッズの平均 → 未来情報
- `std_win_odds`: 確定オッズの標準偏差 → 未来情報
- `popularity_*`: 確定人気 → 未来情報

---

## 5. σモデル再学習手順

### 5.1 前提条件

- μモデルv3.3が学習済みであること
- 学習データが`keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet`に存在すること

### 5.2 実装手順

#### Step 1: μv3.3で全データ予測

```python
import pandas as pd
import pickle
from pathlib import Path

# μv3.3モデル読み込み
model_dir = Path('keibaai/models/mu_v3_3')
with open(model_dir / 'mu_v3_3.pkl', 'rb') as f:
    mu_model = pickle.load(f)

# 学習データ読み込み
df = pd.read_parquet(model_dir / 'train_data_mu_v3_3.parquet')

# 特徴量読み込み
import json
with open(model_dir / 'feature_names.json', 'r') as f:
    feature_cols = json.load(f)

# μ予測
df['mu_pred'] = mu_model.predict(df[feature_cols])
```

#### Step 2: 残差計算

```python
# 残差 = 実績スコア - 予測スコア
# ただしμv3.3はランキングモデルなので、
# 残差は「着順との乖離」として定義

# レース内でランクを計算
df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False)
df['residual'] = df['finish_position'] - df['rank_pred']
```

#### Step 3: σモデル学習

```python
import numpy as np
import lightgbm as lgb

# ターゲット: 残差の絶対値の対数
df['sigma_target'] = np.log(np.abs(df['residual']) + 1e-6)

# σモデル用特徴量
sigma_features = [
    'past_5_finish_position_std',
    'past_10_finish_position_std',
    'career_starts',
    'career_wins',
    'days_since_last_race',
    'horse_weight_change',
    'jockey_win_rate',
    'age'
]

# 時系列分割
train_mask = df['race_date'] < '2023-01-01'
valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')

X_train = df.loc[train_mask, sigma_features]
y_train = df.loc[train_mask, 'sigma_target']
X_valid = df.loc[valid_mask, sigma_features]
y_valid = df.loc[valid_mask, 'sigma_target']

# モデル学習
sigma_model = lgb.LGBMRegressor(
    objective='regression',
    metric='rmse',
    n_estimators=1000,
    learning_rate=0.01,
    num_leaves=31,
    verbose=-1
)

sigma_model.fit(
    X_train, y_train,
    eval_set=[(X_valid, y_valid)],
    callbacks=[lgb.early_stopping(50)]
)

# 保存
with open('keibaai/models/sigma_v1/sigma_model.pkl', 'wb') as f:
    pickle.dump(sigma_model, f)
```

---

## 6. νモデル再学習手順（リーク排除版）

### 6.1 重要: リーク排除

過去のνモデルで使用されていた以下の特徴量は**削除必須**:

```python
# ❌ 使用禁止（リーク）
leak_features = [
    'avg_win_odds',      # 確定オッズ平均
    'std_win_odds',      # 確定オッズ標準偏差
    'min_win_odds',      # 最低オッズ
    'max_win_odds',      # 最高オッズ
    'popularity_spread', # 人気の分散
]
```

### 6.2 リーク排除版の特徴量

```python
# ✅ 使用可能（リークなし）
nu_features = [
    # レース構造
    'head_count',           # 出走頭数
    'distance_m',           # 距離
    'track_surface_turf',   # 芝フラグ
    'track_surface_dirt',   # ダートフラグ
    
    # 馬場状態
    'track_condition_good',      # 良
    'track_condition_slightly_heavy', # 稀重
    'track_condition_heavy',     # 重
    'track_condition_bad',       # 不良
    
    # クラス（順序エンコード）
    'race_class_encoded',   # G1=10, G2=9, ..., 未勝利=1
    
    # その他
    'age_restriction_2yo',  # 2歳限定
    'age_restriction_3yo',  # 3歳限定
    'venue_encoded',        # 開催場所
]
```

### 6.3 νの真値計算（MLE推定）

```python
from scipy.stats import t
from scipy.optimize import minimize_scalar

def estimate_nu_mle(standardized_residuals):
    """
    t分布のMLEでνを推定
    
    Args:
        standardized_residuals: 標準化残差 z = (y - μ) / σ
    
    Returns:
        optimal_nu: 最適なν値（2.1〜30.0）
    """
    if len(standardized_residuals) < 3:
        return 5.0  # デフォルト値
    
    def neg_log_likelihood(nu):
        return -np.sum(t.logpdf(standardized_residuals, df=nu))
    
    result = minimize_scalar(
        neg_log_likelihood,
        bounds=(2.1, 30.0),
        method='bounded'
    )
    
    return result.x
```

### 6.4 νモデル学習

```python
# 1. 標準化残差の計算
df['sigma_pred'] = sigma_model.predict(df[sigma_features])
df['z'] = df['residual'] / np.exp(df['sigma_pred'])

# 2. レースごとにνを推定
race_nu = df.groupby('race_id')['z'].apply(estimate_nu_mle)

# 3. レース特徴量の準備（重複排除）
race_features_df = df.drop_duplicates(subset=['race_id'])[
    ['race_id'] + nu_features
].set_index('race_id')

# 4. νモデル学習
X_nu = race_features_df.loc[race_nu.index]
y_nu = race_nu

nu_model = lgb.LGBMRegressor(
    objective='regression',
    metric='mae',
    n_estimators=500,
    learning_rate=0.01,
    num_leaves=15,
    max_depth=5,
    verbose=-1
)

nu_model.fit(X_nu, y_nu)

# 保存
with open('keibaai/models/nu_v1/nu_model.pkl', 'wb') as f:
    pickle.dump(nu_model, f)
```

---

## 7. μ/σ/ν統合手順

### 7.1 統合評価スクリプト

```python
"""
μ/σ/ν統合評価スクリプト
scripts/evaluation/evaluate_integrated_model.py
"""

import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path

class IntegratedModelEvaluator:
    """μ/σ/ν統合モデル評価クラス"""
    
    def __init__(self, models_dir='keibaai/models'):
        self.models_dir = Path(models_dir)
        self.mu_model = None
        self.sigma_model = None
        self.nu_model = None
        self.feature_cols = None
        self.sigma_features = None
        self.nu_features = None
    
    def load_models(self):
        """モデル読み込み"""
        # μモデル
        with open(self.models_dir / 'mu_v3_3/mu_v3_3.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_3/feature_names.json', 'r') as f:
            self.feature_cols = json.load(f)
        
        # σモデル
        with open(self.models_dir / 'sigma_v1/sigma_model.pkl', 'rb') as f:
            self.sigma_model = pickle.load(f)
        
        # νモデル
        with open(self.models_dir / 'nu_v1/nu_model.pkl', 'rb') as f:
            self.nu_model = pickle.load(f)
    
    def predict_and_filter(self, df, sigma_threshold, nu_threshold):
        """
        予測と選別
        
        Args:
            df: 評価データ
            sigma_threshold: σ閾値（これ以下を選別）
            nu_threshold: ν閾値（これ以上を選別）
        
        Returns:
            selected_df: 選別されたデータ
        """
        # μ予測
        df['mu_pred'] = self.mu_model.predict(df[self.feature_cols])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(
            ascending=False, method='first'
        )
        
        # σ予測
        df['sigma_pred'] = np.exp(self.sigma_model.predict(df[self.sigma_features]))
        
        # ν予測（レース単位）
        race_df = df.drop_duplicates(subset=['race_id'])[['race_id'] + self.nu_features]
        race_df['nu_pred'] = self.nu_model.predict(race_df[self.nu_features])
        df = df.merge(race_df[['race_id', 'nu_pred']], on='race_id', how='left')
        
        # 選別
        selected_df = df[
            (df['sigma_pred'] < sigma_threshold) &
            (df['nu_pred'] > nu_threshold)
        ]
        
        return df, selected_df
    
    def calculate_roi(self, df):
        """ROI計算（Top 1投資）"""
        top1 = df[df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        
        if len(top1) == 0:
            return 0.0
        
        roi = hits['win_odds'].sum() / len(top1)
        return roi
    
    def evaluate(self, df, sigma_threshold, nu_threshold):
        """評価実行"""
        all_df, selected_df = self.predict_and_filter(
            df, sigma_threshold, nu_threshold
        )
        
        roi_all = self.calculate_roi(all_df)
        roi_selected = self.calculate_roi(selected_df)
        
        results = {
            'roi_all_races': roi_all,
            'roi_selected_races': roi_selected,
            'total_races': all_df['race_id'].nunique(),
            'selected_races': selected_df['race_id'].nunique(),
            'selection_rate': selected_df['race_id'].nunique() / all_df['race_id'].nunique(),
            'sigma_threshold': sigma_threshold,
            'nu_threshold': nu_threshold
        }
        
        return results
```

### 7.2 閾値最適化

```python
"""
σ/ν閾値のグリッドサーチ
scripts/optimization/optimize_sigma_nu_threshold.py
"""

import optuna

def optimize_thresholds(evaluator, valid_df, n_trials=100):
    """
    Optunaでσ/ν閾値を最適化
    """
    def objective(trial):
        sigma_t = trial.suggest_float('sigma_threshold', 0.5, 3.0)
        nu_t = trial.suggest_float('nu_threshold', 3.0, 15.0)
        
        results = evaluator.evaluate(valid_df, sigma_t, nu_t)
        
        # 選別率が低すぎる場合はペナルティ
        if results['selection_rate'] < 0.2:
            return 0.0
        
        return results['roi_selected_races']
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials)
    
    return study.best_params, study.best_value
```

---

## 8. 実装コード詳細

### 8.1 ディレクトリ構造

```
keibaai/
├── models/
│   ├── mu_v3_3/                    # μモデルv3.3
│   │   ├── mu_v3_3.pkl
│   │   ├── feature_names.json
│   │   └── train_data_mu_v3_3.parquet
│   ├── sigma_v1/                   # σモデル（新規作成）
│   │   ├── sigma_model.pkl
│   │   └── sigma_features.json
│   └── nu_v1/                      # νモデル（リーク排除版、新規作成）
│       ├── nu_model.pkl
│       └── nu_features.json
│
scripts/
├── training/
│   ├── train_sigma_model_v1.py     # σモデル学習スクリプト（新規作成）
│   └── train_nu_model_v1.py        # νモデル学習スクリプト（新規作成）
└── evaluation/
    └── evaluate_integrated_model.py # 統合評価スクリプト（新規作成）
```

### 8.2 新規作成ファイル一覧

| ファイル | 説明 |
|---------|------|
| `scripts/training/train_sigma_model_v1.py` | σモデル学習スクリプト |
| `scripts/training/train_nu_model_v1.py` | νモデル学習スクリプト（リーク排除版） |
| `scripts/evaluation/evaluate_integrated_model.py` | μ/σ/ν統合評価 |
| `scripts/optimization/optimize_sigma_nu_threshold.py` | 閾値最適化 |

---

## 9. 検証計画

### 9.1 検証ステップ

| Phase | 内容 | 成功基準 |
|-------|------|---------|
| 1 | σモデル学習・評価 | 残差予測RMSE < 1.0 |
| 2 | νモデル学習・評価（リーク排除版） | レース分類精度 > 70% |
| 3 | 閾値最適化 | 選別後ROI > 90% |
| 4 | Test期間評価 | **選別後ROI > 100%** |

### 9.2 リークチェックリスト

- [ ] σモデル特徴量に`finish_position`, `finish_time`, `win_odds`が含まれていないこと
- [ ] νモデル特徴量に`avg_win_odds`, `std_win_odds`, `popularity`が含まれていないこと
- [ ] 時系列分割が正しく適用されていること（Train < Valid < Test）
- [ ] ローリングウィンドウで過去データのみ使用していること

---

## 10. タイムライン

| 週 | 作業内容 | 成果物 |
|----|---------|--------|
| **Week 1** | σモデル再学習 | `sigma_model.pkl` |
| **Week 2** | νモデル再学習（リーク排除版） | `nu_model.pkl` |
| **Week 3** | μ/σ/ν統合・閾値最適化 | `evaluate_integrated_model.py` |
| **Week 4** | Test期間評価・本番適用判断 | 最終レポート |

---

## 付録A: データリークチェックリスト

### A.1 禁止特徴量一覧

| カテゴリ | 特徴量 | 理由 |
|---------|--------|------|
| **確定オッズ** | `win_odds`, `avg_win_odds`, `std_win_odds` | レース後に確定する情報 |
| **確定人気** | `popularity`, `morning_popularity`（欠損多い） | オッズから算出 |
| **着順** | `finish_position` | ターゲット変数 |
| **タイム** | `finish_time_seconds`, `last_3f_time` | レース後に確定 |
| **通過順** | `passing_order_1-4` | レース中の情報 |
| **着差** | `margin_seconds`, `margin_str` | レース後に確定 |

### A.2 安全な特徴量

- `.shift(1)`で1行シフトされた過去走集計
- 予測対象レースより前のデータのみで計算された統計量
- レース開始前に確定している情報（馬体重、斤量、枠順等）

---

## 付録B: 参照ドキュメント

- [07_機械学習モデル.md](../system/07_機械学習モデル.md): σ/νモデルの理論
- [13_モデル改善戦略.md](../system/13_モデル改善戦略.md): 改善戦略の全体像
- [14_モデル命名規則とベストプラクティス.md](../system/14_モデル命名規則とベストプラクティス.md): リークチェックリスト

---

**作成者**: AI Assistant
**最終更新**: 2025-12-04
