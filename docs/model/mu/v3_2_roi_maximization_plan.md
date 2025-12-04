# μモデル v3.2 ROI最大化計画書 (開発者向け詳細仕様)

## 1. はじめに
本ドキュメントは、μモデル（ランキング学習）単体で **ROI 100% 超** を達成するための詳細な実装仕様書です。
開発者は、本セクションの指示に従い、`scripts/training/train_mu_v3_0_ranker.py` および関連ファイルを改修してください。

## 2. 改修のゴール
*   **ターゲット**: Top 1 ROI (2024年 Testデータ) > **100%**
*   **手段**: 「的中率（正解数）」ではなく「**獲得賞金（オッズ加重正解数）**」を最大化するようにモデルを再教育する。

---

## 3. 実装詳細 (Implementation Details)

以下の3つの変更を適用します。

### 3.1 オッズ加重学習 (Odds-Weighted Training)

LightGBMの損失関数に対し、オッズに基づく重みを適用します。これにより、低配当の的中よりも高配当の的中を重視させます。

*   **対象ファイル**: `scripts/training/train_mu_v3_0_ranker.py`
*   **変更箇所**: `train_model` メソッド内の `model.fit` 呼び出し部分。

**実装コード例**:
```python
# 1. 重み (Sample Weight) の作成
# log1p(odds) を使用することで、外れ値（万馬券など）による勾配爆発を防ぎつつ、
# オッズが高いほど重要度を高くする。
# さらに clip(upper=100) で極端な大穴（単勝万馬券以上）の影響を制限する。
train_df['sample_weight'] = np.log1p(train_df['win_odds']).clip(upper=np.log1p(100))
valid_df['sample_weight'] = np.log1p(valid_df['win_odds']).clip(upper=np.log1p(100))

# 2. モデル学習時の指定
model.fit(
    train_df[feature_cols],
    train_df['target_relevance'],
    group=group_train,
    sample_weight=train_df['sample_weight'],  # 【追加】学習データの重み
    eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
    eval_group=[group_valid],
    eval_sample_weight=[valid_df['sample_weight']], # 【追加】検証データの重み
    eval_metric='ndcg',
    callbacks=[lgb.early_stopping(stopping_rounds=50)]
)
```

### 3.2 ターゲット変数の再設計 (Label Engineering)

「1着のみ」の評価から、「2-3着の穴馬」も評価する形に変更し、学習を安定化させます。

*   **対象ファイル**: `scripts/training/train_mu_v3_0_ranker.py`
*   **変更箇所**: `train_model` メソッド冒頭の `target_relevance` 作成部分。

**実装コード例**:
```python
# 変更前: (1着) * log(odds)
# gain = (df['finish_position'] == 1).astype(int) * np.log1p(df['win_odds'].fillna(0))

# 変更後: 1着を重視しつつ、2-3着の高配当馬にも部分点を与える
# 外れ値対策として、オッズは最大100倍（log1p(100)≒4.6）程度で頭打ちにする
def calculate_gain(row):
    odds = min(row['win_odds'], 100.0) if pd.notnull(row['win_odds']) else 1.0
    log_odds = np.log1p(odds)
    
    if row['finish_position'] == 1:
        return log_odds * 10.0  # 1着は強く評価
    elif row['finish_position'] == 2:
        return log_odds * 3.0   # 2着も少し評価
    elif row['finish_position'] == 3:
        return log_odds * 1.0   # 3着もわずかに評価
    else:
        return 0.0

df['target_gain'] = df.apply(calculate_gain, axis=1)

# LightGBM Ranker用に整数化 (0-30程度の範囲に収める)
df['target_relevance'] = df['target_gain'].astype(int)
```

### 3.3 割安感特徴量の追加 (Value-Gap Features)

「実力（勝率）」に対して「オッズ（配当）」が美味しいかどうかを示す特徴量を追加します。
**重要**: ここでは学習時のリークを防ぐため、`win_odds`（確定オッズ）を **特徴量 (X) として直接使用することは厳禁** です。

*   **方針**: オッズそのものではなく、「オッズに影響を与える因子（人気）」と「実力」の乖離を利用します。
*   **対象ファイル**: `scripts/training/train_mu_v3_0_ranker.py` (特徴量生成部分)

**実装コード例**:
```python
# オッズを使わず、人気ランクと実力ランクの乖離を見る
# popularity (人気) はレース直前まで変動するため、学習データでは確定人気を使用するが、
# これは「大衆の評価」のプロキシとして機能する。

# 1. 実力ランクの推定 (過去走の着順平均などから)
# (注: これは簡易的なものであり、モデル自体が学習するものだが、明示的な特徴量として与えることで収束を早める)
df['ability_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True)

# 2. 人気ランク (popularity)
# df['popularity'] は既に存在

# 3. ギャップ特徴量 (プラスが大きいほど「実力の割に人気がない」= 穴馬候補)
df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']

# 特徴量リストに追加
feature_cols.append('gap_ability_popularity')
```

---

## 4. 検証手順 (Verification Steps)

実装後、以下のコマンドで学習と検証を行ってください。

1.  **学習実行**:
    ```bash
    python scripts/training/train_mu_v3_0_ranker.py --n_trials 50
    ```
2.  **ログ確認**:
    *   `Top 1 ROI` が **100%** を超えているか確認する。
    *   `Top 1 Accuracy` が極端に低下していないか（目安: 15%以上）確認する。

---

## 5. 将来的なシステム拡張 (σ・νモデル改修方針)

μモデルの改修完了後、σ・νモデルも従来の「タイム予測誤差」から役割を変更します。

### 5.1 新しい役割分担
μモデルが「ランキング（相対評価）」を出力するため、後段のモデルで「確率（絶対評価）」と「投資判断」を行います。

| モデル | 新しい役割 | 入力 | 出力 |
| :--- | :--- | :--- | :--- |
| **μ (Ranker)** | **候補選出** | 特徴量 | ランク順位 (Score) |
| **σ (Calibrator)** | **品質保証** | μスコア, 2位との差, 混戦度 | **勝率 (Probability)** |
| **ν (Investor)** | **投資判断** | 勝率, オッズ, 期待値 | **予測ROI / 投資額** |

### 5.2 σモデル (Calibration Model)
*   **目的**: μモデルのスコアを、実際の勝率 (0.0〜1.0) に変換する。
*   **実装**: ロジスティック回帰やIsotonic Regressionを用い、「スコア10の馬は勝率30%」といったマッピングを行う。

### 5.3 νモデル (Investment Model)
*   **目的**: 「勝率 × オッズ」が 1.0 (100%) を超える馬のみを選別し、資金配分を決める。

---

## 6. 実運用フロー (Daily Workflow)

計算負荷の高い処理を朝に集約し、直前は瞬時の判断のみを行う構成にします。

### 朝イチ (09:00頃) - Heavy Processing
全レースに対して以下の重い処理を実行し、**「購入候補リスト」と「資金配分」** を確定させます。

1.  **μ推論**: 全レースのランキング算出。
2.  **σ推論**: 各馬の「勝率」を確定。
3.  **ν推論 (予備)**: 朝オッズ時点での期待値を計算し、各レースへの予算配分（例: 1000円〜5000円）を決定。

### レース5分前 - Light Processing
直前オッズを取得し、**可否判断 (Go/No-Go)** のみを行います。

1.  **オッズ取得**: 最新の単勝オッズを取得。
2.  **瞬時判定**:
    *   `期待値 = 朝算出した勝率(σ) × 最新オッズ`
    *   **判定**: `期待値 > 閾値 (例: 1.1)` なら **GO (投票)**。
    *   オッズが下がりすぎて期待値が割れていれば **STOP (見送り)**。

このフローにより、直前の計算時間は数ミリ秒で済み、余裕を持って投票可能です。
