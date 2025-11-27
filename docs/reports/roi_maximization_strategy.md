# μモデル ROI最大化戦略レポート (ROI Maximization Strategy)

**作成日**: 2025-11-27
**対象**: μモデル (Win Probability Model)
**目的**: 購入数を減らすことなく（フィルタリングなしで）、モデルの学習プロセス自体を改良してROIを向上させる。

## 1. 戦略の概要 (Executive Summary)

従来のAIモデル開発では「正解率（AUC/Accuracy）」の最大化がゴールでしたが、競馬においては「正解率が高いこと」と「儲かること」はイコールではありません。
本レポートでは、モデルの学習目標を「正解率」から「収益性（ROI）」にシフトさせるための2つの具体的な技術アプローチを詳述します。

1.  **ROI主導型ハイパーパラメータチューニング (ROI-driven Tuning)**
    *   Optunaの探索指標をAUCからROIに変更し、「儲かるパラメータ設定」を自動探索させる。
2.  **オッズ加重学習 (Odds-Weighted Training)**
    *   「100倍の馬の勝利」を「1.1倍の馬の勝利」よりも重要視して学習させ、穴馬検知能力を強化する。

---

## 2. 戦略詳細と実装ガイド

### 戦略1: ROI主導型ハイパーパラメータチューニング

Optunaが試行錯誤する際、「AUCが良かった設定」ではなく「シミュレーションROIが良かった設定」を採用するように変更します。

#### 理論的背景
AUCは「順位付けの正確さ」を見ますが、ROIは「1位の馬が勝った時のリターン」を見ます。
AUC最適化では「堅い本命馬」を確実に当てる設定が選ばれがちですが、ROI最適化では「多少本命を外しても、中穴を拾える設定（例: 正則化を弱める、深さを変える）」が選ばれる可能性があります。

#### 実装手順 (Implementation Steps)

`train_mu_v2_4_model.py` の `objective` 関数を以下のように書き換えます。

**変更前 (AUC最適化):**
```python
# ValidでのAUCを返す
preds = model.predict(valid_df[feature_cols])
auc = roc_auc_score(valid_df['target'], preds)
return auc
```

**変更後 (ROI最適化):**
```python
# Validでの予測
preds = model.predict(valid_df[feature_cols])
valid_df['pred_prob'] = preds

# レースごとの予測1位を特定
valid_df['rank_pred'] = valid_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')

# 予測1位の馬だけを購入した場合のシミュレーション
bet_df = valid_df[valid_df['rank_pred'] == 1].copy()

# オッズがないデータは除外（計算不能なため）
bet_df = bet_df.dropna(subset=['win_odds'])

# 的中判定と払い戻し計算
hits = bet_df[bet_df['target'] == 1]
return_amount = hits['win_odds'].sum()
bet_amount = len(bet_df)

# ROI計算
roi = return_amount / bet_amount if bet_amount > 0 else 0

return roi  # AUCではなくROIを返す
```

**注意点**:
*   検証データ (`valid_df`) に `win_odds` が含まれている必要があります（現在のスクリプトでは補完済みなのでOK）。
*   ROIはAUCに比べて分散（ばらつき）が大きいため、`n_trials`（試行回数）を多め（50〜100回）に設定することを推奨します。

---

### 戦略2: オッズ加重学習 (Odds-Weighted Training)

LightGBMの学習時に `sample_weight`（サンプル重み）を指定し、オッズが高い馬のデータほど「重要」であると教え込みます。

#### 理論的背景
通常、AIは全てのデータを平等に扱います。しかし競馬では、オッズ1.1倍の馬が勝つデータと、オッズ50倍の馬が勝つデータでは、後者の方が「予測できた時の価値」が圧倒的に高いです。
重み付けを行うことで、モデルは「穴馬の勝利パターン」を必死に学習しようとします。

#### 実装手順 (Implementation Steps)

`train_mu_v2_4_model.py` の `lgb.Dataset` 作成部分を修正します。

**ステップ1: 重みの計算**
単にオッズをそのまま重みにすると、大穴（100倍以上）の影響が強すぎて学習が不安定になります。
一般的には `log(odds)` や `sqrt(odds)` を使うか、キャップ（上限）を設けます。

```python
# 重みの計算例 (Log Odds)
# 1.0未満にならないように +1 して log をとるなどが一般的
train_df['weight'] = np.log1p(train_df['win_odds'].fillna(1.0))

# ターゲット（1着）のデータだけ重みを強くする場合
# 負けた馬（0）の重みは1.0のまま、勝った馬（1）だけオッズに応じて重み付けする手法も有効
train_df['weight'] = np.where(
    train_df['target'] == 1,
    np.log1p(train_df['win_odds']), # 勝った馬はオッズに応じて重視
    1.0                             # 負けた馬は通常通り
)
```

**ステップ2: Datasetへの適用**

```python
lgb_train = lgb.Dataset(
    train_df[feature_cols], 
    label=train_df['target'],
    weight=train_df['weight'] # 重みを指定
)
```

**期待される効果と副作用**:
*   **メリット**: 穴馬の的中率が向上し、ROIの爆発力が上がります。
*   **デメリット**: 本命馬の的中率が下がるため、全体の的中率（Accuracy）は低下する可能性があります。

---

## 3. 実施ロードマップ (Execution Roadmap)

以下の順序で段階的に実施することを推奨します。

### Phase 1: ROI主導チューニングの実施
コードの変更リスクが低く、副作用も少ないため、まずはこちらを実施します。

1.  `train_mu_v2_4_model.py` をコピーし、`train_mu_v2_5_roi_opt.py` を作成。
2.  `objective` 関数内の戻り値を `auc` から `roi` に変更。
3.  学習を実行し、v2.4と比較。
    *   **目標**: Accuracyを維持しつつ、ROI 80%超え。

### Phase 2: オッズ加重学習の実験
Phase 1で限界が見えた場合、またはさらなる向上を目指す場合に実施します。

1.  `train_mu_v2_5_roi_opt.py` をベースに、`train_mu_v2_6_weighted.py` を作成。
2.  学習データ作成時に `weight` カラムを追加。
3.  `lgb.Dataset` に `weight` を渡すように修正。
4.  学習を実行。
    *   **目標**: ROI 85%〜90%超え（ただし的中率低下の許容範囲を見極める）。

## 4. 結論

「購入数を絞らない」という制約下では、モデル自体に「稼ぐこと」を目的化させるこれらのアプローチが最適解です。
まずは **Phase 1 (ROI主導チューニング)** から着手し、AIがどのような「稼げる設定」を見つけ出すかを確認しましょう。
