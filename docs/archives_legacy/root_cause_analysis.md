# 特徴量生成パイプライン - 根本原因分析レポート

## 調査結果サマリー

**問題**: 新モデル(`mu_v2.1_fixed`)の性能が旧モデルより大幅に低下
- 回収率: -24% (0.767 → 0.582)
- Spearman相関: -40% (0.137 → 0.083)
- 的中率: -19% (0.316 → 0.257)

## 根本原因の特定

###  1. `past_X_finish_position_mean`系特徴量が100%ゼロ

**検証結果**:
```
過去着順平均特徴量: 4個

| 特徴量 | 欠損率 | 平均 | 中央値 | 最小 | 最大 | ゼロ率 | 標準偏差 |
|--------|--------|------|--------|-----|------|--------|----------|
| past_1_finish_position_mean  | 0.00% | 0.00 | 0.00 | 0.00 | 0.00 | 100.00% | 0.00 |
| past_3_finish_position_mean  | 0.00% | 0.00 | 0.00 | 0.00 | 0.00 | 100.00% | 0.00 |
| past_5_finish_position_mean  | 0.00% | 0.00 | 0.00 | 0.00 | 0.00 | 100.00% | 0.00 |
| past_10_finish_position_mean | 0.00% | 0.00 | 0.00 | 0.00 | 0.00 | 100.00% | 0.00 |
```

### 2. 問題のあるコードフロー

**FeatureEngine.generate_features**の処理フロー:
1. `shutuba_df`をコピー (`df`)
2. `_create_combined_timeseries_df(df, results_history_df)`を呼び出し
   - `history_df`に`is_target_race=0`を付与
   - `df`に`is_target_race=1`を付与
   - 両方を`concat`して結合
3. `_add_past_performance_features(combined_df)`を呼び出し
   - `groupby('horse_id')`で馬ごとにグループ化
   - `finish_position`カラムをシフト・ロール統計計算

**問題点**:
- `shutuba_df`には`finish_position`カラムが**存在しない**
- `concat`時に`shutuba_df`部分の`finish_position`が`NaN`になる
- その後の`.shift(1).rolling()`計算で全てゼロまたは`NaN`になる
- 欠損値処理(`_handle_missing_values`)で`0.0`に置換される

### 3. レース情報マージは成功していた

`distance_m`と`track_surface`は正常に生成されている:
- `distance_m`: 0%ゼロ (平均1660m)
- `track_芝`/`track_ダート`: 正常に分布

つまり、`generate_features.py`での**レース情報マージは成功**していたが、**過去走集計ロジック**に根本的な問題があった。

## 影響範囲

過去走集計系の特徴量が全滅:
- `past_X_finish_position_*`
- `past_X_margin_seconds_*`
- `past_X_last_3f_time_*`
- `past_X_passing_order_*`

これらは全てμモデルにとって**最も重要な特徴量**であり、性能の-40%低下の直接的原因。

##  修正案

### Option A: `_create_combined_timeseries_df`の修正

`shutuba_df`に`finish_position=NaN`を事前に追加:
```python
def _create_combined_timeseries_df(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    history_df = history_df.copy()
    history_df['is_target_race'] = 0
    
    df = df.copy()
    df['is_target_race'] = 1
    
    # ★ 修正: shutuba_dfにfinish_positionが無い場合はNaNで追加
    if 'finish_position' not in df.columns:
        df['finish_position'] = np.nan
    
    combined = pd.concat([history_df, df], ignore_index=True, sort=False)
    # ... 以下同様
```

### Option B: 過去走集計の前にフィルタリング

`finish_position`が存在する行のみで過去走集計:
```python
def _add_past_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
    # ... 現行コード ...
    
    for col in agg_cols:
        if col not in df.col ums:
            continue
        
        # ★ 修正: 有効なデータのみで集計
        valid_data = df[df[col].notna()]
        if valid_data.empty:
            continue
        
        grouped = valid_data.groupby('horse_id', sort=False)
        # ... 以下、valid_dataに対して集計 ...
```

## 推奨アクション

1. **Option A + B の組み合わせ**を実装
2. 修正後、2024年1月データで`past_X_finish_position_mean`が正常に生成されることを確認
3. 修正版で再トレーニング・評価
4. 旧モデルとの性能比較

## 次のステップ

- [ ] `feature_engine.py`の修正実装
- [ ] 特徴量再生成 (2024年1月でテスト)
- [ ] モデル再トレーニング
- [ ] 性能評価・比較
