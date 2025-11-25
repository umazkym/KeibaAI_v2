# 深刻な問題の分析 - なぜ修正が機能しないのか

## 問題の詳細分析

### データフローの再検証

#### Step 1: `_create_combined_timeseries_df`
```
Input:
- df (shutuba): finish_position = 存在しない → NaN追加
- history_df (races): finish_position = 実データあり

Output:
- combined_df: 
  - history行 (is_target_race=0): finish_position = 実データ
  - shutuba行 (is_target_race=1): finish_position = NaN
```

#### Step 2: `_add_past_performance_features`
```
処理:
- grouped = combined_df.groupby('horse_id')
- shifted = grouped['finish_position'].shift(1)
- rolled = shifted.rolling(window=w)
- past_X_finish_position_mean = rolled.mean()

結果:
- history行: 過去走データから正しく集計 → 非ゼロ値
- shutuba行: 直前の値がhistory行からshiftされる → **本来はここで非ゼロ値が入るべき**
```

#### Step 3: `generate_features`の最終フィルタリング
```python
if 'is_target_race' in combined_df.columns:
    generated_features = combined_df[combined_df['is_target_race'] == 1]
    # ... マージ処理
```

### 問題の核心

**ログでは195,083行全体で非ゼロ値が生成されているのに、なぜshutuba行(is_target_race=1)ではゼロになるのか?**

考えられる原因:
1. **マージ時の問題**: `generated_features`と元の`df`をマージする際に、列が重複してデータが失われている
2. **欠損値処理のタイミング**: `_handle_missing_values`が過去走集計の後に実行され、shutuba行の`past_X_finish_position_mean`を`NaN`→`0`に変換している
3. **フィルタリングのタイミング**: `is_target_race=1`でフィルタリングする前に、何らかの理由でデータが失われている

## 検証が必要な箇所

### 1. `generate_features`のマージロジック (Line 52-56)
```python
if 'is_target_race' in combined_df.columns:
    generated_features = combined_df[combined_df['is_target_race'] == 1]
    cols_to_merge = [c for c in generated_features.columns if c not in df.columns or c in ['race_id', 'horse_id']]
    df = df.merge(generated_features[cols_to_merge], on=['race_id', 'horse_id'], how='left')
```

**問題の可能性**: `past_X_finish_position_mean`が`df.columns`に既に存在する場合、マージされない

### 2. `_handle_missing_values`の実行タイミング
現在のコードフローでは、`_handle_missing_values`がいつ実行されているか不明確

## 推奨修正アプローチ

### Option A: マージロジックの修正
`cols_to_merge`の条件を変更:
```python
cols_to_merge = [c for c in generated_features.columns 
                 if c.startswith('past_') or c not in df.columns or c in ['race_id', 'horse_id']]
```

### Option B: 欠損値処理の除外
`_handle_missing_values`で`past_`系を除外:
```python
cols_to_skip = [col for col in df.columns if col.startswith('past_')]
cols_to_fill = [col for col in num_cols if col in temp_feature_names and col not in cols_to_skip]
```

### Option C: デバッグ出力の追加
各ステップでデータを保存し、どこで問題が発生しているか特定
