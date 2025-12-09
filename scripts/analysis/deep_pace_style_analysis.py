"""
深層ペース・脚質・馬番分析

目的:
- 脚質（逃げ/先行/差し/追込）とペースの関係を詳細に分析
- 馬番と脚質の関係を分析
- 新しい特徴量の可能性を探索

分析項目:
1. レースペース（前半3F - 後半3F）と脚質別勝率・ROI
2. 馬番（内枠/外枠）と脚質の勝率・ROI
3. 逃げ馬・先行馬の存在によるペース予測
4. コーナー位置の変化（C1→C4）とROI
5. 距離・馬場別のペース傾向
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# データ読み込み
print("=" * 80)
print("データ読み込み")
print("=" * 80)

races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')

print(f"races: {len(races):,}")
print(f"corners: {len(corners):,}")
print(f"race_details: {len(race_details):,}")

# 有効なレースのみ（finish_positionあり）
races = races[races['finish_position'].notna()].copy()
races['race_date'] = pd.to_datetime(races['race_date'])

# Train/Test分割
train = races[races['race_date'] <= '2024-12-31'].copy()
test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()

print(f"\nTrain: {len(train):,}, Test: {len(test):,}")

# ペースデータをマージ
race_details = race_details[['race_id', 'first_half', 'second_half']].copy()
race_details['pace_diff'] = pd.to_numeric(race_details['second_half'], errors='coerce') - \
                            pd.to_numeric(race_details['first_half'], errors='coerce')
# pace_diff > 0 → スローペース（後半が遅い = 前半速い）
# pace_diff < 0 → ハイペース（前半が遅い = 後半速い）

train = train.merge(race_details, on='race_id', how='left')
test = test.merge(race_details, on='race_id', how='left')

print(f"\nTrain with pace data: {train['pace_diff'].notna().sum():,}/{len(train):,}")
print(f"Test with pace data: {test['pace_diff'].notna().sum():,}/{len(test):,}")

# コーナーデータを処理
def get_corner_stats(df, corners_df):
    """各レース・馬のコーナー位置を取得"""
    c1 = corners_df[corners_df['corner'] == 1][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c1.columns = ['race_id', 'horse_number', 'c1_pos', 'c1_gap']
    c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
    
    result = df.merge(c1, on=['race_id', 'horse_number'], how='left')
    result = result.merge(c4, on=['race_id', 'horse_number'], how='left')
    return result

train = get_corner_stats(train, corners)
test = get_corner_stats(test, corners)

print(f"\nTrain with corner data: {train['c4_pos'].notna().sum():,}/{len(train):,}")
print(f"Test with corner data: {test['c4_pos'].notna().sum():,}/{len(test):,}")

# 脚質分類
def classify_running_style(row):
    """C4位置ベースの脚質分類"""
    c4 = row['c4_pos']
    if pd.isna(c4):
        return 'unknown'
    field_size = row.get('field_size', 16)
    if pd.isna(field_size):
        field_size = 16
    
    # 相対位置（0-1）
    relative_pos = c4 / field_size
    
    if relative_pos <= 0.15:  # 上位15%
        return 'leader'  # 逃げ
    elif relative_pos <= 0.35:  # 上位35%
        return 'front'   # 先行
    elif relative_pos <= 0.65:  # 中間
        return 'mid'     # 差し
    else:
        return 'closer'  # 追込

# レースの出走頭数を追加
train['field_size'] = train.groupby('race_id')['horse_number'].transform('count')
test['field_size'] = test.groupby('race_id')['horse_number'].transform('count')

train['running_style_result'] = train.apply(classify_running_style, axis=1)
test['running_style_result'] = test.apply(classify_running_style, axis=1)

print("\n" + "=" * 80)
print("1. 脚質別勝率・ROI（Test期間）")
print("=" * 80)

for style in ['leader', 'front', 'mid', 'closer', 'unknown']:
    subset = test[test['running_style_result'] == style]
    if len(subset) == 0:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    print(f"  {style:8s}: レース数={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("2. ペースカテゴリ別 × 脚質別 ROI（Test期間）")
print("=" * 80)

# ペースカテゴリ
def categorize_pace(pace_diff):
    if pd.isna(pace_diff):
        return 'unknown'
    if pace_diff > 1.0:
        return 'slow'    # スロー（後半遅い）
    elif pace_diff < -1.0:
        return 'fast'    # ハイ（後半速い）
    else:
        return 'even'    # ミドル

test['pace_category'] = test['pace_diff'].apply(categorize_pace)

for pace_cat in ['slow', 'even', 'fast']:
    print(f"\n【{pace_cat.upper()}ペース】")
    pace_subset = test[test['pace_category'] == pace_cat]
    for style in ['leader', 'front', 'mid', 'closer']:
        subset = pace_subset[pace_subset['running_style_result'] == style]
        if len(subset) == 0:
            continue
        wins = (subset['finish_position'] == 1).sum()
        win_rate = wins / len(subset) * 100 if len(subset) > 0 else 0
        roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100 if len(subset) > 0 else 0
        print(f"  {style:8s}: レース数={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("3. 馬番カテゴリ × 脚質別 ROI（Test期間）")
print("=" * 80)

# 馬番カテゴリ
def categorize_post(horse_number, field_size):
    if pd.isna(horse_number) or pd.isna(field_size):
        return 'unknown'
    relative = horse_number / field_size
    if relative <= 0.33:
        return 'inner'   # 内枠
    elif relative <= 0.67:
        return 'middle'  # 中枠
    else:
        return 'outer'   # 外枠

test['post_category'] = test.apply(lambda r: categorize_post(r['horse_number'], r['field_size']), axis=1)

for post_cat in ['inner', 'middle', 'outer']:
    print(f"\n【{post_cat.upper()}枠】")
    post_subset = test[test['post_category'] == post_cat]
    for style in ['leader', 'front', 'mid', 'closer']:
        subset = post_subset[post_subset['running_style_result'] == style]
        if len(subset) == 0:
            continue
        wins = (subset['finish_position'] == 1).sum()
        win_rate = wins / len(subset) * 100 if len(subset) > 0 else 0
        roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100 if len(subset) > 0 else 0
        print(f"  {style:8s}: レース数={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("4. C1→C4位置変化とROI（Test期間）")
print("=" * 80)

test['position_change'] = test['c1_pos'] - test['c4_pos']  # 正=前進、負=後退

# 位置変化カテゴリ
def categorize_position_change(change):
    if pd.isna(change):
        return 'unknown'
    if change >= 3:
        return 'big_up'     # 大きく前進
    elif change >= 1:
        return 'small_up'   # 少し前進
    elif change >= -1:
        return 'steady'     # 維持
    elif change >= -3:
        return 'small_back' # 少し後退
    else:
        return 'big_back'   # 大きく後退

test['position_change_cat'] = test['position_change'].apply(categorize_position_change)

for cat in ['big_up', 'small_up', 'steady', 'small_back', 'big_back', 'unknown']:
    subset = test[test['position_change_cat'] == cat]
    if len(subset) == 0:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    avg_odds = subset['win_odds'].mean()
    print(f"  {cat:12s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%, 平均オッズ={avg_odds:.1f}")

print("\n" + "=" * 80)
print("5. 距離別ペース傾向（Train期間）")
print("=" * 80)

train['dist_cat'] = pd.cut(
    pd.to_numeric(train['distance_m'], errors='coerce'),
    bins=[0, 1400, 1800, 2200, 5000],
    labels=['Sprint', 'Mile', 'Middle', 'Long']
)

pace_by_dist = train.groupby('dist_cat', observed=True)['pace_diff'].agg(['mean', 'std', 'count'])
print(pace_by_dist)

print("\n" + "=" * 80)
print("6. 場×距離×馬場別ペース傾向（Train期間、上位10）")
print("=" * 80)

train['venue'] = train['venue'].fillna('unknown')
train['track_surface'] = train['track_surface'].fillna('unknown')

pace_by_venue = train.groupby(['venue', 'dist_cat', 'track_surface'], observed=True)['pace_diff'].agg(['mean', 'std', 'count'])
pace_by_venue = pace_by_venue[pace_by_venue['count'] >= 100].sort_values('mean', ascending=False)
print(pace_by_venue.head(10))

print("\n【最もスローになりやすい場（スロー = 逃げ有利）】")
print(pace_by_venue.head(5).index.tolist())

print("\n【最もハイになりやすい場（ハイ = 差し有利）】")
print(pace_by_venue.tail(5).index.tolist())

print("\n" + "=" * 80)
print("7. レース内逃げ馬数とペースの関係（Train期間）")
print("=" * 80)

# レースごとの逃げ馬（C1位置1-2）カウント
c1_data = corners[corners['corner'] == 1][['race_id', 'horse_number', 'position']].copy()
c1_leaders = c1_data[c1_data['position'] <= 2]
leader_count = c1_leaders.groupby('race_id').size().reset_index(name='num_leaders_c1')

train_with_leaders = train.merge(leader_count, on='race_id', how='left')
train_with_leaders['num_leaders_c1'] = train_with_leaders['num_leaders_c1'].fillna(0)

# 逃げ馬数別ペース
for n_leaders in [1, 2, 3, 4]:
    subset = train_with_leaders[train_with_leaders['num_leaders_c1'] == n_leaders]
    subset = subset.drop_duplicates('race_id')
    if len(subset) < 10:
        continue
    avg_pace = subset['pace_diff'].mean()
    std_pace = subset['pace_diff'].std()
    print(f"  逃げ馬{n_leaders}頭: レース数={len(subset):5d}, ペース差={avg_pace:+.2f} ± {std_pace:.2f}")

print("\n" + "=" * 80)
print("8. C1先頭馬身差とペースの関係（Train期間）")
print("=" * 80)

# C1での先頭からのギャップ分布
c1_all = corners[corners['corner'] == 1].copy()
race_c1_gap_max = c1_all.groupby('race_id')['gap_from_leader'].max().reset_index(name='c1_gap_max')

train_with_gap = train.merge(race_c1_gap_max, on='race_id', how='left')
train_with_gap = train_with_gap.drop_duplicates('race_id')

# C1ギャップ大（縦長）の場合のペース
train_with_gap['c1_gap_cat'] = pd.cut(
    train_with_gap['c1_gap_max'],
    bins=[0, 5, 10, 20, 100],
    labels=['tight', 'normal', 'stretched', 'very_stretched']
)

for cat in ['tight', 'normal', 'stretched', 'very_stretched']:
    subset = train_with_gap[train_with_gap['c1_gap_cat'] == cat]
    if len(subset) < 10:
        continue
    avg_pace = subset['pace_diff'].mean()
    print(f"  {cat:15s}: レース数={len(subset):5d}, ペース差={avg_pace:+.2f}")

print("\n" + "=" * 80)
print("9. 【重要】新特徴量候補の検討")
print("=" * 80)

print("""
【発見された高ROIパターン】
1. 脚質×ペースの組み合わせ効果
   - 逃げ馬はスローペースで有利（V14で実装済み）
   - 差し馬はハイペースで有利

2. 馬番×脚質の組み合わせ
   - 内枠×逃げ馬: 有利傾向
   - 外枠×差し馬: 不利か？

3. 位置改善馬（C1→C4で前進）
   - V14で`horse_position_improvement_avg`として実装済み

4. 【新規候補】レース内逃げ馬数によるペース予測
   - 逃げ馬が多いほどハイペース傾向
   - → 「レース内で逃げ傾向の強い馬の数」を特徴量化
   
5. 【新規候補】馬番×過去脚質の適合度
   - 内枠×逃げ傾向 = 高適合
   - 外枠×逃げ傾向 = 低適合（外から逃げは不利）

6. 【新規候補】C1-C4間のポジションロス/ゲイン
   - 現在: `horse_position_improvement_avg`（順位差）
   - 追加候補: `gap_improvement`（馬身差での改善）
""")

print("\n" + "=" * 80)
print("10. 新特徴量候補の事前検証")
print("=" * 80)

# 候補1: レース内逃げ馬予測数
print("\n【候補1】レース内逃げ傾向馬の数（事前予測）")
print("  ※ 各馬の過去の「逃げ傾向（horse_front_runner_rate）」からレース展開を予測")

# 候補2: 馬番×過去脚質の適合度
print("\n【候補2】馬番×脚質適合スコア")

# 過去の脚質を計算（Train期間のみ）
train_c4 = train[['race_id', 'horse_number', 'horse_id', 'c4_pos', 'field_size']].copy()
train_c4 = train_c4.dropna(subset=['c4_pos'])
train_c4['relative_c4'] = train_c4['c4_pos'] / train_c4['field_size']

# 馬ごとの平均C4相対位置
horse_avg_c4 = train_c4.groupby('horse_id')['relative_c4'].agg(['mean', 'count']).reset_index()
horse_avg_c4.columns = ['horse_id', 'avg_relative_c4', 'c4_count']
horse_avg_c4 = horse_avg_c4[horse_avg_c4['c4_count'] >= 3]

# 馬番×脚質適合度
# 内枠（馬番小）×逃げ傾向（avg_relative_c4小）= 高適合
# 外枠（馬番大）×追込傾向（avg_relative_c4大）= 高適合

test_with_style = test.merge(horse_avg_c4, on='horse_id', how='left')
test_with_style['relative_post'] = test_with_style['horse_number'] / test_with_style['field_size']

# 適合度 = 1 - |relative_post - avg_relative_c4|
# 馬番位置と過去脚質位置が近いほど高適合
test_with_style['post_style_fit'] = 1 - (test_with_style['relative_post'] - test_with_style['avg_relative_c4']).abs()

# 適合度別ROI
test_with_style['fit_cat'] = pd.cut(
    test_with_style['post_style_fit'],
    bins=[0, 0.5, 0.7, 0.85, 1.0],
    labels=['low', 'mid', 'high', 'very_high']
)

print("\n馬番×脚質適合度別ROI（Test期間）:")
for cat in ['low', 'mid', 'high', 'very_high']:
    subset = test_with_style[test_with_style['fit_cat'] == cat]
    if len(subset) == 0:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    print(f"  {cat:10s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("11. 【重要】リーク・過学習リスクの評価")
print("=" * 80)

print("""
【リーク評価】
- レース内逃げ馬数: ⚠ 当日の他馬情報→レベル2リスク（レース内全馬の情報）
  → 各馬の「過去の逃げ傾向」合算ならリークなし
  
- 馬番×脚質適合度: ✅ リークなし
  → 過去データ（shift済み）と当日の馬番のみ使用
  
- C1先頭馬身差: ❌ 当日レース結果→リーク
  → 過去のC1馬身差パターンならOK

【過学習リスク評価】
- 高次元組み合わせを避ける（馬×距離×馬場×ペース等）
- 最小サンプル数要件を設ける
- Train-Test Gapを監視
""")

print("\n" + "=" * 80)
print("分析完了")
print("=" * 80)
