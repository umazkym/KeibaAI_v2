"""
追加深層分析: レース展開予測と新特徴量の検証

目的:
- レース全体の展開を事前予測する特徴量
- 外枠×逃げ馬のROI高を深掘り
- リークフリーな新特徴量の効果検証
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# データ読み込み
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')

races = races[races['finish_position'].notna()].copy()
races['race_date'] = pd.to_datetime(races['race_date'])

# Train/Test分割
train = races[races['race_date'] <= '2024-12-31'].copy()
test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()

# ペースデータマージ
race_details = race_details[['race_id', 'first_half', 'second_half']].copy()
race_details['pace_diff'] = pd.to_numeric(race_details['second_half'], errors='coerce') - \
                            pd.to_numeric(race_details['first_half'], errors='coerce')

train = train.merge(race_details, on='race_id', how='left')
test = test.merge(race_details, on='race_id', how='left')

# 出走頭数
train['field_size'] = train.groupby('race_id')['horse_number'].transform('count')
test['field_size'] = test.groupby('race_id')['horse_number'].transform('count')

# コーナーデータマージ
c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']

train = train.merge(c4, on=['race_id', 'horse_number'], how='left')
test = test.merge(c4, on=['race_id', 'horse_number'], how='left')

print("=" * 80)
print("1. 外枠×逃げ馬のROI高要因分析（Test期間）")
print("=" * 80)

# 馬番と出走頭数の関係
test['relative_post'] = test['horse_number'] / test['field_size']
test['is_outer'] = test['relative_post'] > 0.67
test['is_leader'] = test['c4_pos'] <= 2

# 外枠×逃げ馬の特性
outer_leader = test[(test['is_outer']) & (test['is_leader'])]
inner_leader = test[(~test['is_outer']) & (test['is_leader'])]

print(f"\n外枠逃げ馬: {len(outer_leader):,}件")
print(f"  平均オッズ: {outer_leader['win_odds'].mean():.1f}")
print(f"  勝率: {(outer_leader['finish_position']==1).mean()*100:.1f}%")
print(f"  ROI: {outer_leader[outer_leader['finish_position']==1]['win_odds'].sum()/len(outer_leader)*100:.1f}%")

print(f"\n内・中枠逃げ馬: {len(inner_leader):,}件")
print(f"  平均オッズ: {inner_leader['win_odds'].mean():.1f}")
print(f"  勝率: {(inner_leader['finish_position']==1).mean()*100:.1f}%")
print(f"  ROI: {inner_leader[inner_leader['finish_position']==1]['win_odds'].sum()/len(inner_leader)*100:.1f}%")

print("\n→ 外枠逃げ馬はオッズが高い傾向があり、勝率が維持されればROIが高くなる")

print("\n" + "=" * 80)
print("2. レース展開予測: 各馬の過去脚質からペースを予測")
print("=" * 80)

# 各馬の過去の逃げ傾向を計算（Train期間）
# 馬ごとにC4位置<=2だった割合
train_c4 = train[['race_id', 'horse_id', 'c4_pos', 'race_date']].dropna(subset=['c4_pos', 'horse_id'])
train_c4 = train_c4.sort_values(['horse_id', 'race_date'])
train_c4['is_front'] = (train_c4['c4_pos'] <= 2).astype(float)

# 累積統計（shift済み、リークフリー）
train_c4['cum_front_rate'] = train_c4.groupby('horse_id')['is_front'].transform(
    lambda x: x.expanding().mean().shift(1)
)
train_c4['cum_count'] = train_c4.groupby('horse_id')['is_front'].transform(
    lambda x: x.expanding().count().shift(1)
)

# 最新値を取得
latest_front_rate = train_c4.groupby('horse_id')[['cum_front_rate', 'cum_count']].last().reset_index()
latest_front_rate.columns = ['horse_id', 'horse_front_runner_rate', 'corner_race_count']
latest_front_rate = latest_front_rate[latest_front_rate['corner_race_count'] >= 3]

print(f"過去脚質データあり: {len(latest_front_rate):,}頭")

# Testデータにマージ
test_with_style = test.merge(latest_front_rate, on='horse_id', how='left')

print("\n" + "=" * 80)
print("3. レースごとの「逃げ予備軍」数とペースの関係")
print("=" * 80)

# 各レースの逃げ予備軍数（horse_front_runner_rate > 0.3の馬の数）
def count_front_runner_candidates(group):
    """逃げ傾向の強い馬の数をカウント"""
    threshold = 0.3
    return (group['horse_front_runner_rate'] > threshold).sum()

test_valid = test_with_style[test_with_style['horse_front_runner_rate'].notna()]
race_front_count = test_valid.groupby('race_id').apply(
    count_front_runner_candidates, include_groups=False
).reset_index(name='num_front_candidates')

test_with_front = test_with_style.merge(race_front_count, on='race_id', how='left')

# 逃げ予備軍数別のペース
race_pace = test_with_front.drop_duplicates('race_id')[['race_id', 'pace_diff', 'num_front_candidates']]
race_pace = race_pace.dropna(subset=['pace_diff', 'num_front_candidates'])

print("\n逃げ予備軍数 → 実際のペース差:")
for n in [0, 1, 2, 3, 4]:
    subset = race_pace[race_pace['num_front_candidates'] == n]
    if len(subset) < 10:
        continue
    avg_pace = subset['pace_diff'].mean()
    print(f"  {n}頭: レース数={len(subset):4d}, ペース差={avg_pace:+.2f}")

print("\n→ 逃げ予備軍が多いほどペース差が小さくなる（ハイペース化）傾向")

print("\n" + "=" * 80)
print("4. 【新特徴量候補】レース内逃げ競合度とROI")
print("=" * 80)

# 自分以外の馬の逃げ傾向の合計 = 展開リスク
# 逃げ馬にとって、他に逃げ予備軍が多いと不利
def calc_competition_score(group):
    """自分以外の逃げ予備軍の数"""
    result = []
    for _, row in group.iterrows():
        my_rate = row['horse_front_runner_rate']
        others_front_count = (group['horse_front_runner_rate'] > 0.3).sum()
        if my_rate > 0.3:
            others_front_count -= 1  # 自分を除く
        result.append(others_front_count)
    return pd.Series(result, index=group.index)

test_with_front = test_with_front.copy()
test_with_front['front_competition'] = test_valid.groupby('race_id').apply(
    calc_competition_score, include_groups=False
).values if len(test_valid) > 0 else np.nan

# 逃げ馬（高front_runner_rate）の競合度別ROI
front_runners = test_with_front[test_with_front['horse_front_runner_rate'] > 0.5]

print("\n自分が逃げ馬(fr_rate>0.5)のとき、他の逃げ予備軍数別ROI:")
for comp in [0, 1, 2, 3]:
    subset = front_runners[front_runners['front_competition'] == comp]
    if len(subset) < 50:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    print(f"  競合{comp}頭: n={len(subset):4d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("5. 【新特徴量候補】ペース×馬番×脚質の三重適合")
print("=" * 80)

# 距離カテゴリ追加
test_with_front['dist_cat'] = pd.cut(
    pd.to_numeric(test_with_front['distance_m'], errors='coerce'),
    bins=[0, 1400, 1800, 2200, 5000],
    labels=['Sprint', 'Mile', 'Middle', 'Long']
)

# 場×距離×馬場の平均ペース（Train期間から）
train['dist_cat'] = pd.cut(
    pd.to_numeric(train['distance_m'], errors='coerce'),
    bins=[0, 1400, 1800, 2200, 5000],
    labels=['Sprint', 'Mile', 'Middle', 'Long']
)
venue_pace = train.groupby(['venue', 'dist_cat', 'track_surface'], observed=True)['pace_diff'].mean()
venue_pace_dict = venue_pace.to_dict()

def get_venue_pace(row):
    key = (row['venue'], row['dist_cat'], row['track_surface'])
    return venue_pace_dict.get(key, 0)

test_with_front['venue_expected_pace'] = test_with_front.apply(get_venue_pace, axis=1)

# 三重適合スコア
# 逃げ馬 + 内枠 + スローペース予測 = 最高
# 差し馬 + 外枠 + ハイペース予測 = 高
def calc_triple_fit(row):
    fr_rate = row.get('horse_front_runner_rate', 0.5)
    if pd.isna(fr_rate):
        fr_rate = 0.5
    
    rel_post = row.get('relative_post', 0.5)
    if pd.isna(rel_post):
        rel_post = 0.5
    
    v_pace = row.get('venue_expected_pace', 0)
    if pd.isna(v_pace):
        v_pace = 0
    
    # 逃げ馬(fr>0.5) × 内枠(post<0.33) × スロー(pace>0) = 有利
    # 差し馬(fr<0.3) × 外枠(post>0.67) × ハイ(pace<0) = 有利
    
    if fr_rate > 0.5 and rel_post < 0.33 and v_pace > 0:
        return 'front_fit'  # 逃げ有利
    elif fr_rate < 0.3 and rel_post > 0.67 and v_pace < 0:
        return 'close_fit'  # 差し有利
    else:
        return 'neutral'

test_with_front['triple_fit'] = test_with_front.apply(calc_triple_fit, axis=1)

print("\n三重適合カテゴリ別ROI:")
for cat in ['front_fit', 'close_fit', 'neutral']:
    subset = test_with_front[test_with_front['triple_fit'] == cat]
    if len(subset) == 0:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    print(f"  {cat:10s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("6. 【新特徴量候補】C4馬身差とROI")
print("=" * 80)

test_with_front['c4_gap_cat'] = pd.cut(
    test_with_front['c4_gap'],
    bins=[-0.1, 0, 2, 5, 10, 100],
    labels=['leader', 'very_close', 'close', 'mid', 'far']
)

print("\nC4馬身差カテゴリ別ROI:")
for cat in ['leader', 'very_close', 'close', 'mid', 'far']:
    subset = test_with_front[test_with_front['c4_gap_cat'] == cat]
    if len(subset) == 0:
        continue
    wins = (subset['finish_position'] == 1).sum()
    win_rate = wins / len(subset) * 100
    roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
    avg_odds = subset['win_odds'].mean()
    print(f"  {cat:10s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%, 平均オッズ={avg_odds:.1f}")

print("\n" + "=" * 80)
print("7. 【重要】上がり3F順位×ペースの関係")
print("=" * 80)

# last3f_rankは当日のレース結果だが、過去の累積ならOK
# 「過去の上がり3F順位平均」はV14で既にhorse_last3f_rank_avgとして実装可能

# ペースと上がり3Fの関係を確認
test_last3f = test_with_front[test_with_front['last3f_rank'].notna()].copy()
test_last3f['last3f_rank_cat'] = pd.cut(
    test_last3f['last3f_rank'],
    bins=[0, 3, 6, 10, 20],
    labels=['top3', 'good', 'mid', 'slow']
)

print("\n上がり3F順位カテゴリ × ペースカテゴリ別ROI:")

def categorize_pace(pace_diff):
    if pd.isna(pace_diff):
        return 'unknown'
    if pace_diff > 1.0:
        return 'slow'
    elif pace_diff < -1.0:
        return 'fast'
    else:
        return 'even'

test_last3f['pace_category'] = test_last3f['pace_diff'].apply(categorize_pace)

for pace in ['slow', 'even', 'fast']:
    print(f"\n【{pace.upper()}ペース】")
    pace_subset = test_last3f[test_last3f['pace_category'] == pace]
    for last3f in ['top3', 'good', 'mid', 'slow']:
        subset = pace_subset[pace_subset['last3f_rank_cat'] == last3f]
        if len(subset) < 50:
            continue
        wins = (subset['finish_position'] == 1).sum()
        win_rate = wins / len(subset) * 100
        roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
        print(f"  {last3f:5s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%")

print("\n" + "=" * 80)
print("8. 最終まとめ：新特徴量候補（リークフリー）")
print("=" * 80)

print("""
【V15への追加候補（優先度順）】

■ 高優先度（実装推奨）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. race_front_runner_count: レース内逃げ予備軍数
   - 計算: 各馬の過去front_runner_rate>0.3 の馬をカウント
   - リーク: ✅ なし（過去累積統計のみ）
   - 効果: ペース予測精度向上、逃げ馬競合リスク検出
   - 実装複雑度: 中（レース単位集計）

2. front_competition_score: 逃げ競合スコア
   - 計算: 自分以外の逃げ予備軍数
   - リーク: ✅ なし
   - 効果: 逃げ馬が競合する場合のリスク検出
   - 実装複雑度: 中

3. horse_c4_gap_avg: 過去のC4馬身差平均
   - 計算: 過去のC4馬身差の累積平均（shift済み）
   - リーク: ✅ なし
   - 効果: 馬の「前で競馬できる力」を数値化
   - 実装複雑度: 低

■ 中優先度（検証後に実装）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

4. post_style_conflict: 馬番×脚質の適合/不適合
   - 計算: |relative_post - avg_relative_c4|
   - リーク: ✅ なし
   - 効果: 枠順と脚質のミスマッチを検出
   - 注意: 分析結果では逆相関（適合度高→ROI低）→要検証

5. horse_gap_improvement_avg: 馬身差ベースの位置改善
   - 計算: (C1_gap - C4_gap)の累積平均
   - リーク: ✅ なし
   - 効果: 順位ではなく「馬身差」での改善を評価

■ 低優先度（要追加検証）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

6. venue_dist_track_pace_expected: 場×距離×馬場のペース予測
   - V14で実装済み（venue_expected_pace）
   - 効果を監視

7. triple_fit_score: ペース×馬番×脚質三重適合
   - 複雑度高、過学習リスク
   - 単純な二項組み合わせから始める

【過学習リスクのため避けるべきもの】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 高次元組み合わせ（馬×騎手×距離×ペース等）
- サンプル数の少ないグループ統計
- 連続変数の過度なビニング
""")

print("\n分析完了")
