"""
勝者と非勝者の分布差を深層分析
- オッズとの相関確認
- 有効な特徴量候補の評価
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = 'keibaai/data/parsed/parquet'

races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')

print("=" * 80)
print("1. horses.parquet - 生産地・生産者と勝率の関係")
print("=" * 80)

# racesとhorsesをマージ
races_with_horses = races.merge(horses[['horse_id', 'breeder_name', 'producing_area']], on='horse_id', how='left')

# 生産地別勝率
print("\n【producing_area 別勝率 (上位15)】")
area_stats = races_with_horses.groupby('producing_area').agg({
    'finish_position': 'count',
    'horse_id': lambda x: (races_with_horses.loc[x.index, 'finish_position'] == 1).sum()
}).rename(columns={'finish_position': 'races', 'horse_id': 'wins'})
area_stats['win_rate'] = area_stats['wins'] / area_stats['races']
area_stats = area_stats[area_stats['races'] >= 1000].sort_values('win_rate', ascending=False)
print(area_stats.head(15))

# オッズとの相関確認
print("\n【producing_area と win_oddsの相関】")
area_odds = races_with_horses.groupby('producing_area')['win_odds'].mean()
print("生産地別平均オッズ:")
print(area_odds[area_stats.index].head(10))

# 生産者別勝率
print("\n【breeder_name 別勝率 (上位15)】")
breeder_stats = races_with_horses.groupby('breeder_name').agg({
    'finish_position': 'count',
    'horse_id': lambda x: (races_with_horses.loc[x.index, 'finish_position'] == 1).sum()
}).rename(columns={'finish_position': 'races', 'horse_id': 'wins'})
breeder_stats['win_rate'] = breeder_stats['wins'] / breeder_stats['races']
breeder_stats = breeder_stats[breeder_stats['races'] >= 500].sort_values('win_rate', ascending=False)
print(breeder_stats.head(15))

print("\n" + "=" * 80)
print("2. corner_positions - 4コーナー位置と勝率の関係")
print("=" * 80)

# 4コーナーのみ抽出
c4 = corners[corners['corner'] == 4].copy()
print(f"4コーナーデータ: {len(c4):,}件")

# racesとマージ
races_with_c4 = races.merge(c4[['race_id', 'horse_number', 'position', 'gap_from_leader']], 
                             on=['race_id', 'horse_number'], how='left')

# 4コーナー位置別勝率
print("\n【4コーナー順位別 勝率】")
c4_stats = races_with_c4.groupby('position').agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
c4_stats.columns = ['races', 'wins']
c4_stats['win_rate'] = c4_stats['wins'] / c4_stats['races']
print(c4_stats[c4_stats['races'] >= 100].head(10))

# 4コーナー馬身差別勝率
print("\n【4コーナー馬身差別 勝率】")
races_with_c4['gap_category'] = pd.cut(races_with_c4['gap_from_leader'], 
                                        bins=[-0.1, 0, 2, 5, 10, 100], 
                                        labels=['先頭', '1-2馬身', '2-5馬身', '5-10馬身', '10馬身超'])
gap_stats = races_with_c4.groupby('gap_category', observed=True).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
gap_stats.columns = ['races', 'wins']
gap_stats['win_rate'] = gap_stats['wins'] / gap_stats['races']
print(gap_stats)

# オッズとの相関
print("\n【4コーナー順位とオッズの相関】")
c4_odds = races_with_c4.dropna(subset=['position', 'win_odds'])
corr = c4_odds['position'].corr(c4_odds['win_odds'])
print(f"相関係数: {corr:.3f}")

print("\n" + "=" * 80)
print("3. 上がり3F順位 (last3f_rank) の詳細分析")
print("=" * 80)

# last3f_rank別勝率
print("\n【last3f_rank別 勝率】")
l3f_stats = races.groupby('last3f_rank').agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
l3f_stats.columns = ['races', 'wins']
l3f_stats['win_rate'] = l3f_stats['wins'] / l3f_stats['races']
print(l3f_stats[l3f_stats['races'] >= 100].head(10))

# オッズとの相関
print("\n【last3f_rank とオッズの相関】")
l3f_odds = races.dropna(subset=['last3f_rank', 'win_odds'])
corr = l3f_odds['last3f_rank'].corr(l3f_odds['win_odds'])
print(f"相関係数: {corr:.3f}")

print("\n" + "=" * 80)
print("4. 血統 (generation=1: 父) と勝率の関係")
print("=" * 80)

# 父のみ抽出
sires = pedigrees[pedigrees['generation'] == 1].copy()
print(f"父データ: {len(sires):,}件")

# racesとマージ
races_with_sire = races.merge(sires[['horse_id', 'ancestor_id', 'ancestor_name']], 
                               on='horse_id', how='left')
races_with_sire = races_with_sire.rename(columns={'ancestor_id': 'sire_id', 'ancestor_name': 'sire_name'})

# 父別勝率
print("\n【父別勝率 (上位15)】")
sire_stats = races_with_sire.groupby('sire_name').agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
sire_stats.columns = ['races', 'wins']
sire_stats['win_rate'] = sire_stats['wins'] / sire_stats['races']
sire_stats = sire_stats[sire_stats['races'] >= 500].sort_values('win_rate', ascending=False)
print(sire_stats.head(15))

print("\n" + "=" * 80)
print("5. 母父 (generation=2の2番目) と勝率の関係")
print("=" * 80)

# generation=2で各馬の2番目を取得（母父）
gen2 = pedigrees[pedigrees['generation'] == 2].copy()
# 各馬の2番目のレコードを取得
damsires = gen2.groupby('horse_id').nth(1).reset_index()
print(f"母父データ: {len(damsires):,}件")

# racesとマージ
races_with_damsire = races.merge(damsires[['horse_id', 'ancestor_id', 'ancestor_name']], 
                                  on='horse_id', how='left')
races_with_damsire = races_with_damsire.rename(columns={'ancestor_id': 'damsire_id', 'ancestor_name': 'damsire_name'})

# 母父別勝率
print("\n【母父別勝率 (上位15)】")
damsire_stats = races_with_damsire.groupby('damsire_name').agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
damsire_stats.columns = ['races', 'wins']
damsire_stats['win_rate'] = damsire_stats['wins'] / damsire_stats['races']
damsire_stats = damsire_stats[damsire_stats['races'] >= 500].sort_values('win_rate', ascending=False)
print(damsire_stats.head(15))

print("\n" + "=" * 80)
print("6. 馬体重変動 (horse_weight_change) の詳細分析")
print("=" * 80)

# 馬体重変動カテゴリ
races['weight_change_cat'] = pd.cut(races['horse_weight_change'], 
                                     bins=[-100, -10, -4, 0, 4, 10, 100], 
                                     labels=['大減量(-10以下)', '減量(-4~-10)', '微減(-1~-4)', 
                                             '微増(0~4)', '増量(4~10)', '大増量(10+)'])

print("\n【馬体重変動別 勝率】")
wc_stats = races.groupby('weight_change_cat', observed=True).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
wc_stats.columns = ['races', 'wins']
wc_stats['win_rate'] = wc_stats['wins'] / wc_stats['races']
print(wc_stats)

# オッズとの相関
print("\n【馬体重変動とオッズの相関】")
wc_odds = races.dropna(subset=['horse_weight_change', 'win_odds'])
corr = wc_odds['horse_weight_change'].corr(wc_odds['win_odds'])
print(f"相関係数: {corr:.3f}")

print("\n" + "=" * 80)
print("7. 条件別（距離×馬場）の深層分析")
print("=" * 80)

# 距離×馬場別の統計
print("\n【距離×馬場別 勝率】")
cond_stats = races.groupby(['distance_category', 'track_surface']).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
cond_stats.columns = ['races', 'wins']
cond_stats['win_rate'] = cond_stats['wins'] / cond_stats['races']
print(cond_stats)

print("\n" + "=" * 80)
print("8. 休養日数の分析（races間隔）")
print("=" * 80)

# 馬ごとに前走からの日数を計算
races_sorted = races.sort_values(['horse_id', 'race_date'])
races_sorted['race_date'] = pd.to_datetime(races_sorted['race_date'])
races_sorted['days_since_last'] = races_sorted.groupby('horse_id')['race_date'].diff().dt.days

print("\n【休養日数別 勝率】")
races_sorted['rest_category'] = pd.cut(races_sorted['days_since_last'], 
                                        bins=[-1, 14, 28, 60, 90, 180, 1000], 
                                        labels=['連闘', '中2-4週', '中4-8週', '中2-3ヶ月', '中3-6ヶ月', '長期休養'])
rest_stats = races_sorted.groupby('rest_category', observed=True).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
rest_stats.columns = ['races', 'wins']
rest_stats['win_rate'] = rest_stats['wins'] / rest_stats['races']
print(rest_stats)

# オッズとの相関
print("\n【休養日数とオッズの相関】")
rest_odds = races_sorted.dropna(subset=['days_since_last', 'win_odds'])
corr = rest_odds['days_since_last'].corr(rest_odds['win_odds'])
print(f"相関係数: {corr:.3f}")

print("\n" + "=" * 80)
print("9. 斤量 (basis_weight) の分析")
print("=" * 80)

# 斤量別勝率
print("\n【斤量別 勝率】")
races['weight_cat'] = pd.cut(races['basis_weight'], 
                              bins=[48, 52, 54, 56, 58, 60], 
                              labels=['48-52kg', '52-54kg', '54-56kg', '56-58kg', '58-60kg'])
bw_stats = races.groupby('weight_cat', observed=True).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
bw_stats.columns = ['races', 'wins']
bw_stats['win_rate'] = bw_stats['wins'] / bw_stats['races']
print(bw_stats)

print("\n" + "=" * 80)
print("10. 枠番×馬場状態 の分析")
print("=" * 80)

# 枠番×馬場状態別勝率
print("\n【枠番×馬場状態別 勝率】")
bracket_cond = races.groupby(['bracket_number', 'track_condition']).agg({
    'finish_position': ['count', lambda x: (x == 1).sum()]
})
bracket_cond.columns = ['races', 'wins']
bracket_cond['win_rate'] = bracket_cond['wins'] / bracket_cond['races']
bracket_cond = bracket_cond[bracket_cond['races'] >= 500]
print(bracket_cond.sort_values('win_rate', ascending=False).head(15))

print("\n" + "=" * 80)
print("完了")
print("=" * 80)
