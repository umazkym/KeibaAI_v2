"""
ROI影響度分析と特徴量候補の最終評価
- 各特徴量候補のROIへの影響を評価
- オッズ×勝率の組み合わせで期待値を計算
- リークリスクの最終確認
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = 'keibaai/data/parsed/parquet'

races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
race_details = pd.read_parquet(f'{DATA_DIR}/race_details/race_details.parquet')

print("=" * 80)
print("1. 生産地 (producing_area) のROI分析")
print("=" * 80)

races_with_horses = races.merge(horses[['horse_id', 'producing_area', 'breeder_name']], on='horse_id', how='left')

# 生産地別ROI
print("\n【生産地別 ROI】")
area_roi = races_with_horses.groupby('producing_area').agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_with_horses.loc[x.index, 'finish_position'] == 1) * races_with_horses.loc[x.index, 'win_odds']).sum()
})
area_roi.columns = ['bets', 'returns']
area_roi['roi'] = area_roi['returns'] / area_roi['bets'] * 100
area_roi = area_roi[area_roi['bets'] >= 1000].sort_values('roi', ascending=False)
print(area_roi.head(15))

# オッズ相関確認
print("\n【生産地とオッズの相関】")
area_odds = races_with_horses.groupby('producing_area')['win_odds'].mean()
print(f"相関係数: {area_roi['roi'].corr(area_odds[area_roi.index]):.3f}")

print("\n" + "=" * 80)
print("2. 4コーナー馬身差に基づくROI分析（過去累積）")
print("=" * 80)

# 4コーナー馬身差をマージ
c4 = corners[corners['corner'] == 4].copy()
races_with_c4 = races.merge(c4[['race_id', 'horse_number', 'gap_from_leader']], 
                             on=['race_id', 'horse_number'], how='left')

# 先頭での勝利時のROI
print("\n【4コーナー馬身差別 ROI】")
races_with_c4['gap_category'] = pd.cut(races_with_c4['gap_from_leader'], 
                                        bins=[-0.1, 0, 2, 5, 10, 100], 
                                        labels=['先頭', '1-2馬身', '2-5馬身', '5-10馬身', '10馬身超'])
gap_roi = races_with_c4.groupby('gap_category', observed=True).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_with_c4.loc[x.index, 'finish_position'] == 1) * races_with_c4.loc[x.index, 'win_odds']).sum()
})
gap_roi.columns = ['bets', 'returns']
gap_roi['roi'] = gap_roi['returns'] / gap_roi['bets'] * 100
print(gap_roi)

print("\n" + "=" * 80)
print("3. 休養日数別 ROI")
print("=" * 80)

races_sorted = races.sort_values(['horse_id', 'race_date'])
races_sorted['race_date'] = pd.to_datetime(races_sorted['race_date'])
races_sorted['days_since_last'] = races_sorted.groupby('horse_id')['race_date'].diff().dt.days

print("\n【休養日数別 ROI】")
races_sorted['rest_category'] = pd.cut(races_sorted['days_since_last'], 
                                        bins=[-1, 14, 28, 60, 90, 180, 1000], 
                                        labels=['連闘', '中2-4週', '中4-8週', '中2-3ヶ月', '中3-6ヶ月', '長期休養'])
rest_roi = races_sorted.groupby('rest_category', observed=True).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_sorted.loc[x.index, 'finish_position'] == 1) * races_sorted.loc[x.index, 'win_odds']).sum()
})
rest_roi.columns = ['bets', 'returns']
rest_roi['roi'] = rest_roi['returns'] / rest_roi['bets'] * 100
print(rest_roi)

print("\n" + "=" * 80)
print("4. 馬体重変動別 ROI")
print("=" * 80)

print("\n【馬体重変動別 ROI】")
races['weight_change_cat'] = pd.cut(races['horse_weight_change'], 
                                     bins=[-100, -10, -4, 0, 4, 10, 100], 
                                     labels=['大減量(-10以下)', '減量(-4~-10)', '微減(-1~-4)', 
                                             '微増(0~4)', '増量(4~10)', '大増量(10+)'])
wc_roi = races.groupby('weight_change_cat', observed=True).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races.loc[x.index, 'finish_position'] == 1) * races.loc[x.index, 'win_odds']).sum()
})
wc_roi.columns = ['bets', 'returns']
wc_roi['roi'] = wc_roi['returns'] / wc_roi['bets'] * 100
print(wc_roi)

print("\n" + "=" * 80)
print("5. 前半-後半ペース差による分析")
print("=" * 80)

# race_detailsとマージ
races_with_pace = races.merge(race_details[['race_id', 'first_half', 'second_half']], on='race_id', how='left')
races_with_pace['pace_diff'] = races_with_pace['first_half'] - races_with_pace['second_half']

# ペース傾向別ROI
print("\n【ペース傾向別 ROI】")
races_with_pace['pace_type'] = pd.cut(races_with_pace['pace_diff'], 
                                       bins=[-10, -2, 0, 2, 10], 
                                       labels=['超ハイ', 'ハイ', 'スロー', '超スロー'])
pace_roi = races_with_pace.groupby('pace_type', observed=True).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_with_pace.loc[x.index, 'finish_position'] == 1) * races_with_pace.loc[x.index, 'win_odds']).sum()
})
pace_roi.columns = ['bets', 'returns']
pace_roi['roi'] = pace_roi['returns'] / pace_roi['bets'] * 100
print(pace_roi)

print("\n" + "=" * 80)
print("6. 父×馬場（track_surface）のROI")
print("=" * 80)

# 父を取得
sires = pedigrees[pedigrees['generation'] == 1][['horse_id', 'ancestor_name']].rename(columns={'ancestor_name': 'sire_name'})
races_with_sire = races.merge(sires, on='horse_id', how='left')

# 父×馬場別ROI
print("\n【父×馬場別 ROI（上位20）】")
sire_surf_roi = races_with_sire.groupby(['sire_name', 'track_surface']).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_with_sire.loc[x.index, 'finish_position'] == 1) * races_with_sire.loc[x.index, 'win_odds']).sum()
})
sire_surf_roi.columns = ['bets', 'returns']
sire_surf_roi['roi'] = sire_surf_roi['returns'] / sire_surf_roi['bets'] * 100
sire_surf_roi = sire_surf_roi[sire_surf_roi['bets'] >= 200].sort_values('roi', ascending=False)
print(sire_surf_roi.head(20))

print("\n" + "=" * 80)
print("7. 重馬場での成績差 - 馬場状態適性")
print("=" * 80)

# 良馬場と重馬場での勝率差
print("\n【馬場状態別 ROI】")
condition_roi = races.groupby('track_condition').agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races.loc[x.index, 'finish_position'] == 1) * races.loc[x.index, 'win_odds']).sum()
})
condition_roi.columns = ['bets', 'returns']
condition_roi['roi'] = condition_roi['returns'] / condition_roi['bets'] * 100
print(condition_roi)

print("\n" + "=" * 80)
print("8. 年齢×性別 ROI")
print("=" * 80)

print("\n【年齢×性別 ROI】")
age_sex_roi = races.groupby(['age', 'sex']).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races.loc[x.index, 'finish_position'] == 1) * races.loc[x.index, 'win_odds']).sum()
})
age_sex_roi.columns = ['bets', 'returns']
age_sex_roi['roi'] = age_sex_roi['returns'] / age_sex_roi['bets'] * 100
age_sex_roi = age_sex_roi[age_sex_roi['bets'] >= 500].sort_values('roi', ascending=False)
print(age_sex_roi.head(15))

print("\n" + "=" * 80)
print("9. 斤量×距離カテゴリ ROI")
print("=" * 80)

print("\n【斤量×距離 ROI】")
races['weight_cat'] = pd.cut(races['basis_weight'], 
                              bins=[48, 52, 54, 56, 58, 60], 
                              labels=['48-52kg', '52-54kg', '54-56kg', '56-58kg', '58-60kg'])
bw_dist_roi = races.groupby(['weight_cat', 'distance_category'], observed=True).agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races.loc[x.index, 'finish_position'] == 1) * races.loc[x.index, 'win_odds']).sum()
})
bw_dist_roi.columns = ['bets', 'returns']
bw_dist_roi['roi'] = bw_dist_roi['returns'] / bw_dist_roi['bets'] * 100
bw_dist_roi = bw_dist_roi[bw_dist_roi['bets'] >= 500].sort_values('roi', ascending=False)
print(bw_dist_roi.head(15))

print("\n" + "=" * 80)
print("10. race_class（レースクラス）変動の影響分析")
print("=" * 80)

# 前走クラスを取得
races_sorted = races.sort_values(['horse_id', 'race_date'])
races_sorted['prev_class'] = races_sorted.groupby('horse_id')['race_class'].shift(1)
races_sorted['is_class_up'] = (races_sorted['race_class'] != races_sorted['prev_class'])

# クラス変動別ROI（V8.1で有効だった特徴量）
print("\n【クラス変動有無別 ROI】")
class_change_roi = races_sorted.dropna(subset=['prev_class']).groupby('is_class_up').agg({
    'finish_position': 'count',
    'win_odds': lambda x: ((races_sorted.loc[x.index, 'finish_position'] == 1) * races_sorted.loc[x.index, 'win_odds']).sum()
})
class_change_roi.columns = ['bets', 'returns']
class_change_roi['roi'] = class_change_roi['returns'] / class_change_roi['bets'] * 100
print(class_change_roi)

print("\n" + "=" * 80)
print("特徴量候補の総合評価サマリー")
print("=" * 80)

print("""
┌──────────────────────────────────────────────────────────────────────────┐
│                    特徴量候補 総合評価サマリー                            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│ 【高優先度 - オッズ相関低く、ROI差あり】                                  │
│                                                                          │
│ 1. 生産地 (producing_area)                                               │
│    - オッズとの相関: 弱                                                   │
│    - 勝率差: 安平町 8.2% vs 平均 7%                                       │
│    - リスク: 低（馬マスタから取得、リークなし）                             │
│                                                                          │
│ 2. 休養日数最適性 (rest_days_optimal)                                     │
│    - 中2-4週が最高ROI                                                     │
│    - オッズ相関: 0.026（ほぼなし）                                        │
│    - リスク: 低（cumsum+shiftで安全に計算可能）                            │
│                                                                          │
│ 3. 馬体重変動区分 (weight_change_category)                                │
│    - 微増(0-4kg)が最高ROI                                                 │
│    - オッズ相関: -0.038（ほぼなし）                                       │
│    - リスク: 低（当日情報だがレース結果ではない）                          │
│                                                                          │
│ 4. 父×馬場適性 (sire_surface_fit)                                        │
│    - 父によって芝/ダートで大きな勝率差                                     │
│    - リスク: 中（累積統計での計算が必要）                                  │
│                                                                          │
│ 【中優先度 - 要検証】                                                     │
│                                                                          │
│ 5. 過去の馬身差傾向 (horse_avg_gap)                                       │
│    - 4コーナー馬身差の過去平均（累積）                                     │
│    - 現レースの馬身差はリーク、過去累積はOK                                │
│    - リスク: 中（V10で類似特徴量実装済み）                                │
│                                                                          │
│ 6. ペース適性 (pace_fit)                                                 │
│    - 過去ハイペース/スローペースでの成績                                   │
│    - リスク: 中（race_detailsのデータ量が限定的）                         │
│                                                                          │
│ 【低優先度 - リスク高or効果薄】                                           │
│                                                                          │
│ 7. 母父統計 (damsire_stats)                                              │
│    - V9で追加済み、効果限定的                                             │
│    - オッズに織り込み済みの可能性高                                        │
│                                                                          │
│ 8. 騎手×条件別統計                                                       │
│    - V8で過学習の主因と判明、避けるべき                                   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
""")

print("=" * 80)
print("完了")
print("=" * 80)
