"""
Walk-Forward検証（修正版）: 発見した特徴量が「たまたま」ではないことを確認
- 複数期間でのROI安定性を検証
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = 'keibaai/data/parsed/parquet'

races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')

# 日付変換
races['race_date'] = pd.to_datetime(races['race_date'])

# データ準備
races_with_horses = races.merge(horses[['horse_id', 'producing_area']], on='horse_id', how='left')

# 父を取得
sires = pedigrees[pedigrees['generation'] == 1][['horse_id', 'ancestor_name']].rename(columns={'ancestor_name': 'sire_name'})
races_with_sire = races_with_horses.merge(sires, on='horse_id', how='left')

# 休養日数計算
races_with_sire = races_with_sire.sort_values(['horse_id', 'race_date'])
races_with_sire['days_since_last'] = races_with_sire.groupby('horse_id')['race_date'].diff().dt.days

print("=" * 80)
print("Walk-Forward検証: 特徴量の「たまたま」ではない安定性を確認")
print("=" * 80)

# 期間定義
periods = [
    ('2020-01-01', '2020-12-31', '2020年'),
    ('2021-01-01', '2021-12-31', '2021年'),
    ('2022-01-01', '2022-12-31', '2022年'),
    ('2023-01-01', '2023-12-31', '2023年'),
    ('2024-01-01', '2024-12-31', '2024年'),
]

def calc_roi_simple(df):
    """単純なROI計算"""
    if len(df) == 0:
        return np.nan
    bets = len(df)
    wins_mask = df['finish_position'] == 1
    returns = df.loc[wins_mask, 'win_odds'].sum()
    return returns / bets * 100

print("\n" + "=" * 80)
print("1. 生産地別 ROI推移（上位5地域）")
print("=" * 80)

top_areas = ['安平町', '千歳市', '浦河町', '日高町', '新ひだか町']
for area in top_areas:
    print(f"\n【{area}】")
    results = []
    for start, end, label in periods:
        period_df = races_with_horses[(races_with_horses['race_date'] >= start) & (races_with_horses['race_date'] <= end)]
        area_df = period_df[period_df['producing_area'] == area]
        
        roi_area = calc_roi_simple(area_df)
        roi_all = calc_roi_simple(period_df)
        
        results.append({
            'period': label,
            'bets': len(area_df),
            'roi': round(roi_area, 1),
            'vs_avg': round(roi_area - roi_all, 1) if not np.isnan(roi_area) else np.nan
        })
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    positive_count = (df_results['vs_avg'] > 0).sum()
    print(f"  → 一貫性: {positive_count}/5 期間で平均を上回る")

print("\n" + "=" * 80)
print("2. 休養日数「中4-8週」vs その他 ROI推移")
print("=" * 80)
results = []
for start, end, label in periods:
    period_df = races_with_sire[(races_with_sire['race_date'] >= start) & (races_with_sire['race_date'] <= end)].copy()
    
    optimal = period_df[(period_df['days_since_last'] >= 28) & (period_df['days_since_last'] <= 60)]
    non_optimal = period_df[(period_df['days_since_last'] < 28) | (period_df['days_since_last'] > 60)]
    
    roi_optimal = calc_roi_simple(optimal)
    roi_non = calc_roi_simple(non_optimal)
    
    results.append({
        'period': label,
        'optimal_bets': len(optimal),
        'optimal_roi': round(roi_optimal, 1),
        'non_optimal_roi': round(roi_non, 1),
        'diff': round(roi_optimal - roi_non, 1) if not (np.isnan(roi_optimal) or np.isnan(roi_non)) else np.nan
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
positive_count = (df_results['diff'] > 0).sum()
print(f"\n一貫性: {positive_count}/5 期間で最適休養がROI優位")

print("\n" + "=" * 80)
print("3. 「5歳以上セン馬」vs その他 ROI推移")
print("=" * 80)
results = []
for start, end, label in periods:
    period_df = races_with_horses[(races_with_horses['race_date'] >= start) & (races_with_horses['race_date'] <= end)].copy()
    
    gelding = period_df[(period_df['age'] >= 5) & (period_df['sex'] == 'セ')]
    others = period_df[~((period_df['age'] >= 5) & (period_df['sex'] == 'セ'))]
    
    roi_gelding = calc_roi_simple(gelding)
    roi_others = calc_roi_simple(others)
    
    results.append({
        'period': label,
        'gelding_bets': len(gelding),
        'gelding_roi': round(roi_gelding, 1),
        'others_roi': round(roi_others, 1),
        'diff': round(roi_gelding - roi_others, 1) if not (np.isnan(roi_gelding) or np.isnan(roi_others)) else np.nan
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
positive_count = (df_results['diff'] > 0).sum()
print(f"\n一貫性: {positive_count}/5 期間でセン馬がROI優位")

print("\n" + "=" * 80)
print("4. 父別 芝 vs ダート ROI推移（ディープインパクト）")
print("=" * 80)
results = []
for start, end, label in periods:
    period_df = races_with_sire[(races_with_sire['race_date'] >= start) & (races_with_sire['race_date'] <= end)].copy()
    
    deep_turf = period_df[(period_df['sire_name'] == 'ディープインパクト') & (period_df['track_surface'] == '芝')]
    deep_dirt = period_df[(period_df['sire_name'] == 'ディープインパクト') & (period_df['track_surface'] == 'ダート')]
    
    roi_turf = calc_roi_simple(deep_turf)
    roi_dirt = calc_roi_simple(deep_dirt)
    
    results.append({
        'period': label,
        'turf_bets': len(deep_turf),
        'turf_roi': round(roi_turf, 1) if not np.isnan(roi_turf) else np.nan,
        'dirt_bets': len(deep_dirt),
        'dirt_roi': round(roi_dirt, 1) if not np.isnan(roi_dirt) else np.nan,
        'diff': round(roi_turf - roi_dirt, 1) if not (np.isnan(roi_turf) or np.isnan(roi_dirt)) else np.nan
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
valid_periods = df_results['diff'].notna().sum()
positive_count = (df_results['diff'] > 0).sum()
print(f"\n一貫性: {positive_count}/{valid_periods} 期間で芝がダートを上回る")

print("\n" + "=" * 80)
print("5. 馬場状態「不良」vs「良」 ROI推移")
print("=" * 80)
results = []
for start, end, label in periods:
    period_df = races[(races['race_date'] >= start) & (races['race_date'] <= end)].copy()
    
    heavy = period_df[period_df['track_condition'] == '不良']
    good = period_df[period_df['track_condition'] == '良']
    
    roi_heavy = calc_roi_simple(heavy)
    roi_good = calc_roi_simple(good)
    
    results.append({
        'period': label,
        'heavy_bets': len(heavy),
        'heavy_roi': round(roi_heavy, 1),
        'good_bets': len(good),
        'good_roi': round(roi_good, 1),
        'diff': round(roi_heavy - roi_good, 1) if not (np.isnan(roi_heavy) or np.isnan(roi_good)) else np.nan
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
positive_count = (df_results['diff'] > 0).sum()
print(f"\n一貫性: {positive_count}/5 期間で不良馬場がROI優位")

print("\n" + "=" * 80)
print("6. 馬体重変動「微増(0-4kg)」vs その他 ROI推移")
print("=" * 80)
results = []
for start, end, label in periods:
    period_df = races[(races['race_date'] >= start) & (races['race_date'] <= end)].copy()
    
    optimal = period_df[(period_df['horse_weight_change'] >= 0) & (period_df['horse_weight_change'] <= 4)]
    others = period_df[~((period_df['horse_weight_change'] >= 0) & (period_df['horse_weight_change'] <= 4))]
    
    roi_optimal = calc_roi_simple(optimal)
    roi_others = calc_roi_simple(others)
    
    results.append({
        'period': label,
        'optimal_bets': len(optimal),
        'optimal_roi': round(roi_optimal, 1),
        'others_roi': round(roi_others, 1),
        'diff': round(roi_optimal - roi_others, 1)
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
positive_count = (df_results['diff'] > 0).sum()
print(f"\n一貫性: {positive_count}/5 期間で微増がROI優位")

print("\n" + "=" * 80)
print("総合判定サマリー")
print("=" * 80)
print("""
┌───────────────────────────────────────────────────────────────────────────────┐
│                   Walk-Forward検証 総合結果サマリー                            │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│ 特徴量候補             一貫性(5年中) 判定    実装優先度                        │
│ ───────────────────────────────────────────────────────────────               │
│ 生産地（上位5地域）    変動あり      △       低（一貫性なし）                  │
│ 休養日数（中4-8週）    ?/5          要確認   中（ベット数多い）                 │
│ 5歳以上セン馬          ?/5          要確認   中（データ量少なめ）               │
│ 父×馬場適性            ?/5          要確認   中（ディープ以外も検証必要）       │
│ 重馬場（不良）         ?/5          要確認   低（馬場によりばらつき）           │
│ 馬体重変動（微増）     ?/5          要確認   高（当日情報でリークなし）         │
│                                                                               │
│ ※一貫性4/5以上 = 高（たまたまではない）                                       │
│ ※一貫性3/5 = 中（要検証）                                                     │
│ ※一貫性2/5以下 = 低（たまたまの可能性）                                       │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
""")

print("=" * 80)
print("完了")
print("=" * 80)
