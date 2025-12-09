"""
多角的データ探索スクリプト
- 各Parquetファイルのカラム別統計
- 欠損率、データ型、分布
- 勝者と非勝者の分布差分析
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = 'keibaai/data/parsed/parquet'

print("=" * 80)
print("1. horses.parquet - 馬マスタ分析")
print("=" * 80)
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
print(f"レコード数: {len(horses):,}")
print(f"\nカラム一覧:")
for col in horses.columns:
    non_null = horses[col].notna().sum()
    pct = non_null / len(horses) * 100
    print(f"  {col}: {pct:.1f}% 非null")
print(f"\nbreeder_name ユニーク数: {horses['breeder_name'].nunique()}")
print(f"producing_area ユニーク数: {horses['producing_area'].nunique()}")
print(f"\n【producing_area 上位10】")
print(horses['producing_area'].value_counts().head(10))

print("\n" + "=" * 80)
print("2. shutuba.parquet - 出馬表分析")
print("=" * 80)
shutuba = pd.read_parquet(f'{DATA_DIR}/shutuba/shutuba.parquet')
print(f"レコード数: {len(shutuba):,}")
print(f"\n【重要カラムの非null率】")
important_cols = ['career_stats', 'career_starts', 'career_wins', 'career_places', 
                  'last_5_finishes', 'morning_odds', 'morning_popularity', 'prize_total']
for col in important_cols:
    if col in shutuba.columns:
        non_null = shutuba[col].notna().sum()
        pct = non_null / len(shutuba) * 100
        print(f"  {col}: {pct:.1f}% 非null ({non_null:,} / {len(shutuba):,})")
    else:
        print(f"  {col}: カラム存在せず")

# career_statsの中身確認
if 'career_stats' in shutuba.columns:
    sample = shutuba[shutuba['career_stats'].notna()]['career_stats'].head(3)
    print(f"\n【career_stats サンプル】")
    for i, s in enumerate(sample):
        print(f"  {i}: {s}")

# last_5_finishesの中身確認
if 'last_5_finishes' in shutuba.columns:
    sample = shutuba[shutuba['last_5_finishes'].notna()]['last_5_finishes'].head(3)
    print(f"\n【last_5_finishes サンプル】")
    for i, s in enumerate(sample):
        print(f"  {i}: {s}")

# morning_oddsの分布
if 'morning_odds' in shutuba.columns and shutuba['morning_odds'].notna().sum() > 0:
    print(f"\n【morning_odds 統計】")
    print(shutuba['morning_odds'].describe())

print("\n" + "=" * 80)
print("3. corner_positions.parquet - コーナー位置分析")
print("=" * 80)
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')
print(f"レコード数: {len(corners):,}")
print(f"\n【カラム別統計】")
for col in corners.columns:
    print(f"  {col}:")
    if corners[col].dtype in ['int64', 'float64', 'Int64', 'Float64']:
        print(f"    mean={corners[col].mean():.2f}, std={corners[col].std():.2f}, min={corners[col].min()}, max={corners[col].max()}")
    else:
        print(f"    ユニーク数={corners[col].nunique()}")

# コーナー別の統計
print(f"\n【コーナー別レコード数】")
print(corners['corner'].value_counts().sort_index())

# gap_from_leaderの分布
print(f"\n【gap_from_leader 分布】")
print(f"  0 (先頭): {(corners['gap_from_leader'] == 0).sum():,}")
print(f"  0-3馬身: {((corners['gap_from_leader'] > 0) & (corners['gap_from_leader'] <= 3)).sum():,}")
print(f"  3-7馬身: {((corners['gap_from_leader'] > 3) & (corners['gap_from_leader'] <= 7)).sum():,}")
print(f"  7馬身超: {(corners['gap_from_leader'] > 7).sum():,}")

print("\n" + "=" * 80)
print("4. race_details.parquet - レース詳細分析")
print("=" * 80)
race_details = pd.read_parquet(f'{DATA_DIR}/race_details/race_details.parquet')
print(f"レコード数: {len(race_details):,}")
print(f"\n【カラム別非null率】")
for col in race_details.columns:
    non_null = race_details[col].notna().sum()
    pct = non_null / len(race_details) * 100
    print(f"  {col}: {pct:.1f}%")

# first_half / second_half の統計
if 'first_half' in race_details.columns:
    print(f"\n【first_half (前半3F)】")
    print(race_details['first_half'].describe())
    
if 'second_half' in race_details.columns:
    print(f"\n【second_half (後半3F)】")
    print(race_details['second_half'].describe())

# ペース傾向計算
if 'first_half' in race_details.columns and 'second_half' in race_details.columns:
    race_details['pace_diff'] = race_details['first_half'] - race_details['second_half']
    print(f"\n【ペース傾向 (前半-後半)】")
    print(f"  スロー(前半>後半): {(race_details['pace_diff'] > 0).sum():,} レース")
    print(f"  ハイ(前半<後半): {(race_details['pace_diff'] < 0).sum():,} レース")
    print(f"  平均: {race_details['pace_diff'].mean():.2f}秒")

print("\n" + "=" * 80)
print("5. returns.parquet - 払戻し分析")
print("=" * 80)
returns = pd.read_parquet(f'{DATA_DIR}/returns/returns.parquet')
print(f"レコード数: {len(returns):,}")
print(f"\n【bet_type別レコード数】")
print(returns['bet_type'].value_counts())

# 単勝の払戻額分布
tansho = returns[returns['bet_type'] == 'tansho']
print(f"\n【単勝 payout 統計】")
print(tansho['payout'].describe())
print(f"  1000円以下: {(tansho['payout'] <= 1000).sum():,}")
print(f"  1000-3000円: {((tansho['payout'] > 1000) & (tansho['payout'] <= 3000)).sum():,}")
print(f"  3000-10000円: {((tansho['payout'] > 3000) & (tansho['payout'] <= 10000)).sum():,}")
print(f"  10000円超: {(tansho['payout'] > 10000).sum():,}")

# 人気別払戻し
print(f"\n【人気別 単勝平均pay】")
pop_pay = tansho.groupby('popularity')['payout'].mean()
print(pop_pay.head(10))

print("\n" + "=" * 80)
print("6. pedigrees.parquet - 血統分析")
print("=" * 80)
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
print(f"レコード数: {len(pedigrees):,}")
print(f"\n【generation別レコード数】")
print(pedigrees['generation'].value_counts().sort_index())
print(f"\n【ancestor_name ユニーク数】: {pedigrees['ancestor_name'].nunique()}")

# 最頻出の祖先
print(f"\n【最頻出祖先 (generation=1, 父)】")
gen1 = pedigrees[pedigrees['generation'] == 1]
print(gen1['ancestor_name'].value_counts().head(10))

# 母父 (generation=2の2番目)
print(f"\n【generation=2 で最頻出（祖父母）】")
gen2 = pedigrees[pedigrees['generation'] == 2]
print(gen2['ancestor_name'].value_counts().head(10))

print("\n" + "=" * 80)
print("7. races.parquet - レース結果と勝者分析")
print("=" * 80)
races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
print(f"レコード数: {len(races):,}")

# 勝者と非勝者の分布差
winners = races[races['finish_position'] == 1].copy()
non_winners = races[races['finish_position'] > 1].copy()

print(f"\n勝者数: {len(winners):,}, 非勝者数: {len(non_winners):,}")

# 重要な数値カラムの比較
compare_cols = ['age', 'basis_weight', 'horse_weight', 'horse_weight_change']
print(f"\n【勝者 vs 非勝者 の平均比較】")
for col in compare_cols:
    if col in races.columns:
        w_mean = winners[col].mean()
        nw_mean = non_winners[col].mean()
        diff = w_mean - nw_mean
        print(f"  {col}: 勝者={w_mean:.2f}, 非勝者={nw_mean:.2f}, 差={diff:.2f}")

# last_3f_time の比較（過去の上がり3F）
if 'last_3f_time' in races.columns:
    print(f"\n【last_3f_time (上がり3F)】")
    print(f"  勝者平均: {winners['last_3f_time'].mean():.2f}秒")
    print(f"  非勝者平均: {non_winners['last_3f_time'].mean():.2f}秒")

# last3f_rank の比較
if 'last3f_rank' in races.columns:
    print(f"\n【last3f_rank (上がり順位)】")
    print(f"  勝者平均: {winners['last3f_rank'].mean():.2f}")
    print(f"  非勝者平均: {non_winners['last3f_rank'].mean():.2f}")
    print(f"  勝者のlast3f_rank=1の割合: {(winners['last3f_rank'] == 1).mean()*100:.1f}%")

# 4コーナー順位の比較
if 'passing_order_4' in races.columns:
    print(f"\n【passing_order_4 (4コーナー順位)】")
    print(f"  勝者平均: {winners['passing_order_4'].mean():.2f}")
    print(f"  非勝者平均: {non_winners['passing_order_4'].mean():.2f}")
    print(f"  勝者が4コーナー先頭の割合: {(winners['passing_order_4'] == 1).mean()*100:.1f}%")

# race_class の分析
if 'race_class' in races.columns:
    print(f"\n【race_class 別勝率】")
    class_stats = races.groupby('race_class').agg({
        'finish_position': ['count', lambda x: (x == 1).mean()]
    }).round(3)
    class_stats.columns = ['count', 'win_rate']
    print(class_stats.sort_values('count', ascending=False).head(10))

print("\n" + "=" * 80)
print("完了")
print("=" * 80)
