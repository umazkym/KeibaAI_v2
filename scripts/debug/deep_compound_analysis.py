"""
複合的データ問題の深層分析スクリプト

単独では問題なくても、組み合わせで問題になるケースを調査：
1. 馬ごとのデータ完全性（全期間を通じた一貫性）
2. レースID-馬番の対応関係の整合性
3. 時系列データの欠損パターン
4. 特徴量計算に影響する複合的な問題
5. horse_idとrace_idの参照整合性
6. 2014-2019と2020-2025でのデータ構造の違い
7. 血統データの完全性
8. コーナーデータと着順の関係
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def load_all_data():
    """全データを読み込み"""
    print("データ読み込み中...")
    data = {}
    
    data['races'] = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    data['races']['race_date'] = pd.to_datetime(data['races']['race_date'])
    data['races']['year'] = data['races']['race_date'].dt.year
    
    data['corners'] = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    data['corners']['year'] = data['corners']['race_id'].astype(str).str[:4].astype(int)
    
    data['race_details'] = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    data['race_details']['year'] = data['race_details']['race_id'].astype(str).str[:4].astype(int)
    
    data['returns'] = pd.read_parquet(DATA_DIR / "returns" / "returns.parquet")
    data['returns']['year'] = data['returns']['race_id'].astype(str).str[:4].astype(int)
    
    data['horses'] = pd.read_parquet(DATA_DIR / "horses" / "horses.parquet")
    
    data['pedigrees'] = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
    
    print(f"  races: {len(data['races']):,}件")
    print(f"  corners: {len(data['corners']):,}件")
    print(f"  race_details: {len(data['race_details']):,}件")
    print(f"  returns: {len(data['returns']):,}件")
    print(f"  horses: {len(data['horses']):,}件")
    print(f"  pedigrees: {len(data['pedigrees']):,}件")
    
    return data

def analyze_horse_data_completeness(data):
    """馬ごとのデータ完全性を分析"""
    print_section("1. 馬ごとのデータ完全性分析")
    
    races = data['races']
    corners = data['corners']
    horses = data['horses']
    pedigrees = data['pedigrees']
    
    # 全ユニーク馬
    all_horse_ids = set(races['horse_id'].dropna().unique())
    print(f"\n  races.parquetに登場する馬: {len(all_horse_ids):,}頭")
    
    # horses.parquetにある馬
    horses_in_master = set(horses['horse_id'].unique())
    print(f"  horses.parquetにある馬: {len(horses_in_master):,}頭")
    
    # マスターにない馬
    missing_in_master = all_horse_ids - horses_in_master
    if len(missing_in_master) > 0:
        print(f"  ⚠️ horses.parquetにない馬: {len(missing_in_master):,}頭")
        
        # 年別分布
        missing_races = races[races['horse_id'].isin(missing_in_master)]
        year_dist = missing_races.groupby('year')['horse_id'].nunique()
        print(f"  年別マスター欠損馬数:")
        for year, count in year_dist.items():
            print(f"    {year}: {count}頭")
    else:
        print(f"  ✓ 全馬がhorses.parquetに存在")
    
    # 血統データの完全性
    horses_with_pedigree = set(pedigrees['horse_id'].unique())
    missing_pedigree = all_horse_ids - horses_with_pedigree
    if len(missing_pedigree) > 0:
        print(f"\n  ⚠️ 血統データがない馬: {len(missing_pedigree):,}頭")
        missing_ped_races = races[races['horse_id'].isin(missing_pedigree)]
        year_dist = missing_ped_races.groupby('year')['horse_id'].nunique()
        print(f"  年別血統欠損馬数:")
        for year, count in year_dist.items():
            print(f"    {year}: {count}頭")
    else:
        print(f"  ✓ 全馬に血統データが存在")
    
    # 馬の出走回数の分布
    race_counts = races.groupby('horse_id').size()
    print(f"\n  馬の出走回数分布:")
    print(f"    1回のみ: {(race_counts == 1).sum():,}頭")
    print(f"    2-5回: {((race_counts >= 2) & (race_counts <= 5)).sum():,}頭")
    print(f"    6-10回: {((race_counts >= 6) & (race_counts <= 10)).sum():,}頭")
    print(f"    11回以上: {(race_counts > 10).sum():,}頭")

def analyze_cross_file_consistency(data):
    """ファイル間の整合性を深く分析"""
    print_section("2. ファイル間の整合性分析（詳細）")
    
    races = data['races']
    corners = data['corners']
    race_details = data['race_details']
    returns = data['returns']
    
    # race_id x horse_numberの組み合わせ確認
    races_pairs = set(zip(races['race_id'], races['horse_number']))
    corners_pairs = set(zip(corners['race_id'], corners['horse_number']))
    
    print(f"\n  races.parquet: {len(races_pairs):,} (race_id, horse_number)ペア")
    
    # cornerで参照されているがracesにないペア
    orphan_corners = corners_pairs - races_pairs
    if len(orphan_corners) > 0:
        print(f"  ⚠️ cornersにあってracesにないペア: {len(orphan_corners):,}")
        sample = list(orphan_corners)[:5]
        for rid, hn in sample:
            print(f"      {rid}, 馬番{hn}")
    else:
        print(f"  ✓ corners → races: 参照整合性OK")
    
    # racesにあってcornersにないペア
    c4_corners = corners[corners['corner'] == 4]
    c4_pairs = set(zip(c4_corners['race_id'], c4_corners['horse_number']))
    missing_c4 = races_pairs - c4_pairs
    
    # レース単位でのC4欠損を調査
    c4_race_ids = set(c4_corners['race_id'].unique())
    all_race_ids = set(races['race_id'].unique())
    missing_c4_races = all_race_ids - c4_race_ids
    
    print(f"\n  C4データがないレース: {len(missing_c4_races):,}件")
    
    if len(missing_c4_races) > 0:
        # 欠損レースの特徴
        missing_race_data = races[races['race_id'].isin(missing_c4_races)]
        
        # 距離別
        print(f"  C4欠損レースの距離分布（上位5）:")
        dist_counts = missing_race_data.groupby('distance_m')['race_id'].nunique().sort_values(ascending=False).head()
        for dist, count in dist_counts.items():
            print(f"    {dist}m: {count}レース")
        
        # 年別
        print(f"\n  C4欠損レースの年別分布:")
        year_counts = missing_race_data.groupby('year')['race_id'].nunique()
        for year, count in year_counts.items():
            print(f"    {year}: {count}レース")
    
    # returns と races の整合性
    returns_race_ids = set(returns['race_id'].unique())
    races_race_ids = set(races['race_id'].unique())
    
    missing_returns = races_race_ids - returns_race_ids
    if len(missing_returns) > 0:
        print(f"\n  ⚠️ returnsにないレース: {len(missing_returns):,}件")
        missing_ret_data = races[races['race_id'].isin(missing_returns)]
        year_counts = missing_ret_data.groupby('year')['race_id'].nunique()
        print(f"  年別分布:")
        for year, count in year_counts.items():
            print(f"    {year}: {count}レース")
    else:
        print(f"  ✓ races → returns: 参照整合性OK")

def analyze_temporal_patterns(data):
    """時系列パターンの分析"""
    print_section("3. 時系列パターン分析")
    
    races = data['races']
    
    # 月別レース数の推移
    races['month'] = races['race_date'].dt.to_period('M')
    monthly_counts = races.groupby('month').size()
    
    # 極端に少ない月を検出
    mean_count = monthly_counts.mean()
    std_count = monthly_counts.std()
    anomaly_months = monthly_counts[monthly_counts < mean_count - 2*std_count]
    
    if len(anomaly_months) > 0:
        print(f"\n  ⚠️ レース数が極端に少ない月（平均-2σ以下）:")
        for month, count in anomaly_months.items():
            print(f"    {month}: {count}件（平均: {mean_count:.0f}）")
    else:
        print(f"  ✓ 月別レース数に極端な欠損なし")
    
    # 年月別の着順NULL率
    print(f"\n  年月別 finish_position NULL率の推移:")
    races['ym'] = races['race_date'].dt.to_period('M').astype(str)
    null_rate_by_month = races.groupby('ym')['finish_position'].apply(
        lambda x: x.isnull().mean() * 100
    )
    
    # 高いNULL率の月
    high_null = null_rate_by_month[null_rate_by_month > 2]
    if len(high_null) > 0:
        print(f"  ⚠️ NULL率2%超の月:")
        for ym, rate in high_null.head(10).items():
            print(f"    {ym}: {rate:.2f}%")
    else:
        print(f"  ✓ 全月でNULL率2%以下")

def analyze_feature_calculation_issues(data):
    """特徴量計算に影響する問題を分析"""
    print_section("4. 特徴量計算に影響する問題分析")
    
    races = data['races']
    corners = data['corners']
    
    # 馬ごとのC4データ保有率
    c4 = corners[corners['corner'] == 4]
    
    # racesとマージ
    c4_with_id = c4.merge(
        races[['race_id', 'horse_number', 'horse_id', 'race_date', 'year']].drop_duplicates(),
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 馬ごとのC4レース数
    horse_c4_counts = c4_with_id.groupby('horse_id').size()
    horse_total_races = races.groupby('horse_id').size()
    
    # C4データ保有率
    horse_c4_rate = horse_c4_counts / horse_total_races
    horse_c4_rate = horse_c4_rate.dropna()
    
    print(f"\n  馬ごとのC4データ保有率:")
    print(f"    100%: {(horse_c4_rate == 1.0).sum():,}頭")
    print(f"    90-99%: {((horse_c4_rate >= 0.9) & (horse_c4_rate < 1.0)).sum():,}頭")
    print(f"    50-89%: {((horse_c4_rate >= 0.5) & (horse_c4_rate < 0.9)).sum():,}頭")
    print(f"    50%未満: {(horse_c4_rate < 0.5).sum():,}頭")
    
    # C4保有率が低い馬の分析
    low_c4_horses = horse_c4_rate[horse_c4_rate < 0.5].index
    if len(low_c4_horses) > 0:
        print(f"\n  C4保有率50%未満の馬の特徴:")
        low_c4_races = races[races['horse_id'].isin(low_c4_horses)]
        
        # 距離別
        dist_counts = low_c4_races['distance_m'].value_counts().head()
        print(f"    よく走る距離（上位5）:")
        for dist, count in dist_counts.items():
            print(f"      {dist}m: {count}件")
    
    # 同一馬の年をまたいだデータ連続性
    print(f"\n  馬の年またぎデータ連続性:")
    
    horse_years = races.groupby('horse_id')['year'].agg(['min', 'max', 'nunique'])
    horse_years['span'] = horse_years['max'] - horse_years['min'] + 1
    horse_years['gap'] = horse_years['span'] - horse_years['nunique']
    
    gapped_horses = horse_years[horse_years['gap'] > 0]
    print(f"    年が飛んでいる馬: {len(gapped_horses):,}頭")
    
    if len(gapped_horses) > 0:
        # サンプル
        sample = gapped_horses.head(5)
        print(f"    サンプル:")
        for horse_id, row in sample.iterrows():
            years = sorted(races[races['horse_id'] == horse_id]['year'].unique())
            print(f"      {horse_id}: {years}")

def analyze_odds_and_finish_consistency(data):
    """オッズと着順の一貫性分析"""
    print_section("5. オッズと着順の一貫性分析")
    
    races = data['races']
    
    # 1着馬のオッズ分布
    winners = races[races['finish_position'] == 1]
    
    print(f"\n  1着馬のオッズ分布（年別）:")
    print(f"  {'年':<6} {'平均':>10} {'中央値':>10} {'最大':>10} {'100倍超':>10}")
    print("-" * 55)
    
    for year in sorted(winners['year'].unique()):
        year_winners = winners[winners['year'] == year]
        mean_odds = year_winners['win_odds'].mean()
        median_odds = year_winners['win_odds'].median()
        max_odds = year_winners['win_odds'].max()
        over_100 = (year_winners['win_odds'] > 100).sum()
        print(f"  {year:<6} {mean_odds:>10.1f} {median_odds:>10.1f} {max_odds:>10.1f} {over_100:>10}")
    
    # 人気1位が勝つ率
    print(f"\n  人気1位の勝率（年別）:")
    fav_wins = races[races['popularity'] == 1]
    for year in sorted(fav_wins['year'].unique()):
        year_fav = fav_wins[fav_wins['year'] == year]
        win_rate = (year_fav['finish_position'] == 1).mean() * 100
        print(f"    {year}: {win_rate:.1f}%")
    
    # 異常なオッズ
    abnormal_odds = races[races['win_odds'] > 500]
    if len(abnormal_odds) > 0:
        print(f"\n  オッズ500倍超のレコード: {len(abnormal_odds):,}件")
        # 1着になったケース
        high_odds_winners = abnormal_odds[abnormal_odds['finish_position'] == 1]
        print(f"    そのうち1着: {len(high_odds_winners):,}件")

def analyze_jockey_trainer_consistency(data):
    """騎手・調教師データの一貫性分析"""
    print_section("6. 騎手・調教師データの一貫性")
    
    races = data['races']
    
    # 騎手IDの欠損
    jockey_null = races['jockey_id'].isnull().sum()
    trainer_null = races['trainer_id'].isnull().sum()
    
    print(f"\n  jockey_id NULL: {jockey_null:,}件 ({jockey_null/len(races)*100:.2f}%)")
    print(f"  trainer_id NULL: {trainer_null:,}件 ({trainer_null/len(races)*100:.2f}%)")
    
    # 年別の騎手数
    print(f"\n  年別アクティブ騎手数:")
    jockey_counts = races.groupby('year')['jockey_id'].nunique()
    for year, count in jockey_counts.items():
        print(f"    {year}: {count}名")
    
    # 同一騎手×馬の組み合わせ数
    jh_pairs = races.groupby(['jockey_id', 'horse_id']).size()
    print(f"\n  騎手×馬の組み合わせ:")
    print(f"    総組み合わせ数: {len(jh_pairs):,}")
    print(f"    1回のみ: {(jh_pairs == 1).sum():,}")
    print(f"    2回以上: {(jh_pairs >= 2).sum():,}")

def analyze_pedigree_completeness(data):
    """血統データの詳細分析"""
    print_section("7. 血統データの詳細分析")
    
    pedigrees = data['pedigrees']
    races = data['races']
    
    # 世代別レコード数
    print(f"\n  世代別レコード数:")
    gen_counts = pedigrees.groupby('generation').size()
    for gen, count in gen_counts.items():
        print(f"    世代{gen}: {count:,}件")
    
    # 馬ごとの血統レコード数
    horse_ped_counts = pedigrees.groupby('horse_id').size()
    
    print(f"\n  馬ごとの血統レコード数:")
    expected_62 = (horse_ped_counts == 62).sum()  # 5世代で62先祖
    less_than_62 = (horse_ped_counts < 62).sum()
    
    print(f"    62件（完全）: {expected_62:,}頭")
    print(f"    62件未満（不完全）: {less_than_62:,}頭")
    
    # 不完全な血統を持つ馬のレース数
    incomplete_horses = horse_ped_counts[horse_ped_counts < 62].index
    incomplete_races = races[races['horse_id'].isin(incomplete_horses)]
    
    print(f"\n  不完全血統馬のレース数: {len(incomplete_races):,}件")
    print(f"    全レースに占める割合: {len(incomplete_races)/len(races)*100:.2f}%")
    
    # 年別分布
    if len(incomplete_races) > 0:
        print(f"  年別分布:")
        year_dist = incomplete_races.groupby('year').size()
        for year, count in year_dist.items():
            print(f"    {year}: {count}件")

def analyze_data_drift(data):
    """データドリフト分析（2014-2019 vs 2020-2025）"""
    print_section("8. データドリフト分析")
    
    races = data['races']
    
    old = races[races['year'] < 2020]
    new = races[races['year'] >= 2020]
    
    # カテゴリカル変数の分布比較
    print(f"\n  カテゴリカル変数の分布比較:")
    
    for col in ['track_surface', 'track_condition', 'venue']:
        if col in races.columns:
            old_dist = old[col].value_counts(normalize=True) * 100
            new_dist = new[col].value_counts(normalize=True) * 100
            
            print(f"\n  【{col}】")
            all_vals = set(old_dist.index) | set(new_dist.index)
            for val in sorted(all_vals, key=str):
                old_pct = old_dist.get(val, 0)
                new_pct = new_dist.get(val, 0)
                diff = new_pct - old_pct
                flag = "⚠️" if abs(diff) > 5 else ""
                print(f"    {val}: {old_pct:.1f}% → {new_pct:.1f}% ({diff:+.1f}%) {flag}")
    
    # 数値変数の分布比較
    print(f"\n  数値変数の分布比較:")
    
    for col in ['distance_m', 'basis_weight', 'horse_weight', 'age']:
        if col in races.columns:
            old_mean = old[col].mean()
            new_mean = new[col].mean()
            old_std = old[col].std()
            new_std = new[col].std()
            
            diff_mean = (new_mean - old_mean) / old_mean * 100 if old_mean != 0 else 0
            diff_std = (new_std - old_std) / old_std * 100 if old_std != 0 else 0
            
            flag = "⚠️" if abs(diff_mean) > 5 else ""
            print(f"\n  【{col}】")
            print(f"    平均: {old_mean:.2f} → {new_mean:.2f} ({diff_mean:+.1f}%) {flag}")
            print(f"    標準偏差: {old_std:.2f} → {new_std:.2f} ({diff_std:+.1f}%)")

def analyze_complex_feature_dependencies(data):
    """複雑な特徴量依存関係の分析"""
    print_section("9. 特徴量依存関係の問題分析")
    
    races = data['races']
    corners = data['corners']
    race_details = data['race_details']
    
    # V15で使用される特徴量の依存関係を確認
    
    # 1. horse_c4_gap_avg の計算に必要なデータ
    print(f"\n  【horse_c4_gap_avgの計算依存関係】")
    
    # corners.parquetのcorner=4データ
    c4 = corners[corners['corner'] == 4]
    print(f"    corners(corner=4)レコード数: {len(c4):,}")
    
    # races.parquetとのマージ
    c4_merged = c4.merge(
        races[['race_id', 'horse_number', 'horse_id']].drop_duplicates(),
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # horse_idがNULLのケース
    null_horse_id = c4_merged['horse_id'].isnull().sum()
    print(f"    マージ後horse_id NULL: {null_horse_id:,}件")
    
    if null_horse_id > 0:
        print(f"    ⚠️ cornersにあるがracesにないペアがある")
        orphan = c4_merged[c4_merged['horse_id'].isnull()]
        sample = orphan[['race_id', 'horse_number']].head()
        print(f"    サンプル:")
        print(sample.to_string(index=False))
    
    # 2. venue_expected_pace の計算に必要なデータ
    print(f"\n  【venue_expected_paceの計算依存関係】")
    
    # race_detailsのfirst_half, second_half
    pace_null = race_details[['first_half', 'second_half']].isnull().any(axis=1).sum()
    print(f"    race_detailsでペースデータNULL: {pace_null:,}件")
    
    if pace_null > 0:
        null_pace = race_details[race_details[['first_half', 'second_half']].isnull().any(axis=1)]
        year_dist = null_pace.groupby('year').size()
        print(f"    年別分布:")
        for year, count in year_dist.items():
            print(f"      {year}: {count}件")
    
    # 3. front_runner_rate の計算に必要なデータ
    print(f"\n  【front_runner_rateの計算依存関係】")
    
    # C4 position <= 2 の馬
    front_runners = c4[c4['position'] <= 2]
    print(f"    C4位置1-2位のレコード数: {len(front_runners):,}")
    
    # 馬ごとのカウント
    fr_by_horse = front_runners.merge(
        races[['race_id', 'horse_number', 'horse_id']].drop_duplicates(),
        on=['race_id', 'horse_number'],
        how='left'
    ).groupby('horse_id').size()
    
    print(f"    逃げ経験のある馬: {len(fr_by_horse):,}頭")

def analyze_potential_leakage(data):
    """潜在的なリーク源の分析"""
    print_section("10. 潜在的なリーク源の分析")
    
    races = data['races']
    
    # 同一race_idで複数日付があるかチェック
    race_dates = races.groupby('race_id')['race_date'].nunique()
    multi_date_races = race_dates[race_dates > 1]
    
    if len(multi_date_races) > 0:
        print(f"\n  ⚠️ 複数日付を持つrace_id: {len(multi_date_races)}件")
        for rid in multi_date_races.head().index:
            dates = races[races['race_id'] == rid]['race_date'].unique()
            print(f"    {rid}: {dates}")
    else:
        print(f"\n  ✓ 全race_idが単一日付")
    
    # 同一馬が同日同レースに複数回登場するかチェック
    dup_entries = races.groupby(['race_id', 'horse_id']).size()
    multi_entries = dup_entries[dup_entries > 1]
    
    if len(multi_entries) > 0:
        print(f"\n  ⚠️ 同一レース同一馬が複数回登場: {len(multi_entries)}件")
        for (rid, hid), count in multi_entries.head().items():
            print(f"    {rid}, {hid}: {count}回")
    else:
        print(f"\n  ✓ 同一レース同一馬の重複なし")
    
    # 着順の重複チェック（同一レース内）
    finish_dup = races.groupby(['race_id', 'finish_position']).size()
    multi_finish = finish_dup.reset_index(name='count')
    multi_finish = multi_finish[(multi_finish['count'] > 1) & (multi_finish['finish_position'].notna())]
    
    # 同着以外の重複
    non_dead_heat = multi_finish[multi_finish['count'] > 2]
    if len(non_dead_heat) > 0:
        print(f"\n  ⚠️ 3頭以上同着: {len(non_dead_heat)}件")
    else:
        print(f"\n  ✓ 異常な着順重複なし")

def main():
    print("=" * 80)
    print("  KeibaAI_v2 複合データ問題の深層分析")
    print("=" * 80)
    
    data = load_all_data()
    
    analyze_horse_data_completeness(data)
    analyze_cross_file_consistency(data)
    analyze_temporal_patterns(data)
    analyze_feature_calculation_issues(data)
    analyze_odds_and_finish_consistency(data)
    analyze_jockey_trainer_consistency(data)
    analyze_pedigree_completeness(data)
    analyze_data_drift(data)
    analyze_complex_feature_dependencies(data)
    analyze_potential_leakage(data)
    
    print("\n" + "=" * 80)
    print("  分析完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
