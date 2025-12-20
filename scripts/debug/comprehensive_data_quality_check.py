"""
包括的データ品質チェックスクリプト

2014-2019データ拡張後のParquetファイルの品質を詳細に検証する。
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# パス設定
DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def load_all_parquet():
    """全Parquetファイルを読み込み"""
    print("=" * 80)
    print("1. Parquetファイルの読み込み")
    print("=" * 80)
    
    files = {
        'races': DATA_DIR / 'races' / 'races.parquet',
        'horses': DATA_DIR / 'horses' / 'horses.parquet',
        'pedigrees': DATA_DIR / 'pedigrees' / 'pedigrees.parquet',
        'corners': DATA_DIR / 'corners' / 'corner_positions.parquet',
        'returns': DATA_DIR / 'returns' / 'returns.parquet',
        'race_details': DATA_DIR / 'race_details' / 'race_details.parquet',
        'shutuba': DATA_DIR / 'shutuba' / 'shutuba.parquet',
    }
    
    data = {}
    for name, path in files.items():
        if path.exists():
            df = pd.read_parquet(path)
            data[name] = df
            print(f"  ✓ {name}: {len(df):,} rows, {len(df.columns)} columns")
        else:
            print(f"  ✗ {name}: ファイルが存在しません - {path}")
    
    return data


def check_basic_statistics(data):
    """基本統計の確認"""
    print("\n" + "=" * 80)
    print("2. 基本統計の確認")
    print("=" * 80)
    
    # races.parquet
    if 'races' in data:
        df = data['races']
        print(f"\n【races.parquet】")
        print(f"  行数: {len(df):,}")
        print(f"  列数: {len(df.columns)}")
        print(f"  主要カラム:")
        
        # race_date の確認
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            print(f"    期間: {df['race_date'].min()} ~ {df['race_date'].max()}")
        
        # 欠損値率
        null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
        high_null = null_pct[null_pct > 5]
        if len(high_null) > 0:
            print(f"  欠損率5%超のカラム:")
            for col, pct in high_null.head(10).items():
                print(f"    - {col}: {pct:.1f}%")
        
        # ユニーク値
        print(f"  ユニークrace_id数: {df['race_id'].nunique():,}")
        print(f"  ユニークhorse_id数: {df['horse_id'].nunique():,}")
        
    # pedigrees.parquet
    if 'pedigrees' in data:
        df = data['pedigrees']
        print(f"\n【pedigrees.parquet】")
        print(f"  行数: {len(df):,}")
        print(f"  ユニーク馬数: {df['horse_id'].nunique():,}")
        
        # 馬あたりレコード数
        records_per_horse = df.groupby('horse_id').size()
        print(f"  馬あたりレコード数:")
        print(f"    平均: {records_per_horse.mean():.1f}")
        print(f"    最小: {records_per_horse.min()}")
        print(f"    最大: {records_per_horse.max()}")
        print(f"    中央値: {records_per_horse.median():.1f}")
        
        # 世代別カバレッジ
        if 'generation' in df.columns:
            gen_counts = df.groupby('generation').size()
            print(f"  世代別レコード数:")
            for gen, cnt in gen_counts.items():
                expected = df['horse_id'].nunique() * (2 ** gen)
                coverage = cnt / expected * 100 if expected > 0 else 0
                print(f"    Generation {gen}: {cnt:,} (理論値の{coverage:.1f}%)")
    
    # horses.parquet
    if 'horses' in data:
        df = data['horses']
        print(f"\n【horses.parquet】")
        print(f"  行数: {len(df):,}")
        null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
        high_null = null_pct[null_pct > 5]
        if len(high_null) > 0:
            print(f"  欠損率5%超のカラム:")
            for col, pct in high_null.head(10).items():
                print(f"    - {col}: {pct:.1f}%")
    
    # corners.parquet
    if 'corners' in data:
        df = data['corners']
        print(f"\n【corners.parquet】")
        print(f"  行数: {len(df):,}")
        print(f"  ユニークrace_id数: {df['race_id'].nunique():,}")
        if 'corner' in df.columns:
            corner_counts = df.groupby('corner').size()
            print(f"  コーナー別レコード数:")
            for corner, cnt in corner_counts.items():
                print(f"    Corner {corner}: {cnt:,}")
    
    # returns.parquet
    if 'returns' in data:
        df = data['returns']
        print(f"\n【returns.parquet】")
        print(f"  行数: {len(df):,}")
        if 'bet_type' in df.columns:
            bet_counts = df.groupby('bet_type').size().sort_values(ascending=False)
            print(f"  bet_type別レコード数:")
            for bet, cnt in bet_counts.items():
                print(f"    {bet}: {cnt:,}")


def check_temporal_consistency(data):
    """時系列一貫性の検証"""
    print("\n" + "=" * 80)
    print("3. 時系列一貫性の検証")
    print("=" * 80)
    
    if 'races' not in data:
        print("  racesデータがありません")
        return
    
    df = data['races'].copy()
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['year'] = df['race_date'].dt.year
    df['month'] = df['race_date'].dt.month
    
    # 年別レース数
    yearly_races = df.groupby('year')['race_id'].nunique()
    print(f"\n【年別ユニークレース数】")
    for year, cnt in yearly_races.items():
        indicator = "⚠️" if cnt < 2000 else "✓"
        print(f"  {year}: {cnt:,} レース {indicator}")
    
    # 年別出走記録数
    yearly_records = df.groupby('year').size()
    print(f"\n【年別出走記録数】")
    for year, cnt in yearly_records.items():
        print(f"  {year}: {cnt:,} 記録")
    
    # 2014-2019 vs 2020-2025 の比較
    old_data = df[df['year'] <= 2019]
    new_data = df[df['year'] >= 2020]
    
    print(f"\n【期間別統計比較】")
    print(f"  2014-2019: {len(old_data):,} 記録")
    print(f"  2020-2025: {len(new_data):,} 記録")
    
    # 欠損率の年代別比較
    print(f"\n【欠損率の年代別比較（主要カラム）】")
    key_cols = ['finish_time_seconds', 'last_3f_time', 'passing_order_1', 
                'passing_order_4', 'win_odds', 'horse_weight']
    
    for col in key_cols:
        if col in df.columns:
            old_null = old_data[col].isnull().mean() * 100
            new_null = new_data[col].isnull().mean() * 100
            diff = old_null - new_null
            indicator = "⚠️" if abs(diff) > 5 else ""
            print(f"  {col}:")
            print(f"    2014-2019: {old_null:.1f}%, 2020-2025: {new_null:.1f}% (差: {diff:+.1f}%) {indicator}")


def check_referential_integrity(data):
    """参照整合性の検証"""
    print("\n" + "=" * 80)
    print("4. 参照整合性の検証")
    print("=" * 80)
    
    # races.horse_id → horses.horse_id
    if 'races' in data and 'horses' in data:
        races_horses = set(data['races']['horse_id'].dropna().unique())
        horses_ids = set(data['horses']['horse_id'].dropna().unique())
        
        missing = races_horses - horses_ids
        print(f"\n【races → horses 参照整合性】")
        print(f"  racesに存在する馬数: {len(races_horses):,}")
        print(f"  horsesに存在する馬数: {len(horses_ids):,}")
        print(f"  horsesに存在しない馬数: {len(missing):,}")
        
        if len(missing) > 0 and len(missing) <= 10:
            print(f"  欠損馬ID: {list(missing)}")
        elif len(missing) > 10:
            print(f"  欠損馬ID（上位10件）: {list(missing)[:10]}")
    
    # races.horse_id → pedigrees.horse_id
    if 'races' in data and 'pedigrees' in data:
        races_horses = set(data['races']['horse_id'].dropna().unique())
        pedigrees_horses = set(data['pedigrees']['horse_id'].dropna().unique())
        
        missing = races_horses - pedigrees_horses
        print(f"\n【races → pedigrees 参照整合性】")
        print(f"  racesに存在する馬数: {len(races_horses):,}")
        print(f"  pedigreesに存在する馬数: {len(pedigrees_horses):,}")
        print(f"  血統データがない馬数: {len(missing):,}")
        
        if len(missing) > 0:
            # 年代別に分析
            races_df = data['races'].copy()
            races_df['race_date'] = pd.to_datetime(races_df['race_date'])
            races_df['year'] = races_df['race_date'].dt.year
            
            missing_by_year = races_df[races_df['horse_id'].isin(missing)].groupby('year').size()
            if len(missing_by_year) > 0:
                print(f"  血統欠損馬の年別出走数:")
                for year, cnt in missing_by_year.items():
                    print(f"    {year}: {cnt} 記録")
    
    # races.race_id → corners.race_id
    if 'races' in data and 'corners' in data:
        races_ids = set(data['races']['race_id'].unique())
        corners_races = set(data['corners']['race_id'].unique())
        
        missing = races_ids - corners_races
        print(f"\n【races → corners 参照整合性】")
        print(f"  racesに存在するレース数: {len(races_ids):,}")
        print(f"  cornersに存在するレース数: {len(corners_races):,}")
        print(f"  コーナーデータがないレース数: {len(missing):,} ({len(missing)/len(races_ids)*100:.1f}%)")


def check_anomalies(data):
    """異常値・外れ値の検出"""
    print("\n" + "=" * 80)
    print("5. 異常値・外れ値の検出")
    print("=" * 80)
    
    if 'races' not in data:
        return
    
    df = data['races'].copy()
    
    # finish_time_seconds
    if 'finish_time_seconds' in df.columns:
        valid = df['finish_time_seconds'].dropna()
        print(f"\n【finish_time_seconds】")
        print(f"  有効値数: {len(valid):,}")
        print(f"  統計: min={valid.min():.1f}秒, max={valid.max():.1f}秒, mean={valid.mean():.1f}秒")
        
        # 異常値（30秒未満 or 300秒超）
        anomaly = df[(df['finish_time_seconds'] < 30) | (df['finish_time_seconds'] > 300)]
        if len(anomaly) > 0:
            print(f"  ⚠️ 異常値（<30秒 or >300秒）: {len(anomaly)} 件")
    
    # distance_m
    if 'distance_m' in df.columns:
        valid = df['distance_m'].dropna()
        print(f"\n【distance_m】")
        print(f"  有効値数: {len(valid):,}")
        print(f"  NULL数: {df['distance_m'].isnull().sum():,}")
        print(f"  分布:")
        dist_counts = valid.value_counts().sort_index()
        for dist in [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2500, 3000, 3200, 3600]:
            if dist in dist_counts.index:
                print(f"    {dist}m: {dist_counts[dist]:,}")
        
        # 異常値（800m未満 or 4000m超）
        anomaly = df[(df['distance_m'] < 800) | (df['distance_m'] > 4500)]
        if len(anomaly) > 0:
            print(f"  ⚠️ 異常値（<800m or >4500m）: {len(anomaly)} 件")
            print(f"      異常距離値: {anomaly['distance_m'].unique()}")
    
    # win_odds
    if 'win_odds' in df.columns:
        valid = df['win_odds'].dropna()
        print(f"\n【win_odds】")
        print(f"  有効値数: {len(valid):,}")
        print(f"  統計: min={valid.min():.1f}, max={valid.max():.1f}, median={valid.median():.1f}")
        
        # 極端値（1.0未満 or 1000超）
        anomaly = df[(df['win_odds'] < 1.0) | (df['win_odds'] > 1000)]
        if len(anomaly) > 0:
            print(f"  ⚠️ 極端値（<1.0 or >1000）: {len(anomaly)} 件")
    
    # horse_weight
    if 'horse_weight' in df.columns:
        valid = df['horse_weight'].dropna()
        print(f"\n【horse_weight】")
        print(f"  有効値数: {len(valid):,}")
        print(f"  統計: min={valid.min():.0f}kg, max={valid.max():.0f}kg, mean={valid.mean():.0f}kg")
        
        # 範囲外（300kg未満 or 600kg超）
        anomaly = df[(df['horse_weight'] < 300) | (df['horse_weight'] > 650)]
        if len(anomaly) > 0:
            print(f"  ⚠️ 範囲外（<300kg or >650kg）: {len(anomaly)} 件")
    
    # track_surface
    if 'track_surface' in df.columns:
        print(f"\n【track_surface】")
        surface_counts = df['track_surface'].value_counts()
        for surface, cnt in surface_counts.items():
            print(f"  {surface}: {cnt:,} ({cnt/len(df)*100:.1f}%)")


def check_compound_issues(data):
    """複合問題の分析"""
    print("\n" + "=" * 80)
    print("6. 複合問題の分析")
    print("=" * 80)
    
    if 'races' not in data:
        return
    
    df = data['races'].copy()
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['year'] = df['race_date'].dt.year
    
    # 新潟直線コースの影響
    print(f"\n【新潟直線コースの影響】")
    if 'venue' in df.columns and 'distance_m' in df.columns:
        niigata = df[df['venue'] == '新潟']
        niigata_1000 = niigata[niigata['distance_m'] == 1000]
        
        print(f"  新潟レース総数: {niigata['race_id'].nunique():,}")
        print(f"  新潟1000m（直線コース候補）: {niigata_1000['race_id'].nunique():,}")
        
        # 直線コースは passing_order が全てNULL
        if 'passing_order_1' in df.columns:
            no_passing = niigata_1000[niigata_1000['passing_order_1'].isnull()]
            print(f"  passing_order_1がNULLの1000mレース: {no_passing['race_id'].nunique():,}")
            
            # これらの馬が他のレースに出走した数
            straight_horses = no_passing['horse_id'].unique()
            other_races = df[(df['horse_id'].isin(straight_horses)) & 
                            ~((df['venue'] == '新潟') & (df['distance_m'] == 1000))]
            print(f"  直線コース出走馬の通常レース出走数: {len(other_races):,}")
    
    # 障害レースの影響
    print(f"\n【障害レースの影響】")
    if 'track_surface' in df.columns:
        obstacle = df[df['track_surface'] == '障害']
        print(f"  障害レース記録数: {len(obstacle):,}")
        print(f"  障害レース数: {obstacle['race_id'].nunique():,}")
        
        if len(obstacle) > 0:
            # 距離分布
            if 'distance_m' in df.columns:
                obs_dist = obstacle['distance_m'].value_counts().head(5)
                print(f"  障害レース距離分布（上位5）:")
                for dist, cnt in obs_dist.items():
                    print(f"    {dist}m: {cnt:,}")
            
            # 障害馬が平地に出走しているか
            obstacle_horses = obstacle['horse_id'].unique()
            flat_races = df[(df['horse_id'].isin(obstacle_horses)) & 
                           (df['track_surface'] != '障害')]
            print(f"  障害馬の平地レース出走数: {len(flat_races):,}")
    
    # 年代別データ品質の不均一性
    print(f"\n【年代別データ品質の不均一性】")
    quality_metrics = []
    
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        metrics = {
            'year': year,
            'records': len(year_data),
            'races': year_data['race_id'].nunique(),
        }
        
        # 主要カラムの欠損率
        for col in ['finish_time_seconds', 'last_3f_time', 'win_odds', 'horse_weight']:
            if col in year_data.columns:
                metrics[f'{col}_null_pct'] = year_data[col].isnull().mean() * 100
        
        quality_metrics.append(metrics)
    
    quality_df = pd.DataFrame(quality_metrics)
    print(quality_df.to_string(index=False))


def main():
    print("=" * 80)
    print("KeibaAI_v2 包括的データ品質チェック")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # データ読み込み
    data = load_all_parquet()
    
    # 各種チェック
    check_basic_statistics(data)
    check_temporal_consistency(data)
    check_referential_integrity(data)
    check_anomalies(data)
    check_compound_issues(data)
    
    print("\n" + "=" * 80)
    print("チェック完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
