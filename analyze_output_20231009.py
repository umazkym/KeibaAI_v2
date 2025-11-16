# output_20231009 詳細分析スクリプト

import pandas as pd
import sys
from pathlib import Path

# CSVファイルのパス
race_results_path = Path('output_20231009/race_results.csv')
shutuba_metadata_path = Path('output_20231009/shutuba_metadata.csv')

print("=" * 60)
print("output_20231009 詳細分析")
print("=" * 60)

# 1. race_results.csv の分析
if race_results_path.exists():
    print(f"\n【1】race_results.csv 分析")
    df = pd.read_csv(race_results_path)

    print(f"\n基本統計:")
    print(f"  総行数: {len(df)}")
    print(f"  ユニークレース数: {df['race_id'].nunique()}")

    # 欠損分析
    print(f"\n欠損分析:")
    distance_missing = df['distance_m'].isna().sum()
    surface_missing = df['track_surface'].isna().sum()
    print(f"  distance_m 欠損: {distance_missing}行 ({distance_missing/len(df)*100:.2f}%)")
    print(f"  track_surface 欠損: {surface_missing}行 ({surface_missing/len(df)*100:.2f}%)")

    # 欠損が発生しているレースを特定
    if distance_missing > 0:
        print(f"\n欠損しているレース:")
        missing_races = df[df['distance_m'].isna()]['race_id'].unique()
        for race_id in sorted(missing_races):
            race_data = df[df['race_id'] == race_id]
            race_name = race_data.iloc[0]['race_name'] if not race_data.empty else 'N/A'
            print(f"    {race_id}: {race_name} ({len(race_data)}頭)")

    # race_class分布
    print(f"\nrace_class 分布:")
    race_class_counts = df['race_class'].value_counts()
    for cls, count in race_class_counts.items():
        unique_races = df[df['race_class'] == cls]['race_id'].nunique()
        print(f"  {cls}: {count}行 ({unique_races}レース)")

    # track_surface分布
    print(f"\ntrack_surface 分布:")
    surface_counts = df['track_surface'].value_counts(dropna=False)
    for surface, count in surface_counts.items():
        print(f"  {surface}: {count}行")

    # サンプルデータ表示
    print(f"\n最初の3行（主要カラムのみ）:")
    sample_cols = ['race_id', 'race_name', 'distance_m', 'track_surface', 'race_class', 'horse_name', 'finish_position']
    print(df[sample_cols].head(3).to_string(index=False))

else:
    print(f"\n[!] race_results.csv が見つかりません: {race_results_path}")

# 2. shutuba_metadata.csv の分析
if shutuba_metadata_path.exists():
    print(f"\n\n【2】shutuba_metadata.csv 分析")
    df_shutuba = pd.read_csv(shutuba_metadata_path)

    print(f"\n基本統計:")
    print(f"  総行数: {len(df_shutuba)}")

    # 欠損分析
    print(f"\n欠損分析:")
    for col in ['distance_m', 'track_surface', 'weather', 'track_condition']:
        if col in df_shutuba.columns:
            missing = df_shutuba[col].isna().sum()
            print(f"  {col} 欠損: {missing}行 ({missing/len(df_shutuba)*100:.2f}%)")

    # サンプルデータ表示
    print(f"\n最初の5行:")
    print(df_shutuba.head(5).to_string(index=False))

else:
    print(f"\n[!] shutuba_metadata.csv が見つかりません: {shutuba_metadata_path}")

print(f"\n{'='*60}")
print("分析完了")
print(f"{'='*60}")
