"""

evaluate_model.pyのマージ処理をデバッグするスクリプト

"""

import pandas as pd

from pathlib import Path

import pyarrow.parquet as pq

 

print("="*80)

print("evaluate_model.py マージ処理デバッグ")

print("="*80)

 

# 1. 特徴量データを読み込む（評価用データ）

print("\n[1] 特徴量データの読み込み...")

features_path = Path('keibaai/data/features/parquet')

 

# 2024年1月のデータを1つだけ読み込む（テスト用）

test_file = list(features_path.glob("year=2024/month=1/*.parquet"))[0]

print(f"  テストファイル: {test_file}")

 

df = pd.read_parquet(test_file)

print(f"  行数: {len(df):,}")

print(f"  カラム数: {len(df.columns)}")

 

# 重要なカラムの存在確認

print(f"\n[2] キーカラムの存在確認:")

print(f"  race_id: {'✅' if 'race_id' in df.columns else '❌'}")

print(f"  horse_id: {'✅' if 'horse_id' in df.columns else '❌'}")

 

if 'race_id' in df.columns:

    print(f"  race_idのデータ型: {df['race_id'].dtype}")

    print(f"  race_idのサンプル: {df['race_id'].iloc[0]}")

 

if 'horse_id' in df.columns:

    print(f"  horse_idのデータ型: {df['horse_id'].dtype}")

    print(f"  horse_idのサンプル: {df['horse_id'].iloc[0]}")

 

# 3. horses_performanceを読み込む

print(f"\n[3] horses_performance.parquetの読み込み...")

perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')

perf_df = pd.read_parquet(perf_path)

 

print(f"  行数: {len(perf_df):,}")

print(f"  win_oddsカラム: {'✅' if 'win_odds' in perf_df.columns else '❌'}")

print(f"  race_idのデータ型: {perf_df['race_id'].dtype}")

print(f"  horse_idのデータ型: {perf_df['horse_id'].dtype}")

 

# 4. データ型を統一

print(f"\n[4] データ型の統一...")

if 'race_id' in df.columns and 'horse_id' in df.columns:

    # 特徴量データのキーを文字列に変換

    df['race_id'] = df['race_id'].astype(str)

    df['horse_id'] = df['horse_id'].astype(str)

 

    # performanceデータのキーも文字列に変換

    perf_df['race_id'] = perf_df['race_id'].astype(str)

    perf_df['horse_id'] = perf_df['horse_id'].astype(str)

 

    print(f"  特徴量データ - race_id: {df['race_id'].dtype}, horse_id: {df['horse_id'].dtype}")

    print(f"  performanceデータ - race_id: {perf_df['race_id'].dtype}, horse_id: {perf_df['horse_id'].dtype}")

 

# 5. マージ前の確認

print(f"\n[5] マージ前の確認:")

print(f"  特徴量データのユニークキー数: {df[['race_id', 'horse_id']].drop_duplicates().shape[0]:,}")

print(f"  performanceデータのユニークキー数: {perf_df[['race_id', 'horse_id']].drop_duplicates().shape[0]:,}")

 

# サンプルキーを取得

sample_race_id = df['race_id'].iloc[0]

sample_horse_id = df['horse_id'].iloc[0]

print(f"\n  サンプルキー: race_id={sample_race_id}, horse_id={sample_horse_id}")

 

# performanceデータに存在するか確認

matching_perf = perf_df[

    (perf_df['race_id'] == sample_race_id) &

    (perf_df['horse_id'] == sample_horse_id)

]

print(f"  performanceデータに存在: {'✅' if len(matching_perf) > 0 else '❌'}")

if len(matching_perf) > 0:

    print(f"  win_odds: {matching_perf['win_odds'].iloc[0]}")

 

# 6. マージを実行

print(f"\n[6] マージの実行...")

# win_oddsカラムのみを取得

odds_df = perf_df[['race_id', 'horse_id', 'win_odds']].copy()

 

print(f"  odds_df行数: {len(odds_df):,}")

print(f"  odds_dfカラム: {list(odds_df.columns)}")

 

merged_df = df.merge(

    odds_df,

    on=['race_id', 'horse_id'],

    how='left'

)

 

print(f"\n[7] マージ結果:")

print(f"  マージ後の行数: {len(merged_df):,}")

print(f"  win_oddsカラムが存在: {'✅' if 'win_odds' in merged_df.columns else '❌'}")

 

if 'win_odds' in merged_df.columns:

    odds_count = merged_df['win_odds'].notna().sum()

    print(f"  win_odds非null数: {odds_count:,}/{len(merged_df):,} ({odds_count/len(merged_df)*100:.1f}%)")

    print(f"  win_oddsのサンプル値:")

    print(merged_df[merged_df['win_odds'].notna()][['race_id', 'horse_id', 'win_odds']].head(5))

else:

    print(f"  ❌ win_oddsカラムが消失しました！")

    print(f"  マージ後のカラム: {list(merged_df.columns)[:20]}...")

 

print("\n" + "="*80)

print("デバッグ完了")

print("="*80)