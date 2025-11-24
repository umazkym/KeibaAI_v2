"""
マージ後にwin_oddsカラムが消失する問題をデバッグするスクリプト
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

print("="*80)
print("マージ後のwin_odds消失問題デバッグ")
print("="*80)

# 1. 特徴量データのサンプルを読み込む
features_path = Path('keibaai/data/features/parquet')
test_file = list(features_path.glob("year=2024/month=1/*.parquet"))[0]
features_df = pd.read_parquet(test_file)

print(f"\n[1] 特徴量データ")
print(f"  行数: {len(features_df):,}")
print(f"  race_id, horse_idの存在: race_id={'✅' if 'race_id' in features_df.columns else '❌'}, horse_id={'✅' if 'horse_id' in features_df.columns else '❌'}")
print(f"  win_oddsの存在: {'✅ 存在（問題）' if 'win_odds' in features_df.columns else '❌ 不在（正常）'}")

# 2. horses_performanceを読み込む
perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
perf_df = pd.read_parquet(perf_path)

# 2024年のデータのみ
perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
start_dt = datetime(2024, 1, 1)
end_dt = datetime(2024, 12, 31)
perf_df = perf_df[(perf_df['race_date'] >= start_dt) & (perf_df['race_date'] <= end_dt)]

print(f"\n[2] horses_performance（2024年データ）")
print(f"  行数: {len(perf_df):,}")
print(f"  win_oddsの存在: {'✅' if 'win_odds' in perf_df.columns else '❌'}")
if 'win_odds' in perf_df.columns:
    print(f"  win_odds非null数: {perf_df['win_odds'].notna().sum():,}/{len(perf_df):,}")

# 3. キーを統一
features_df['race_id'] = features_df['race_id'].astype(str).str.strip()
features_df['horse_id'] = features_df['horse_id'].astype(str).str.strip()
perf_df['race_id'] = perf_df['race_id'].astype(str).str.strip()
perf_df['horse_id'] = perf_df['horse_id'].astype(str).str.strip()

print(f"\n[3] キーの統一完了")
print(f"  features_df: race_id={features_df['race_id'].dtype}, horse_id={features_df['horse_id'].dtype}")
print(f"  perf_df: race_id={perf_df['race_id'].dtype}, horse_id={perf_df['horse_id'].dtype}")

# 4. マージ前の確認
print(f"\n[4] マージ前の確認")
sample_race_id = features_df['race_id'].iloc[0]
sample_horse_id = features_df['horse_id'].iloc[0]
print(f"  サンプルキー: race_id={sample_race_id}, horse_id={sample_horse_id}")

matching_perf = perf_df[
    (perf_df['race_id'] == sample_race_id) &
    (perf_df['horse_id'] == sample_horse_id)
]
print(f"  perf_dfに存在: {'✅' if len(matching_perf) > 0 else '❌'}")
if len(matching_perf) > 0:
    print(f"  win_odds: {matching_perf['win_odds'].iloc[0]}")

# 5. オッズデータのみを取得
odds_df = perf_df[['race_id', 'horse_id', 'win_odds']].copy()
print(f"\n[5] odds_df作成")
print(f"  行数: {len(odds_df):,}")
print(f"  カラム: {list(odds_df.columns)}")
print(f"  win_odds非null数: {odds_df['win_odds'].notna().sum():,}")

# 6. マージ実行（suffixesを明示）
print(f"\n[6] マージ実行")
print(f"  マージ前のfeatures_dfカラム数: {len(features_df.columns)}")
print(f"  win_odds存在: {'✅' if 'win_odds' in features_df.columns else '❌'}")

merged_df = features_df.merge(
    odds_df,
    on=['race_id', 'horse_id'],
    how='left',
    suffixes=('', '_odds')  # 明示的にsuffixesを指定
)

print(f"\n[7] マージ結果")
print(f"  マージ後の行数: {len(merged_df):,}")
print(f"  マージ後のカラム数: {len(merged_df.columns)}")
print(f"  win_odds存在: {'✅' if 'win_odds' in merged_df.columns else '❌'}")

if 'win_odds' in merged_df.columns:
    odds_count = merged_df['win_odds'].notna().sum()
    print(f"  win_odds非null数: {odds_count:,}/{len(merged_df):,}")
    print(f"\n  サンプルデータ:")
    print(merged_df[merged_df['win_odds'].notna()][['race_id', 'horse_id', 'win_odds']].head(3))
else:
    print(f"  ❌ win_oddsカラムが消失しました！")
    # win_oddsで始まるカラムを探す
    odds_cols = [col for col in merged_df.columns if 'odds' in col.lower()]
    print(f"  オッズ関連のカラム: {odds_cols}")

print("\n" + "="*80)
print("デバッグ完了")
print("="*80)
