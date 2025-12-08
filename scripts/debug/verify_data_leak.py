"""
データリーク検証スクリプト

統計計算が未来のデータを含んでいるか確認
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def main():
    print("=" * 70)
    print("データリーク検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[(df['race_date'] >= '2020-01-01') & (df['race_date'] <= '2024-06-30')]
    df = df.dropna(subset=['finish_position', 'win_odds'])
    
    print(f"\n全データ: {len(df):,} records")
    print(f"期間: {df['race_date'].min()} - {df['race_date'].max()}")
    
    # テスト期間の定義
    max_date = df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=6)
    
    train_df = df[df['race_date'] < test_start]
    test_df = df[df['race_date'] >= test_start]
    
    print(f"\nTrain: {len(train_df):,} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    print(f"Test: {len(test_df):,} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # === 問題1: 騎手統計 ===
    print("\n" + "=" * 50)
    print("[問題1] 騎手統計のリーク検証")
    print("=" * 50)
    
    # 全期間で計算した騎手勝率
    all_jockey_stats = df.groupby('jockey_id').apply(
        lambda x: (x['finish_position'] == 1).sum() / len(x)
    ).reset_index()
    all_jockey_stats.columns = ['jockey_id', 'win_rate_all']
    
    # 訓練期間のみで計算した騎手勝率
    train_jockey_stats = train_df.groupby('jockey_id').apply(
        lambda x: (x['finish_position'] == 1).sum() / len(x)
    ).reset_index()
    train_jockey_stats.columns = ['jockey_id', 'win_rate_train']
    
    # 比較
    merged = all_jockey_stats.merge(train_jockey_stats, on='jockey_id', how='inner')
    merged['diff'] = merged['win_rate_all'] - merged['win_rate_train']
    
    print(f"\n騎手数: {len(merged)}")
    print(f"勝率差の平均: {merged['diff'].mean():.4f}")
    print(f"勝率差の標準偏差: {merged['diff'].std():.4f}")
    print(f"勝率が大きく変わった騎手数 (|diff| > 0.05): {(merged['diff'].abs() > 0.05).sum()}")
    
    # 具体例
    top_diff = merged.nlargest(5, 'diff')
    print("\n全期間 vs 訓練期間で勝率が最も上がった騎手:")
    for _, row in top_diff.iterrows():
        print(f"  {row['jockey_id']}: {row['win_rate_train']:.3f} → {row['win_rate_all']:.3f} (+{row['diff']:.3f})")
    
    # === 問題2: テスト期間の騎手が訓練期間にいない場合 ===
    print("\n" + "=" * 50)
    print("[問題2] テスト期間の新規騎手")
    print("=" * 50)
    
    train_jockeys = set(train_df['jockey_id'].unique())
    test_jockeys = set(test_df['jockey_id'].unique())
    new_jockeys = test_jockeys - train_jockeys
    
    print(f"訓練期間の騎手数: {len(train_jockeys)}")
    print(f"テスト期間の騎手数: {len(test_jockeys)}")
    print(f"新規騎手数（訓練期間にいなかった）: {len(new_jockeys)}")
    
    # === 問題3: 血統統計のリーク ===
    print("\n" + "=" * 50)
    print("[問題3] 血統統計のリーク検証")
    print("=" * 50)
    
    # 血統データ読み込み
    pedigrees_path = data_dir / "parsed/parquet/pedigrees/pedigrees.parquet"
    pedigrees = pd.read_parquet(pedigrees_path)
    sires = pedigrees[pedigrees['generation'] == 1][['horse_id', 'ancestor_id']]
    sires.columns = ['horse_id', 'sire_id']
    
    df_with_sire = df.merge(sires, on='horse_id', how='left')
    df_with_sire = df_with_sire.dropna(subset=['sire_id'])
    
    train_with_sire = df_with_sire[df_with_sire['race_date'] < test_start]
    
    # 全期間の父馬勝率
    all_sire_stats = df_with_sire.groupby('sire_id').apply(
        lambda x: (x['finish_position'] == 1).sum() / len(x)
    ).reset_index()
    all_sire_stats.columns = ['sire_id', 'win_rate_all']
    
    # 訓練期間の父馬勝率
    train_sire_stats = train_with_sire.groupby('sire_id').apply(
        lambda x: (x['finish_position'] == 1).sum() / len(x)
    ).reset_index()
    train_sire_stats.columns = ['sire_id', 'win_rate_train']
    
    sire_merged = all_sire_stats.merge(train_sire_stats, on='sire_id', how='inner')
    sire_merged['diff'] = sire_merged['win_rate_all'] - sire_merged['win_rate_train']
    
    print(f"\n父馬数: {len(sire_merged)}")
    print(f"勝率差の平均: {sire_merged['diff'].mean():.4f}")
    print(f"勝率差の標準偏差: {sire_merged['diff'].std():.4f}")
    
    # === 結論 ===
    print("\n" + "=" * 70)
    print("結論")
    print("=" * 70)
    
    print("""
[問題の特定]
現在の実装では、fit()で全期間のデータを使用して統計を計算しています。
これには2024年1-6月のテスト期間のレース結果も含まれています。

例えば：
- 2024年5月に好成績を収めた騎手の勝率が上がる
- その高い勝率は、2024年1月のレース予測時にも使用される
- これは「未来の情報を使った予測」= データリーク

[影響の程度]
- 騎手勝率の差: 平均{avg_diff:.4f}
- 父馬勝率の差: 平均{sire_diff:.4f}

これらの差分が予測に影響を与え、ROIを過大評価している可能性が高いです。

[正しい実装]
1. fit()を訓練データのみで行う
2. または、各レースごとにそのレース以前のデータのみで統計を計算する（ローリング方式）
""".format(avg_diff=merged['diff'].mean(), sire_diff=sire_merged['diff'].mean()))


if __name__ == '__main__':
    main()
