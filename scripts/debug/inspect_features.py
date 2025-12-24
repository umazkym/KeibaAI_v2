"""
特定のレースで使用されている全特徴量の詳細確認

1着になった馬の特徴量を詳しく見て、リークがないか確認
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

print("="*80)
print("特定レースの特徴量詳細確認")
print("="*80)

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
corners_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')

train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

# V7 fit & transform
fe = LeakFreeFeatureEngineerV7()
fe.fit(
    races_df=train_df,
    pedigrees_df=pedigrees_df,
    corners_df=corners_df,
    race_details_df=race_details_df,
    returns_df=returns_df
)
test_features = fe.transform(test_df)

# 特定のレースを選択（2025年のレースから1つ）
sample_race_ids = test_features['race_id'].unique()[:3]

for sample_race_id in sample_race_ids:
    race_data = test_features[test_features['race_id'] == sample_race_id].copy()
    race_date = race_data['race_date'].iloc[0]
    
    print("\n" + "="*80)
    print(f"レースID: {sample_race_id}")
    print(f"レース日: {race_date}")
    print(f"出走頭数: {len(race_data)}")
    print("="*80)
    
    # 1着の馬を特定
    winner = race_data[race_data['finish_position'] == 1]
    if len(winner) == 0:
        print("1着馬なし")
        continue
    winner = winner.iloc[0]
    
    print(f"\n【1着馬の情報】")
    print(f"  馬番: {winner['horse_number']}")
    print(f"  馬ID: {winner['horse_id']}")
    print(f"  着順: {winner['finish_position']} (これは特徴量ではなく評価用)")
    print(f"  オッズ: {winner['win_odds']}")
    
    print(f"\n【使用された特徴量一覧】")
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in test_features.columns]
    
    for col in feature_cols:
        val = winner.get(col, 'N/A')
        if pd.notna(val):
            if isinstance(val, float):
                print(f"  {col}: {val:.4f}")
            else:
                print(f"  {col}: {val}")
        else:
            print(f"  {col}: NaN")
    
    # この馬の過去レース履歴を確認
    horse_id = winner['horse_id']
    all_data = pd.concat([train_df, test_df])
    horse_history = all_data[(all_data['horse_id'] == horse_id) & (all_data['race_date'] < race_date)].sort_values('race_date')
    
    print(f"\n【この馬の過去レース履歴（{len(horse_history)}レース）】")
    if len(horse_history) > 0:
        print(f"  最初のレース: {horse_history['race_date'].iloc[0].date()}")
        print(f"  最後のレース: {horse_history['race_date'].iloc[-1].date()}")
        wins = (horse_history['finish_position'] == 1).sum()
        top3 = (horse_history['finish_position'] <= 3).sum()
        print(f"  勝利数: {wins}/{len(horse_history)} = {wins/len(horse_history)*100:.1f}%")
        print(f"  トップ3: {top3}/{len(horse_history)} = {top3/len(horse_history)*100:.1f}%")
        
        # V7が計算した値と比較
        v7_win = winner.get('horse_win_rate', np.nan)
        v7_top3 = winner.get('horse_top3_rate', np.nan)
        manual_win = wins/len(horse_history) if len(horse_history) >= 3 else np.nan
        manual_top3 = top3/len(horse_history) if len(horse_history) >= 3 else np.nan
        
        print(f"\n【V7の計算値 vs 手動計算】")
        print(f"  horse_total_races: V7={winner.get('horse_total_races', 'N/A')}, 手動={len(horse_history)}")
        v7_win_str = f"{v7_win:.4f}" if pd.notna(v7_win) else "N/A"
        manual_win_str = f"{manual_win:.4f}" if pd.notna(manual_win) else "N/A"
        v7_top3_str = f"{v7_top3:.4f}" if pd.notna(v7_top3) else "N/A"
        manual_top3_str = f"{manual_top3:.4f}" if pd.notna(manual_top3) else "N/A"
        print(f"  horse_win_rate: V7={v7_win_str}, 手動={manual_win_str}")
        print(f"  horse_top3_rate: V7={v7_top3_str}, 手動={manual_top3_str}")

    # 今回のレース自体の情報がリークしていないか確認
    print(f"\n【リークチェック】")
    print(f"  今回のレース結果（着順={int(winner['finish_position'])}）が特徴量に含まれていないか:")
    print(f"    - horse_last_finish = {winner.get('horse_last_finish', 'N/A')} "
          f"({'✓ 今回の着順と異なる' if winner.get('horse_last_finish') != winner['finish_position'] else '⚠️ 一致!'})")
    
    # 騎手・調教師の統計も確認
    jockey_id = winner.get('jockey_id')
    trainer_id = winner.get('trainer_id')
    
    jockey_history = all_data[(all_data['jockey_id'] == jockey_id) & (all_data['race_date'] < race_date)]
    trainer_history = all_data[(all_data['trainer_id'] == trainer_id) & (all_data['race_date'] < race_date)]
    
    print(f"\n【騎手統計の検証】")
    print(f"  騎手ID: {jockey_id}")
    print(f"  過去レース数: {len(jockey_history)}")
    if len(jockey_history) >= 3:
        j_wins = (jockey_history['finish_position'] == 1).sum()
        j_rate = j_wins / len(jockey_history)
        print(f"  手動計算 win_rate: {j_rate:.4f}")
        print(f"  V7 jockey_win_rate: {winner.get('jockey_win_rate', 'N/A'):.4f if pd.notna(winner.get('jockey_win_rate', np.nan)) else 'N/A'}")

print("\n" + "="*80)
print("検証完了")
print("="*80)
