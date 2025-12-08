"""
V7特徴量のリーク調査

【問題】
- 人気1位ベースライン: 的中率33%, ROI 78%
- V7モデル予測: 的中率41%, ROI 241%
- この差は異常。V7にリークがある可能性が高い

【調査対象】
1. horse_last_finish（前走着順）がリークしていないか
2. cumsum+shiftのパターンが正しいか
3. transform時に未来データが混入していないか
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*70)
    print("V7特徴量のリーク調査")
    print("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    print(f"Train: {len(train_df):,}行 ({train_df['race_date'].min().date()} - {train_df['race_date'].max().date()})")
    print(f"Test: {len(test_df):,}行 ({test_df['race_date'].min().date()} - {test_df['race_date'].max().date()})")
    
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    # [1] fit時に使用されるデータを確認
    print("\n[1] fit時のデータ範囲を確認...")
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df, 
           race_details_df=race_details_df, returns_df=returns_df)
    
    print(f"  fit_date: {fe.fit_date}")
    print(f"  全履歴行数: {len(fe._full_history):,}")
    
    # [2] Test期間の特定の馬のデータを追跡
    print("\n[2] 特定の馬のデータを追跡...")
    
    # Test期間で1着だった馬をサンプリング
    test_winners = test_df[test_df['finish_position'] == 1].head(3)
    sample_horse_id = test_winners.iloc[0]['horse_id']
    sample_race_id = test_winners.iloc[0]['race_id']
    sample_race_date = test_winners.iloc[0]['race_date']
    
    print(f"  サンプル馬: {sample_horse_id}")
    print(f"  サンプルレース: {sample_race_id}")
    print(f"  レース日: {sample_race_date}")
    
    # この馬の全履歴を確認
    horse_hist = races_df[races_df['horse_id'] == sample_horse_id].sort_values('race_date')
    print(f"  馬の全レース数: {len(horse_hist)}")
    
    # fit履歴に含まれているか
    horse_in_fit = fe._full_history[fe._full_history['horse_id'] == sample_horse_id]
    print(f"  fit履歴に含まれる数: {len(horse_in_fit)}")
    
    # Test期間のレースが fit履歴に含まれていないか確認
    test_races_in_fit = fe._full_history[fe._full_history['race_date'] >= pd.to_datetime('2025-01-01')]
    print(f"  fit履歴のTest期間レース数: {len(test_races_in_fit)}")
    
    if len(test_races_in_fit) > 0:
        print("  ⚠ 問題発見: fit履歴にTest期間のデータが含まれています！")
    else:
        print("  ✓ fit履歴にTest期間のデータは含まれていません")
    
    # [3] transform時のデータフロー確認
    print("\n[3] transform時のデータフロー確認...")
    
    # Test期間の1レースだけを変換
    single_race = test_df[test_df['race_id'] == sample_race_id].copy()
    print(f"  単一レースの行数: {len(single_race)}")
    
    # transform
    result = fe.transform(single_race)
    
    # サンプル馬のhorse_last_finishを確認
    sample_row = result[result['horse_id'] == sample_horse_id]
    if len(sample_row) > 0:
        horse_last_finish = sample_row.iloc[0]['horse_last_finish']
        horse_win_rate = sample_row.iloc[0]['horse_win_rate']
        horse_total_races = sample_row.iloc[0]['horse_total_races']
        
        print(f"  horse_last_finish: {horse_last_finish}")
        print(f"  horse_win_rate: {horse_win_rate}")
        print(f"  horse_total_races: {horse_total_races}")
        
        # 実際の前走を確認
        actual_prev = horse_hist[horse_hist['race_date'] < sample_race_date].tail(1)
        if len(actual_prev) > 0:
            actual_last_finish = actual_prev.iloc[0]['finish_position']
            print(f"  実際の前走着順: {actual_last_finish}")
            
            if horse_last_finish == actual_last_finish:
                print("  ✓ horse_last_finishは正しい")
            else:
                print(f"  ⚠ horse_last_finishが不一致！({horse_last_finish} vs {actual_last_finish})")
    
    # [4] 相関チェック
    print("\n[4] 特徴量とfinish_positionの相関チェック...")
    
    test_features = fe.transform(test_df)
    
    # 各特徴量とfinish_positionの相関
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in test_features.columns]
    
    correlations = []
    for col in feature_cols:
        if test_features[col].notna().sum() > 100:
            corr = test_features[[col, 'finish_position']].corr().iloc[0, 1]
            correlations.append((col, corr))
    
    correlations = sorted(correlations, key=lambda x: abs(x[1]), reverse=True)
    
    print("  最も相関が高い特徴量:")
    for col, corr in correlations[:10]:
        # 高い相関（>0.3）は要注意
        marker = "⚠" if abs(corr) > 0.3 else ""
        print(f"    {marker}{col}: {corr:.3f}")
    
    # [5] 結論
    print("\n" + "="*70)
    print("[結論]")
    
    # 的中率の差を再確認
    test_features['popularity'] = pd.to_numeric(test_features['popularity'], errors='coerce')
    pop1_hit = (test_features[test_features['popularity'] == 1]['finish_position'] == 1).mean()
    
    print(f"  人気1位の的中率: {pop1_hit*100:.1f}%")


if __name__ == '__main__':
    main()
