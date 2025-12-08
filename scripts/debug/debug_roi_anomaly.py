"""
V7 ROI異常値の原因調査

【仮説】
1. ROI計算のhorse_number/race_id型不一致によるマージ重複
2. returns.parquetの払戻データ異常
3. V7の特徴量にリークがある
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
    print("V7 ROI異常値の原因調査")
    print("="*70)
    
    # データ読み込み
    print("\n[1] データ読み込み...")
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    print(f"  races_df: {len(races_df):,}行")
    print(f"  returns_df: {len(returns_df):,}行")
    
    # Test期間のデータ
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    print(f"  Test期間: {len(test_df):,}行")
    
    # [2] 単勝払戻データを確認
    print("\n[2] 単勝払戻データを確認...")
    tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
    print(f"  tansho: {len(tansho):,}行")
    print(f"  race_id型: {tansho['race_id'].dtype}")
    print(f"  horse_number型: {tansho['horse_number'].dtype}")
    print(f"  payout統計: min={tansho['payout'].min()}, max={tansho['payout'].max()}, mean={tansho['payout'].mean():.0f}")
    
    # [3] マージの確認
    print("\n[3] マージの確認...")
    
    # Test期間のレースID
    test_race_ids = set(test_df['race_id'].astype(str).unique())
    print(f"  Test期間のレース数: {len(test_race_ids)}")
    
    # 単勝データのレースID
    tansho_race_ids = set(tansho['race_id'].astype(str).unique())
    print(f"  単勝データのレース数: {len(tansho_race_ids)}")
    
    # 重複確認
    overlap = test_race_ids & tansho_race_ids
    print(f"  重複レース数: {len(overlap)}")
    
    # [4] 単純なROI計算（人気順位1位に賭けた場合）
    print("\n[4] 人気1位に賭けた場合のROI（ベースライン）...")
    
    # 人気1位を抽出
    test_df['popularity'] = pd.to_numeric(test_df['popularity'], errors='coerce')
    pop1 = test_df[test_df['popularity'] == 1].copy()
    print(f"  人気1位の馬: {len(pop1)}頭")
    
    # 単勝払戻をマージ
    tansho_for_merge = tansho[['race_id', 'horse_number', 'payout']].copy()
    tansho_for_merge['race_id'] = tansho_for_merge['race_id'].astype(str)
    tansho_for_merge['horse_number'] = pd.to_numeric(tansho_for_merge['horse_number'], errors='coerce')
    
    pop1['race_id'] = pop1['race_id'].astype(str)
    pop1['horse_number'] = pd.to_numeric(pop1['horse_number'], errors='coerce')
    
    merged = pop1.merge(tansho_for_merge, on=['race_id', 'horse_number'], how='left')
    print(f"  マージ後: {len(merged)}行（重複確認）")
    
    # 重複チェック
    dup_count = merged.duplicated(subset=['race_id', 'horse_number']).sum()
    print(f"  重複行数: {dup_count}")
    
    # ROI計算
    total_bet = len(pop1) * 100  # 重複前の行数で計算
    total_payout = merged['payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100
    
    hits = (merged['payout'] > 0).sum()
    hit_rate = hits / len(pop1) * 100
    
    print(f"\n  人気1位ベースライン:")
    print(f"    ベット数: {len(pop1)}")
    print(f"    的中数: {hits}")
    print(f"    的中率: {hit_rate:.1f}%")
    print(f"    払戻合計: ¥{total_payout:,.0f}")
    print(f"    投資合計: ¥{total_bet:,}")
    print(f"    ROI: {roi:.1f}%")
    
    # [5] 単勝払戻データの重複確認
    print("\n[5] 単勝払戻データの重複確認...")
    tansho_dup = tansho.duplicated(subset=['race_id', 'horse_number']).sum()
    print(f"  単勝データの重複: {tansho_dup}行")
    
    if tansho_dup > 0:
        print("  ⚠ 単勝データに重複があります！")
        dup_examples = tansho[tansho.duplicated(subset=['race_id', 'horse_number'], keep=False)].head(10)
        print(dup_examples)
    
    # [6] win_oddsとの比較
    print("\n[6] win_oddsとpayoutの比較...")
    
    # 1着馬のwin_odds
    winners = test_df[test_df['finish_position'] == 1].copy()
    print(f"  Test期間の1着馬: {len(winners)}頭")
    
    winners['race_id'] = winners['race_id'].astype(str)
    winners['horse_number'] = pd.to_numeric(winners['horse_number'], errors='coerce')
    
    winners_with_payout = winners.merge(tansho_for_merge, on=['race_id', 'horse_number'], how='left')
    
    # win_odds * 100 と payout の比較
    winners_with_payout['expected_payout'] = winners_with_payout['win_odds'] * 100
    winners_with_payout['diff'] = winners_with_payout['payout'] - winners_with_payout['expected_payout']
    
    print(f"  payout vs win_odds*100:")
    print(f"    一致率: {(abs(winners_with_payout['diff']) < 10).mean()*100:.1f}%")
    print(f"    平均差: {winners_with_payout['diff'].mean():.1f}")
    
    # [7] 結論
    print("\n" + "="*70)
    print("[結論]")
    if roi > 150:
        print("  ⚠ ROIが異常に高い。以下を確認してください:")
        print("    - 単勝データに重複がないか")
        print("    - マージキーの型が一致しているか")
        print("    - 払戻金額が正しいか")
    else:
        print(f"  ✓ 人気1位のROI {roi:.1f}% は妥当な範囲です")


if __name__ == '__main__':
    main()
