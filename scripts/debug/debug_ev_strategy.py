"""
EV戦略のデバッグスクリプト

原因を特定するため、以下を確認：
1. レースごとのTop1選択をしているか
2. evaluate_mu_v2_for_evaluation.pyとの違い
3. データの範囲と集計方法
"""

import pandas as pd
import numpy as np
import pickle
from pathlib import Path

def debug_ev_strategy():
    """
    デバッグ分析
    """
    
    print("="*80)
    print("EV戦略デバッグ分析")
    print("="*80)
    
    # 1. データ読み込み
    print("\n[1] データ読み込み")
    model_path = 'keibaai/models/mu_v2/mu_v2_model.pkl'
    test_data_path = 'keibaai/models/mu_v2/train_data_mu_v2.parquet'
    odds_data_path = 'keibaai/data/parsed/parquet/races/races.parquet'
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    test_df = pd.read_parquet(test_data_path)
    test_df['race_date'] = pd.to_datetime(test_df['race_date'])
    test_df = test_df[test_df['race_date'] >= '2024-01-01'].copy()
    
    odds_df = pd.read_parquet(odds_data_path)
    odds_df = odds_df[['race_id', 'horse_id', 'win_odds', 'finish_position']].copy()
    
    # 型統一
    test_df['race_id'] = test_df['race_id'].astype(str)
    test_df['horse_id'] = test_df['horse_id'].astype(str)
    odds_df['race_id'] = odds_df['race_id'].astype(str)
    odds_df['horse_id'] = odds_df['horse_id'].astype(str)
    
    # マージ
    test_df = test_df.merge(odds_df, on=['race_id', 'horse_id'], how='left', suffixes=('', '_odds'))
    test_df = test_df[test_df['win_odds'].notna()].copy()
    
    print(f"  総行数: {len(test_df):,}")
    print(f"  総レース数: {test_df['race_id'].nunique():,}")
    
    # 2. 予測
    print("\n[2] 予測実行")
    feature_cols = model.feature_name()
    X = test_df[feature_cols]
    win_prob = model.predict(X, num_iteration=model.best_iteration)
    test_df['win_prob'] = win_prob
    
    print(f"  予測勝率（平均）: {win_prob.mean():.4f}")
    print(f"  予測勝率（最大）: {win_prob.max():.4f}")
    print(f"  予測勝率（最小）: {win_prob.min():.4f}")
    
    # 3. レースごとのTop1を確認
    print("\n[3] レースごとのTop1分析")
    
    # レース内順位
    test_df['rank_in_race'] = test_df.groupby('race_id')['win_prob'].rank(
        ascending=False, method='first'
    )
    
    # Top1のみ
    top1 = test_df[test_df['rank_in_race'] == 1].copy()
    
    print(f"  Top1レース数: {len(top1):,}")
    print(f"  総レース数: {test_df['race_id'].nunique():,}")
    
    # Top1の的中率
    top1_hits = (top1['finish_position'] == 1).sum()
    top1_hit_rate = top1_hits / len(top1)
    
    print(f"\n  Top1の的中:")
    print(f"    的中数: {top1_hits}")
    print(f"    的中率: {top1_hit_rate:.2%}")
    
    # Top1の平均配当
    top1_avg_odds = top1['win_odds'].mean()
    print(f"    平均配当: {top1_avg_odds:.2f}倍")
    
    # Top1のROI
    top1_return = (top1[top1['finish_position'] == 1]['win_odds'].sum()) * 100
    top1_cost = len(top1) * 100
    top1_roi = top1_return / top1_cost
    
    print(f"    ROI: {top1_roi:.2%}")
    print(f"    収支: {top1_return - top1_cost:,.0f}円")
    
    # 4. EV分析
    print("\n[4] EV分析")
    top1['ev'] = top1['win_prob'] * top1['win_odds'] - (1 - top1['win_prob'])
    
    print(f"  Top1のEV（平均）: {top1['ev'].mean():.4f}")
    print(f"  Top1のEV > 0: {(top1['ev'] > 0).sum()} / {len(top1)} ({(top1['ev'] > 0).mean():.1%})")
    print(f"  Top1のEV > 0.1: {(top1['ev'] > 0.1).sum()} / {len(top1)} ({(top1['ev'] > 0.1).mean():.1%})")
    
    # 5. 全馬でのEV（間違った方法）
    print("\n[5] 全馬でのEV（現在の実装）")
    test_df['ev'] = test_df['win_prob'] * test_df['win_odds'] - (1 - test_df['win_prob'])
    
    all_positive_ev = test_df[test_df['ev'] > 0].copy()
    
    print(f"  EV > 0の馬数: {len(all_positive_ev):,}")
    print(f"  的中数: {(all_positive_ev['finish_position'] == 1).sum()}")
    print(f"  的中率: {(all_positive_ev['finish_position'] == 1).mean():.2%}")
    print(f"  平均配当: {all_positive_ev['win_odds'].mean():.2f}倍")
    
    # 6. evaluate_mu_v2_for_evaluation.pyと比較
    print("\n[6] evaluate_mu_v2_for_evaluation.pyとの比較")
    
    # レースごとに最高予測の馬を選択（正しい方法）
    top1_correct = test_df.loc[test_df.groupby('race_id')['win_prob'].idxmax()].copy()
    
    print(f"  正しいTop1選択:")
    print(f"    レース数: {len(top1_correct):,}")
    print(f"    的中数: {(top1_correct['finish_position'] == 1).sum()}")
    print(f"    的中率: {(top1_correct['finish_position'] == 1).mean():.2%}")
    
    # ROI計算
    correct_return = (top1_correct[top1_correct['finish_position'] == 1]['win_odds'].sum()) * 100
    correct_cost = len(top1_correct) * 100
    correct_roi = correct_return / correct_cost
    
    print(f"    平均配当: {top1_correct['win_odds'].mean():.2f}倍")
    print(f"    ROI: {correct_roi:.2%}")
    print(f"    収支: {correct_return - correct_cost:,.0f}円")
    
    # 7. EV戦略（正しい方法）
    print("\n[7] EV戦略（正しい実装）")
    
    top1_correct['ev'] = (
        top1_correct['win_prob'] * top1_correct['win_odds'] - 
        (1 - top1_correct['win_prob'])
    )
    
    for threshold in [0.0, 0.1, 0.15, 0.2]:
        bets = top1_correct[top1_correct['ev'] > threshold].copy()
        
        if len(bets) == 0:
            continue
        
        hits = (bets['finish_position'] == 1).sum()
        hit_rate = hits / len(bets)
        total_return = (bets[bets['finish_position'] == 1]['win_odds'].sum()) * 100
        total_cost = len(bets) * 100
        roi = total_return / total_cost
        
        print(f"\n  EV閾値 = {threshold:.2f}:")
        print(f"    ベット数: {len(bets):,} ({len(bets)/len(top1_correct):.1%})")
        print(f"    的中率: {hit_rate:.2%}")
        print(f"    平均配当: {bets['win_odds'].mean():.2f}倍")
        print(f"    ROI: {roi:.2%}")
        print(f"    収支: {total_return - total_cost:,.0f}円")
    
    # 8. サンプル確認
    print("\n[8] サンプルデータ確認")
    
    # ランダムに5レース
    sample_races = test_df['race_id'].unique()[:5]
    
    for race_id in sample_races:
        race_data = test_df[test_df['race_id'] == race_id].copy()
        race_data = race_data.sort_values('win_prob', ascending=False)
        
        print(f"\n  レース {race_id}:")
        print(f"    出走数: {len(race_data)}")
        print(f"\n    順位 | 予測勝率 | オッズ | EV    | 実着順")
        print(f"    " + "-" * 50)
        
        for i, (_, horse) in enumerate(race_data.head(3).iterrows(), 1):
            ev = horse['win_prob'] * horse['win_odds'] - (1 - horse['win_prob'])
            print(f"    {i:2d}位 | {horse['win_prob']:7.4f} | {horse['win_odds']:5.1f}倍 | {ev:6.2f} | {horse['finish_position']:.0f}着")
    
    print("\n" + "="*80)
    print("デバッグ完了")
    print("="*80)

if __name__ == "__main__":
    debug_ev_strategy()
