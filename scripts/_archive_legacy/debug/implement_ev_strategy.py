"""
System A: EV戦略実装 - Step 1
期待値ベースのベット選択とバックテスト評価

このスクリプトは:
1. μv2.0モデルで勝率予測
2. 確定オッズと組み合わせてEV計算
3. EV閾値でベット選択
4. バックテストで各閾値のROI評価
"""

import pandas as pd
import numpy as np
import pickle
from pathlib import Path
from sklearn.metrics import roc_auc_score

def calculate_ev_strategy(
    model_path='keibaai/models/mu_v2/mu_v2_model.pkl',
    test_data_path='keibaai/models/mu_v2/train_data_mu_v2.parquet',
    odds_data_path='keibaai/data/parsed/parquet/races/races.parquet',
    test_start_date='2024-01-01'
):
    """
    EV戦略でベット選択とバックテスト
    """
    
    print("="*80)
    print("System A: EV戦略バックテスト")
    print("="*80)
    
    # 1. モデル読み込み
    print("\nStep 1: モデル読み込み...")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    print(f"  ✓ モデル読み込み完了")
    
    # 2. テストデータ読み込み
    print("\nStep 2: テストデータ読み込み...")
    test_df = pd.read_parquet(test_data_path)
    test_df['race_date'] = pd.to_datetime(test_df['race_date'])
    
    # テスト期間で絞り込み
    test_df = test_df[test_df['race_date'] >= test_start_date].copy()
    print(f"  ✓ テストデータ: {len(test_df):,}行")
    
    # 3. オッズデータ読み込み
    print("\nStep 3: オッズデータ読み込み...")
    odds_df = pd.read_parquet(odds_data_path)
    odds_df = odds_df[['race_id', 'horse_id', 'win_odds', 'finish_position']].copy()
    
    # 型統一
    test_df['race_id'] = test_df['race_id'].astype(str)
    test_df['horse_id'] = test_df['horse_id'].astype(str)
    odds_df['race_id'] = odds_df['race_id'].astype(str)
    odds_df['horse_id'] = odds_df['horse_id'].astype(str)
    
    # マージ
    test_df = test_df.merge(odds_df, on=['race_id', 'horse_id'], how='left', suffixes=('', '_odds'))
    
    # オッズがあるデータのみ
    test_df = test_df[test_df['win_odds'].notna()].copy()
    print(f"  ✓ オッズあり: {len(test_df):,}行")
    
    # 4. 特徴量抽出
    print("\nStep 4: 予測実行...")
    feature_cols = model.feature_name()
    X = test_df[feature_cols]
    
    # 予測勝率
    win_prob = model.predict(X, num_iteration=model.best_iteration)
    test_df['win_prob'] = win_prob
    
    # AUC確認
    if 'finish_position' in test_df.columns:
        is_win = (test_df['finish_position'] == 1).astype(int)
        auc = roc_auc_score(is_win, win_prob)
        print(f"  ✓ AUC: {auc:.4f}")
    
    # 5. EV計算
    print("\nStep 5: EV計算...")
    test_df['ev'] = (
        test_df['win_prob'] * test_df['win_odds'] - 
        (1 - test_df['win_prob'])
    )
    print(f"  ✓ 平均EV: {test_df['ev'].mean():.4f}")
    print(f"  ✓ 正のEV: {(test_df['ev'] > 0).sum():,}件 ({(test_df['ev'] > 0).mean():.1%})")
    
    return test_df

def evaluate_ev_strategy(results_df, ev_threshold):
    """
    EV戦略のバックテスト評価
    """
    
    # ベット対象を抽出
    bets = results_df[results_df['ev'] > ev_threshold].copy()
    
    if len(bets) == 0:
        return {
            'threshold': ev_threshold,
            'roi': 0.0,
            'hit_rate': 0.0,
            'avg_odds': 0.0,
            'bet_ratio': 0.0,
            'profit': 0.0,
            'total_bets': 0,
            'total_races': len(results_df)
        }
    
    # 的中判定
    bets['is_hit'] = (bets['finish_position'] == 1).astype(int)
    
    # 指標計算
    total_bets = len(bets)
    total_races = len(results_df)
    hits = bets['is_hit'].sum()
    hit_rate = hits / total_bets if total_bets > 0 else 0
    
    # 配当計算（100円ベット）
    total_return = (bets['is_hit'] * bets['win_odds']).sum() * 100
    total_cost = total_bets * 100
    roi = total_return / total_cost if total_cost > 0 else 0
    profit = total_return - total_cost
    
    avg_odds = bets['win_odds'].mean()
    bet_ratio = total_bets / total_races
    
    return {
        'threshold': ev_threshold,
        'roi': roi,
        'hit_rate': hit_rate,
        'avg_odds': avg_odds,
        'bet_ratio': bet_ratio,
        'profit': profit,
        'total_bets': total_bets,
        'total_races': total_races,
        'hits': hits
    }

def grid_search_threshold(results_df, thresholds=None):
    """
    閾値のグリッドサーチ
    """
    
    if thresholds is None:
        thresholds = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]
    
    print("\n" + "="*80)
    print("閾値グリッドサーチ")
    print("="*80)
    
    results = []
    
    for threshold in thresholds:
        metrics = evaluate_ev_strategy(results_df, threshold)
        results.append(metrics)
        
        print(f"\nEV閾値: {threshold:.2f}")
        print(f"  ROI: {metrics['roi']:.2%}")
        print(f"  的中率: {metrics['hit_rate']:.2%}")
        print(f"  平均配当: {metrics['avg_odds']:.2f}倍")
        print(f"  ベット率: {metrics['bet_ratio']:.2%} ({metrics['total_bets']}/{metrics['total_races']})")
        print(f"  収支: {metrics['profit']:,.0f}円")
        print(f"  的中数: {metrics['hits']}")
    
    # DataFrame化
    results_df = pd.DataFrame(results)
    
    # 最適閾値
    print("\n" + "="*80)
    print("最適閾値分析")
    print("="*80)
    
    best_roi_idx = results_df['roi'].idxmax()
    best_threshold = results_df.loc[best_roi_idx, 'threshold']
    best_roi = results_df.loc[best_roi_idx, 'roi']
    
    print(f"\n最高ROI: {best_roi:.2%} (閾値={best_threshold:.2f})")
    
    # ROI > 1.0の閾値
    profitable = results_df[results_df['roi'] > 1.0]
    if len(profitable) > 0:
        print(f"\nROI > 100%の閾値: {profitable['threshold'].tolist()}")
    else:
        print("\n⚠️ ROI > 100%を達成する閾値なし")
    
    # 保存
    output_path = 'keibaai/data/evaluation/ev_threshold_analysis.csv'
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ 結果を保存: {output_path}")
    
    return results_df

def main():
    """
    メイン処理
    """
    
    # Step 1: EV計算
    results = calculate_ev_strategy(
        model_path='keibaai/models/mu_v2/mu_v2_model.pkl',
        test_data_path='keibaai/models/mu_v2/train_data_mu_v2.parquet',
        odds_data_path='keibaai/data/parsed/parquet/races/races.parquet',
        test_start_date='2024-01-01'
    )
    
    # Step 2: グリッドサーチ
    threshold_results = grid_search_threshold(results)
    
    # Step 3: ベースライン比較
    print("\n" + "="*80)
    print("ベースライン比較")
    print("="*80)
    
    baseline = evaluate_ev_strategy(results, ev_threshold=-999)  # 全レースベット
    print(f"\n全レースベット（ベースライン）:")
    print(f"  ROI: {baseline['roi']:.2%}")
    print(f"  的中率: {baseline['hit_rate']:.2%}")
    print(f"  平均配当: {baseline['avg_odds']:.2f}倍")
    print(f"  収支: {baseline['profit']:,.0f}円")
    
    # 改善効果
    best_roi = threshold_results['roi'].max()
    improvement = best_roi - baseline['roi']
    print(f"\n最大改善効果: +{improvement:.2%}pt ({baseline['roi']:.2%} → {best_roi:.2%})")
    
    print("\n" + "="*80)
    print("EV戦略バックテスト完了")
    print("="*80)

if __name__ == "__main__":
    main()
