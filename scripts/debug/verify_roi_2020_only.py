# -*- coding: utf-8 -*-
"""
ROI 113.7% 報告値の追加検証（2020年～のデータのみ使用）

【目的】
報告書と同じ条件（2020年以降のデータのみ）で検証し、
ROI 113.7%が再現できるか確認する。
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 70)
    print("ROI 113.7% 追加検証（2020年以降のデータのみ使用）")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path("keibaai/data/parsed/parquet")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 2020年以降のデータのみに限定
    races = races[races['race_date'] >= '2020-01-01'].copy()
    
    print(f"\n[データ概要（2020年以降のみ）]")
    print(f"  件数: {len(races):,}")
    print(f"  期間: {races['race_date'].min()} ～ {races['race_date'].max()}")
    
    # 年別レコード数
    print("\n  [年別レコード数]")
    for year in sorted(races['race_date'].dt.year.unique()):
        count = len(races[races['race_date'].dt.year == year])
        print(f"    {year}: {count:,}")
    
    # Test期間の定義（報告書と同じ）
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    results = []
    
    for year, train_end, test_start, test_end in test_periods:
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        if len(train) < 1000 or len(test) < 100:
            print(f"\n  [{year}年] スキップ（データ不足: Train={len(train)}, Test={len(test)}）")
            continue
        
        print(f"\n  [{year}年]")
        print(f"    Train: ～{train_end} ({len(train):,}件)")
        print(f"    Test: {test_start}～{test_end} ({len(test):,}件)")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"    特徴量数: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(params, train_ds, num_boost_round=200)
        
        train_f['pred'] = model.predict(X_train)
        test_f['pred'] = model.predict(X_test)
        
        # Train ROI
        train_f['pred_rank'] = train_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
        train_top1 = train_f[train_f['pred_rank'] == 1]
        train_wins = train_top1[train_top1['finish_position'] == 1]
        train_roi = train_wins['win_odds'].sum() / len(train_top1) * 100 if len(train_top1) > 0 else 0
        
        # Test ROI（ベースライン）
        test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
        test_top1 = test_f[test_f['pred_rank'] == 1]
        test_wins = test_top1[test_top1['finish_position'] == 1]
        baseline_roi = test_wins['win_odds'].sum() / len(test_top1) * 100 if len(test_top1) > 0 else 0
        baseline_hit = len(test_wins) / len(test_top1) * 100 if len(test_top1) > 0 else 0
        
        # Test ROI（オッズ10-50倍）
        odds_mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)
        odds_bets = test_f[odds_mask]
        odds_wins = odds_bets[odds_bets['finish_position'] == 1]
        odds_roi = odds_wins['win_odds'].sum() / len(odds_bets) * 100 if len(odds_bets) > 0 else 0
        odds_hit = len(odds_wins) / len(odds_bets) * 100 if len(odds_bets) > 0 else 0
        
        gap = train_roi - baseline_roi
        
        print(f"    Train ROI: {train_roi:.1f}%")
        print(f"    Test ROI (ベースライン): {baseline_roi:.1f}% (的中率: {baseline_hit:.1f}%)")
        print(f"    Test ROI (オッズ10-50): {odds_roi:.1f}% (的中率: {odds_hit:.1f}%, n={len(odds_bets)})")
        print(f"    Gap: {gap:.1f}%")
        
        results.append({
            'year': year,
            'train_roi': train_roi,
            'baseline_roi': baseline_roi,
            'odds_roi': odds_roi,
            'odds_n': len(odds_bets),
            'gap': gap
        })
    
    # サマリー
    print("\n" + "=" * 70)
    print("検証結果サマリー（2020年以降データのみ）")
    print("=" * 70)
    
    print(f"\n{'年':>6} {'ベースROI':>12} {'オッズ戦略':>12} {'改善幅':>10} {'n':>6} {'Gap':>8}")
    print("-" * 60)
    
    for r in results:
        improvement = r['odds_roi'] - r['baseline_roi']
        print(f"{r['year']:>6} {r['baseline_roi']:>11.1f}% {r['odds_roi']:>11.1f}% {improvement:>+9.1f}% {r['odds_n']:>6} {r['gap']:>7.1f}%")
    
    if results:
        avg_baseline = np.mean([r['baseline_roi'] for r in results])
        avg_odds = np.mean([r['odds_roi'] for r in results])
        avg_gap = np.mean([r['gap'] for r in results])
        
        print("-" * 60)
        print(f"{'平均':>6} {avg_baseline:>11.1f}% {avg_odds:>11.1f}% {avg_odds - avg_baseline:>+9.1f}%        {avg_gap:>7.1f}%")
        
        print("\n" + "=" * 70)
        print("報告値との比較")
        print("=" * 70)
        print(f"\n  報告値: 113.7%")
        print(f"  実測値（2020年～）: {avg_odds:.1f}%")
        print(f"  差異: {avg_odds - 113.7:+.1f}%")


if __name__ == "__main__":
    main()
