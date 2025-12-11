# -*- coding: utf-8 -*-
"""
オッズ帯戦略の複数年検証

発見した「オッズ10-50倍の予測1位のみ購入」戦略を
2022～2025の4年間で検証し、堅牢性を確認する。
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def evaluate_strategies(test_f):
    """各戦略のROIを計算"""
    test_f = test_f.copy()
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    results = {}
    
    # ベースライン
    mask = test_f['pred_rank'] == 1
    bets = test_f[mask]
    wins = bets[bets['finish_position'] == 1]
    results['baseline'] = {
        'roi': wins['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'hit': len(wins) / len(bets) * 100 if len(bets) > 0 else 0,
        'n': len(bets)
    }
    
    # オッズ10-50倍
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)
    bets = test_f[mask]
    wins = bets[bets['finish_position'] == 1]
    results['odds_10_50'] = {
        'roi': wins['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'hit': len(wins) / len(bets) * 100 if len(bets) > 0 else 0,
        'n': len(bets)
    }
    
    # オッズ20-50倍
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 20) & (test_f['win_odds'] < 50)
    bets = test_f[mask]
    wins = bets[bets['finish_position'] == 1]
    results['odds_20_50'] = {
        'roi': wins['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'hit': len(wins) / len(bets) * 100 if len(bets) > 0 else 0,
        'n': len(bets)
    }
    
    # オッズ10-30倍（中間帯）
    mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 30)
    bets = test_f[mask]
    wins = bets[bets['finish_position'] == 1]
    results['odds_10_30'] = {
        'roi': wins['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'hit': len(wins) / len(bets) * 100 if len(bets) > 0 else 0,
        'n': len(bets)
    }
    
    return results


def main():
    logger.info("=" * 60)
    logger.info("オッズ帯戦略の複数年検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 年別のテスト期間を定義
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
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
    
    for year, train_end, test_start, test_end in test_periods:
        logger.info("")
        logger.info(f"=" * 60)
        logger.info(f"{year} Test (Train~{train_end})")
        logger.info(f"=" * 60)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}件, Test: {len(test):,}件")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(params, train_ds, num_boost_round=200)
        test_f['pred'] = model.predict(X_test)
        
        results = evaluate_strategies(test_f)
        results['year'] = year
        all_results.append(results)
        
        logger.info(f"\n  {'戦略':12} {'ROI':>8} {'的中率':>8} {'n':>6}")
        logger.info("  " + "-" * 40)
        logger.info(f"  {'ベースライン':12} {results['baseline']['roi']:>7.1f}% {results['baseline']['hit']:>7.1f}% {results['baseline']['n']:>6,}")
        logger.info(f"  {'オッズ10-50':12} {results['odds_10_50']['roi']:>7.1f}% {results['odds_10_50']['hit']:>7.1f}% {results['odds_10_50']['n']:>6,}")
        logger.info(f"  {'オッズ20-50':12} {results['odds_20_50']['roi']:>7.1f}% {results['odds_20_50']['hit']:>7.1f}% {results['odds_20_50']['n']:>6,}")
        logger.info(f"  {'オッズ10-30':12} {results['odds_10_30']['roi']:>7.1f}% {results['odds_10_30']['hit']:>7.1f}% {results['odds_10_30']['n']:>6,}")
    
    # サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("年別・戦略別ROIサマリー")
    logger.info("=" * 60)
    
    logger.info(f"\n  {'年':6} {'ベースライン':>12} {'オッズ10-50':>12} {'オッズ20-50':>12} {'オッズ10-30':>12}")
    logger.info("  " + "-" * 60)
    
    for r in all_results:
        logger.info(f"  {r['year']:6} {r['baseline']['roi']:>11.1f}% {r['odds_10_50']['roi']:>11.1f}% {r['odds_20_50']['roi']:>11.1f}% {r['odds_10_30']['roi']:>11.1f}%")
    
    # 平均
    avg_baseline = np.mean([r['baseline']['roi'] for r in all_results])
    avg_10_50 = np.mean([r['odds_10_50']['roi'] for r in all_results])
    avg_20_50 = np.mean([r['odds_20_50']['roi'] for r in all_results])
    avg_10_30 = np.mean([r['odds_10_30']['roi'] for r in all_results])
    
    logger.info("  " + "-" * 60)
    logger.info(f"  {'平均':6} {avg_baseline:>11.1f}% {avg_10_50:>11.1f}% {avg_20_50:>11.1f}% {avg_10_30:>11.1f}%")
    
    logger.info("")
    if avg_10_50 > avg_baseline:
        logger.info(f"  ✅ オッズ10-50戦略は年間平均で+{avg_10_50 - avg_baseline:.1f}%の改善!")
    else:
        logger.info(f"  ❌ オッズ10-50戦略は年間平均で{avg_10_50 - avg_baseline:.1f}%悪化")


if __name__ == "__main__":
    main()
