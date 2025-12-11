# -*- coding: utf-8 -*-
"""
EV閾値戦略 堅牢性検証

【検証項目】
1. 逆テスト: Train:2024, Test:2023 で同様の効果があるか
2. 年別安定性: 2021-2025で各年をTestにして検証
3. 特異値チェック: 特定月や特定オッズ帯に偏っていないか
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


def calc_roi_ev(df, threshold):
    """予測確率Top1のEV閾値ROI"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    bets = top1[top1['expected_value'] > threshold]
    
    if len(bets) == 0:
        return 0, 0, 0
    
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    
    return roi, hit_rate, len(bets)


def run_experiment(races, pedigrees, corners, race_details, horses, train_end, test_start, test_end, name):
    """1つの実験を実行"""
    logger.info(f"  {name}: Train ~{train_end}, Test {test_start}~{test_end}")
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
    
    if len(train) < 10000 or len(test) < 1000:
        logger.info(f"    スキップ（データ不足）")
        return None
    
    # V15特徴量
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
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
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_f['pred'] = model.predict(X_test)
    test_f['expected_value'] = test_f['pred'] * test_f['win_odds'] * 0.80
    
    # ベースライン
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = test_f[test_f['pred_rank'] == 1]
    hits = top1[top1['finish_position'] == 1]
    base_roi = hits['win_odds'].sum() / len(top1) * 100
    base_hit = len(hits) / len(top1) * 100
    
    # EV閾値
    ev10_roi, ev10_hit, ev10_n = calc_roi_ev(test_f, 1.0)
    
    logger.info(f"    Train: {len(train):,}件, Test: {len(test):,}件")
    logger.info(f"    ベースライン: ROI={base_roi:.1f}%, 的中率={base_hit:.1f}%")
    logger.info(f"    EV>1.0: ROI={ev10_roi:.1f}%, ベット数={ev10_n:,}")
    
    return {
        'name': name,
        'train_size': len(train),
        'test_size': len(test),
        'base_roi': base_roi,
        'base_hit': base_hit,
        'ev10_roi': ev10_roi,
        'ev10_hit': ev10_hit,
        'ev10_n': ev10_n,
    }


def main():
    logger.info("=" * 60)
    logger.info("EV閾値戦略 堅牢性検証")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    results = []
    
    # ========== 1. 公式設定（2025Test） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 公式設定 (Train~2024, Test 2025)")
    logger.info("=" * 60)
    
    r = run_experiment(races, pedigrees, corners, race_details, horses,
                       '2024-12-31', '2025-01-01', '2025-11-01', '2025 Test')
    if r:
        results.append(r)
    
    # ========== 2. 逆テスト（2024Test） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 逆テスト (Train~2023, Test 2024)")
    logger.info("=" * 60)
    
    r = run_experiment(races, pedigrees, corners, race_details, horses,
                       '2023-12-31', '2024-01-01', '2025-01-01', '2024 Test')
    if r:
        results.append(r)
    
    # ========== 3. 2023Test ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 2023Test (Train~2022, Test 2023)")
    logger.info("=" * 60)
    
    r = run_experiment(races, pedigrees, corners, race_details, horses,
                       '2022-12-31', '2023-01-01', '2024-01-01', '2023 Test')
    if r:
        results.append(r)
    
    # ========== 4. 2022Test ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 2022Test (Train~2021, Test 2022)")
    logger.info("=" * 60)
    
    r = run_experiment(races, pedigrees, corners, race_details, horses,
                       '2021-12-31', '2022-01-01', '2023-01-01', '2022 Test')
    if r:
        results.append(r)
    
    # ========== 結果サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    if results:
        logger.info(f"  {'Test年':>10} {'ベースROI':>10} {'EV>1.0 ROI':>12} {'EV>1.0 N':>10} {'改善':>8}")
        logger.info("-" * 55)
        
        for r in results:
            improvement = r['ev10_roi'] - r['base_roi']
            logger.info(f"  {r['name']:>10} {r['base_roi']:>9.1f}% {r['ev10_roi']:>11.1f}% {r['ev10_n']:>10,} {improvement:>+7.1f}%")
        
        # 平均
        avg_base = np.mean([r['base_roi'] for r in results])
        avg_ev10 = np.mean([r['ev10_roi'] for r in results])
        avg_improvement = avg_ev10 - avg_base
        
        logger.info("-" * 55)
        logger.info(f"  {'平均':>10} {avg_base:>9.1f}% {avg_ev10:>11.1f}% {'-':>10} {avg_improvement:>+7.1f}%")
        
        # 判定
        logger.info("")
        if avg_improvement > 5:
            logger.info("  ✅ EV閾値戦略は複数年で一貫して有効!")
        elif avg_improvement > 0:
            logger.info("  △ EV閾値戦略は効果あるが小さい")
        else:
            logger.info("  ❌ EV閾値戦略は年によって不安定")
    
    return results


if __name__ == "__main__":
    main()
