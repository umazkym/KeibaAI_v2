"""
V15特徴量テストスクリプト

目的:
1. V15特徴量が正しく計算されるかを確認
2. リーク検証: Test期間で特徴量が変化しないかを確認
3. V14 vs V15の比較
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses, race_details


def test_feature_generation():
    """特徴量生成テスト"""
    logger.info("=" * 60)
    logger.info("V15 Feature Generation Test")
    logger.info("=" * 60)
    
    train, test, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # V15 fit
    engine = LeakFreeFeatureEngineerV15()
    logger.info("\nFitting V15...")
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    # V15 transform (Test)
    logger.info("\nTransforming Test data...")
    test_f = engine.transform(test)
    
    # 新特徴量の確認
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    logger.info("\n--- V15 New Features Summary ---")
    for col in new_features:
        if col in test_f.columns:
            non_null = test_f[col].notna().sum()
            mean_val = test_f[col].mean()
            std_val = test_f[col].std()
            logger.info(f"{col:30s}: 非NaN={non_null:,}/{len(test_f):,} ({non_null/len(test_f)*100:.1f}%), mean={mean_val:.3f}, std={std_val:.3f}")
        else:
            logger.warning(f"{col}: NOT FOUND")
    
    return engine, test_f


def test_leak_verification(engine, test_df):
    """リーク検証"""
    logger.info("\n" + "=" * 60)
    logger.info("Leak Verification")
    logger.info("=" * 60)
    
    # Test期間内で特定の馬の特徴量が変化しないかを確認
    # （fit時にTrain期間のみで計算しているため、Test期間内で変化してはいけない）
    
    sample_horses = test_df['horse_id'].value_counts().head(5).index.tolist()
    
    for hid in sample_horses:
        horse_data = test_df[test_df['horse_id'] == hid].sort_values('race_date')
        if len(horse_data) < 2:
            continue
        
        # horse_c4_gap_avg が Test期間内で一定であることを確認
        c4_gap_values = horse_data['horse_c4_gap_avg'].dropna().unique()
        if len(c4_gap_values) > 1:
            logger.warning(f"  ⚠ horse_id={hid}: horse_c4_gap_avg changed in Test period! Values: {c4_gap_values}")
        else:
            logger.info(f"  ✓ horse_id={hid}: horse_c4_gap_avg is constant ({c4_gap_values[0] if len(c4_gap_values) > 0 else 'NaN'})")
    
    logger.info("\n[Result] No major leaks detected if all values are constant.")


def test_comparison_with_v14():
    """V14 vs V15 性能比較"""
    logger.info("\n" + "=" * 60)
    logger.info("V14 vs V15 Performance Comparison")
    logger.info("=" * 60)
    
    import lightgbm as lgb
    
    train, test, pedigrees, corners, horses, race_details = load_data()
    
    # 共通ハイパーパラメータ
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
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
    
    for version, EngineClass in [("V14", LeakFreeFeatureEngineerV14), ("V15", LeakFreeFeatureEngineerV15)]:
        logger.info(f"\n--- Training {version} ---")
        
        engine = EngineClass()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        logger.info(f"Features: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        X_test = test_f[feature_cols].fillna(0)
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(params, train_ds, num_boost_round=200)
        
        train_pred = model.predict(X_train)
        test_pred = model.predict(X_test)
        
        # ROI計算
        def calc_roi(df, preds):
            df = df.copy()
            df['pred'] = preds
            df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
            bets = df[df['rank'] == 1]
            hits = bets[bets['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
            hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
            return roi, hit_rate
        
        train_roi, train_hit = calc_roi(train_f, train_pred)
        test_roi, test_hit = calc_roi(test_f, test_pred)
        gap = train_roi - test_roi
        
        logger.info(f"{version}: Train ROI={train_roi:.1f}%, Test ROI={test_roi:.1f}%, Gap={gap:.1f}%")
        
        results.append({
            'version': version,
            'train_roi': train_roi,
            'test_roi': test_roi,
            'gap': gap,
            'train_hit': train_hit,
            'test_hit': test_hit,
            'n_features': len(feature_cols)
        })
        
        # 新特徴量の重要度
        if version == "V15":
            imp = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')})
            imp = imp.sort_values('gain', ascending=False).reset_index(drop=True)
            
            new_cols = ['race_front_runner_count', 'horse_c4_gap_avg', 'post_style_conflict', 'front_runner_competition']
            logger.info("\n--- V15 New Features Importance ---")
            for c in new_cols:
                if c in imp['feature'].values:
                    rank = imp[imp['feature'] == c].index[0] + 1
                    gain = imp[imp['feature'] == c]['gain'].values[0]
                    logger.info(f"  {c}: Rank {rank}/{len(feature_cols)}, Gain {gain:.1f}")
    
    # 比較サマリー
    logger.info("\n" + "=" * 60)
    logger.info("COMPARISON SUMMARY")
    logger.info("=" * 60)
    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    v14_roi = df_results[df_results['version'] == 'V14']['test_roi'].values[0]
    v15_roi = df_results[df_results['version'] == 'V15']['test_roi'].values[0]
    v14_gap = df_results[df_results['version'] == 'V14']['gap'].values[0]
    v15_gap = df_results[df_results['version'] == 'V15']['gap'].values[0]
    
    logger.info(f"\n--- Delta (V15 - V14) ---")
    logger.info(f"Test ROI: {v15_roi - v14_roi:+.1f}%")
    logger.info(f"Gap:      {v15_gap - v14_gap:+.1f}%")
    
    if v15_roi > v14_roi and v15_gap < 50:
        logger.info("\n✅ V15 improves Test ROI without excessive overfitting!")
    elif v15_roi > v14_roi:
        logger.info("\n⚠️ V15 improves Test ROI but Gap increased - monitor overfitting!")
    else:
        logger.info("\n❌ V15 did not improve Test ROI")


def main():
    # 1. 特徴量生成テスト
    engine, test_f = test_feature_generation()
    
    # 2. リーク検証
    test_leak_verification(engine, test_f)
    
    # 3. V14 vs V15 比較
    test_comparison_with_v14()


if __name__ == "__main__":
    main()
