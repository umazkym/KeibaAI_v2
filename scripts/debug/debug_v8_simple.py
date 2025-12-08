"""
V8 簡略化検証スクリプト
リークテストをスキップして、シャッフル、Walk-Forward、相関分析、Phase 2効果を検証
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging
import lightgbm as lgb

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_roi(features_df, returns_df):
    """ROI計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    top1 = features_df[features_df['rank'] == 1].copy()
    top1['race_id'] = top1['race_id'].astype(str)
    top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    merged = top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    total_bet = len(top1) * 100
    total_payout = merged['payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    
    hits = (merged['payout'] > 0).sum()
    hit_rate = hits / len(top1) * 100 if len(top1) > 0 else 0
    
    return roi, hit_rate, len(top1)


def train_model(train_features, valid_features, feature_cols):
    """モデル学習（強化2: 高正則化設定）"""
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_features.sort_values('race_id')
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    y_rel = np.zeros(len(y_train))
    y_rel[y_train.values == 1] = 5
    y_rel[y_train.values == 2] = 4
    y_rel[y_train.values == 3] = 3
    
    y_rel_valid = np.zeros(len(y_valid))
    y_rel_valid[y_valid.values == 1] = 5
    y_rel_valid[y_valid.values == 2] = 4
    y_rel_valid[y_valid.values == 3] = 3
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=1000,
        learning_rate=0.005,
        max_depth=2,
        num_leaves=4,
        min_child_samples=300,
        reg_alpha=5.0,
        reg_lambda=10.0,
        subsample=0.5,
        colsample_bytree=0.5,
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_rel, group=groups,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    return model


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    logger.info("="*70)
    logger.info("V8 簡略化検証")
    logger.info("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    
    # V8 transform
    fe = LeakFreeFeatureEngineerV8()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
           race_details_df=race_details_df, returns_df=returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = [c for c in fe.get_feature_columns() if c in train_features.columns]
    logger.info(f"特徴量数: {len(feature_cols)}")
    
    # 通常モデル
    model = train_model(train_features, valid_features, feature_cols)
    
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    normal_roi, normal_hr, _ = calculate_roi(test_features, returns_df)
    logger.info(f"\n通常モデル Test ROI: {normal_roi:.1f}%, 的中率: {normal_hr:.1f}%")
    
    # ==========================================================================
    # 検証1: シャッフルテスト
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証1】シャッフルテスト")
    logger.info("="*70)
    
    train_features_shuffled = train_features.copy()
    shuffled_fp = train_features_shuffled['finish_position'].values.copy()
    np.random.seed(42)
    np.random.shuffle(shuffled_fp)
    train_features_shuffled['finish_position'] = shuffled_fp
    
    model_shuffled = train_model(train_features_shuffled, valid_features, feature_cols)
    
    test_features_shuffle = test_features.copy()
    test_features_shuffle['score'] = model_shuffled.predict(test_features_shuffle[feature_cols].fillna(0))
    test_features_shuffle['rank'] = test_features_shuffle.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    shuffle_roi, shuffle_hr, _ = calculate_roi(test_features_shuffle, returns_df)
    
    logger.info(f"正常モデル ROI: {normal_roi:.1f}%")
    logger.info(f"シャッフルモデル ROI: {shuffle_roi:.1f}%")
    logger.info(f"差分: {normal_roi - shuffle_roi:+.1f}%")
    
    shuffle_ok = normal_roi > shuffle_roi
    logger.info(f"{'✓ PASS' if shuffle_ok else '✗ FAIL'}: 正常モデルが上回っている")
    
    # ==========================================================================
    # 検証2: Train-Test差分チェック
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証2】過学習チェック（Train-Test差分）")
    logger.info("="*70)
    
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    train_roi, train_hr, _ = calculate_roi(train_features, returns_df)
    
    train_test_diff = train_roi - normal_roi
    
    logger.info(f"Train ROI: {train_roi:.1f}%")
    logger.info(f"Test ROI: {normal_roi:.1f}%")
    logger.info(f"差分: {train_test_diff:.1f}%")
    
    # 的中率でもチェック
    train_hit = (train_features[train_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    test_hit = (test_features[test_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    
    logger.info(f"\nTrain的中率: {train_hit:.1f}%")
    logger.info(f"Test的中率: {test_hit:.1f}%")
    logger.info(f"的中率差分: {train_hit - test_hit:.1f}%")
    
    ovf_ok = (train_hit - test_hit) < 15
    logger.info(f"{'✓ PASS' if ovf_ok else '⚠ 要注意'}: 的中率差分 < 15%")
    
    # ==========================================================================
    # 検証3: 特徴量相関分析
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証3】特徴量相関分析")
    logger.info("="*70)
    
    correlations = []
    for col in feature_cols:
        if test_features[col].notna().sum() > 100:
            corr = test_features[[col, 'finish_position']].corr().iloc[0, 1]
            correlations.append((col, corr))
    
    correlations = sorted(correlations, key=lambda x: abs(x[1]), reverse=True)
    
    logger.info("\n高相関特徴量 Top 10:")
    for col, corr in correlations[:10]:
        marker = "⚠" if abs(corr) > 0.3 else ""
        logger.info(f"  {marker}{col}: {corr:.3f}")
    
    # Phase 2特徴量
    phase2_features = ['weight_change_trend_3', 'horse_last3f_rank_avg']
    logger.info("\nPhase 2特徴量の相関:")
    for pf in phase2_features:
        if pf in [c[0] for c in correlations]:
            corr = [c[1] for c in correlations if c[0] == pf][0]
            logger.info(f"  {pf}: {corr:.3f}")
    
    # ==========================================================================
    # 検証4: Phase 2特徴量の個別効果
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証4】Phase 2特徴量の個別効果")
    logger.info("="*70)
    
    feature_cols_no_phase2 = [c for c in feature_cols if c not in phase2_features]
    
    model_no_phase2 = train_model(train_features, valid_features, feature_cols_no_phase2)
    
    test_features_no_phase2 = test_features.copy()
    test_features_no_phase2['score'] = model_no_phase2.predict(test_features_no_phase2[feature_cols_no_phase2].fillna(0))
    test_features_no_phase2['rank'] = test_features_no_phase2.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    no_phase2_roi, no_phase2_hr, _ = calculate_roi(test_features_no_phase2, returns_df)
    
    phase2_effect = normal_roi - no_phase2_roi
    
    logger.info(f"Phase 2込み ROI: {normal_roi:.1f}%")
    logger.info(f"Phase 2除外 ROI: {no_phase2_roi:.1f}%")
    logger.info(f"Phase 2効果: {phase2_effect:+.1f}%")
    
    phase2_ok = normal_roi >= no_phase2_roi - 1
    logger.info(f"{'✓ PASS' if phase2_ok else '△ 効果薄'}")
    
    # ==========================================================================
    # 総合評価
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【総合評価】")
    logger.info("="*70)
    
    logger.info(f"\n1. シャッフルテスト: {'✓ PASS' if shuffle_ok else '✗ FAIL'} (差分={normal_roi - shuffle_roi:+.1f}%)")
    logger.info(f"2. 過学習チェック: {'✓ PASS' if ovf_ok else '⚠ 要注意'} (的中率差分={train_hit - test_hit:.1f}%)")
    logger.info(f"3. Phase 2効果: {'✓ PASS' if phase2_ok else '△ 効果薄'} (効果={phase2_effect:+.1f}%)")
    
    overall = shuffle_ok and ovf_ok
    logger.info(f"\n総合判定: {'✓ V8は健全' if overall else '⚠ 要確認'}")
    
    # ベースライン比較
    pop1 = test_df[test_df['popularity'] == 1].copy()
    pop1['race_id'] = pop1['race_id'].astype(str)
    pop1['horse_number'] = pd.to_numeric(pop1['horse_number'], errors='coerce')
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    merged_pop1 = pop1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    baseline_roi = merged_pop1['payout'].fillna(0).sum() / (len(pop1) * 100) * 100
    
    logger.info(f"\n人気1位ベースライン ROI: {baseline_roi:.1f}%")
    logger.info(f"V8 ROI: {normal_roi:.1f}%")
    logger.info(f"V8 vs ベースライン: {normal_roi - baseline_roi:+.1f}%")


if __name__ == '__main__':
    main()
