"""
V8 徹底的リーク・過学習検証

【検証項目】
1. リークテスト: finish_positionを隠した場合のROI差
2. シャッフルテスト: ターゲット変数をシャッフルした場合のROI
3. Walk-Forward検証: 複数期間での安定性
4. 特徴量相関分析: 各特徴量とfinish_positionの相関
5. Phase 2特徴量の個別效果: 新特徴量を除外した場合のROI
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
    
    # Relevanceスコア
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
    logger.info("V8 徹底的リーク・過学習検証")
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
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"Train: {len(train_df):,}行")
    logger.info(f"Valid: {len(valid_df):,}行")
    logger.info(f"Test: {len(test_df):,}行")
    
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    
    # ==========================================================================
    # 検証1: リークテスト（finish_positionを隠す）
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証1】リークテスト")
    logger.info("="*70)
    
    # 通常のV8
    fe = LeakFreeFeatureEngineerV8()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
           race_details_df=race_details_df, returns_df=returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = [c for c in fe.get_feature_columns() if c in train_features.columns]
    logger.info(f"特徴量数: {len(feature_cols)}")
    
    model = train_model(train_features, valid_features, feature_cols)
    
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    normal_roi, normal_hr, _ = calculate_roi(test_features, returns_df)
    
    # finish_positionを隠したV8
    test_df_hidden = test_df.copy()
    test_df_hidden['finish_position_orig'] = test_df_hidden['finish_position']
    test_df_hidden['finish_position'] = np.nan
    
    fe2 = LeakFreeFeatureEngineerV8()
    fe2.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
            race_details_df=race_details_df, returns_df=returns_df)
    test_features_hidden = fe2.transform(test_df_hidden)
    
    # キーでfinish_positionを復元
    fp_lookup = test_df_hidden[['race_id', 'horse_number', 'finish_position_orig']].copy()
    fp_lookup['race_id'] = fp_lookup['race_id'].astype(str)
    fp_lookup['horse_number'] = pd.to_numeric(fp_lookup['horse_number'], errors='coerce')
    test_features_hidden['race_id'] = test_features_hidden['race_id'].astype(str)
    test_features_hidden['horse_number'] = pd.to_numeric(test_features_hidden['horse_number'], errors='coerce')
    test_features_hidden = test_features_hidden.merge(
        fp_lookup, on=['race_id', 'horse_number'], how='left'
    )
    test_features_hidden['finish_position'] = test_features_hidden['finish_position_orig']
    test_features_hidden = test_features_hidden.drop(columns=['finish_position_orig'], errors='ignore')
    
    test_features_hidden['score'] = model.predict(test_features_hidden[feature_cols].fillna(0))
    test_features_hidden['rank'] = test_features_hidden.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    hidden_roi, hidden_hr, _ = calculate_roi(test_features_hidden, returns_df)
    
    leak_diff = normal_roi - hidden_roi
    logger.info(f"\n通常Test ROI: {normal_roi:.1f}%")
    logger.info(f"隠しTest ROI: {hidden_roi:.1f}%")
    logger.info(f"差分: {leak_diff:+.1f}%")
    
    if abs(leak_diff) < 5:
        logger.info("✓ リークなし")
    else:
        logger.warning("⚠ リークの可能性あり")
    
    # ==========================================================================
    # 検証2: シャッフルテスト
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証2】シャッフルテスト")
    logger.info("="*70)
    
    # シャッフルしたターゲットで学習
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
    
    logger.info(f"\n正常モデルTest ROI: {normal_roi:.1f}%")
    logger.info(f"シャッフルモデルTest ROI: {shuffle_roi:.1f}%")
    logger.info(f"差分: {normal_roi - shuffle_roi:+.1f}%")
    
    if normal_roi > shuffle_roi:
        logger.info(f"✓ 正常モデルがシャッフルを{normal_roi - shuffle_roi:.1f}%上回る")
    else:
        logger.warning("⚠ 正常モデルがシャッフル以下（問題あり）")
    
    # ==========================================================================
    # 検証3: Walk-Forward検証
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証3】Walk-Forward検証")
    logger.info("="*70)
    
    # 複数期間でのテスト
    test_periods = [
        ('2024-01', '2024-06', '2020-01-01', '2023-12-31'),  # 2024前半
        ('2024-07', '2024-12', '2020-01-01', '2024-06-30'),  # 2024後半
        ('2025-01', '2025-06', '2020-01-01', '2024-12-31'),  # 2025前半
        ('2025-07', '2025-10', '2020-01-01', '2025-06-30'),  # 2025後半
    ]
    
    wf_results = []
    for test_start, test_end, train_start, train_end in test_periods:
        # 期間フィルタ
        train_wf = races_df[(races_df['race_date'] >= train_start) & 
                           (races_df['race_date'] <= train_end)].copy()
        valid_wf = races_df[(races_df['race_date'] > train_end) & 
                           (races_df['race_date'] < test_start + '-01')].copy()
        test_wf = races_df[(races_df['race_date'] >= test_start + '-01') & 
                          (races_df['race_date'] <= test_end + '-31')].copy()
        
        if len(test_wf) < 100:
            continue
        
        if len(valid_wf) < 100:
            valid_wf = train_wf.tail(int(len(train_wf) * 0.1))
        
        # fit & transform
        fe_wf = LeakFreeFeatureEngineerV8()
        fe_wf.fit(races_df=train_wf, pedigrees_df=pedigrees_df, corners_df=corners_df,
                  race_details_df=race_details_df, returns_df=returns_df)
        
        train_feat = fe_wf.transform(train_wf)
        valid_feat = fe_wf.transform(valid_wf)
        test_feat = fe_wf.transform(test_wf)
        
        feat_cols = [c for c in fe_wf.get_feature_columns() if c in train_feat.columns]
        
        model_wf = train_model(train_feat, valid_feat, feat_cols)
        
        test_feat['score'] = model_wf.predict(test_feat[feat_cols].fillna(0))
        test_feat['rank'] = test_feat.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        roi, hr, n = calculate_roi(test_feat, returns_df)
        wf_results.append({'period': f"{test_start}~{test_end}", 'roi': roi, 'hr': hr, 'n': n})
    
    logger.info("\nWalk-Forward結果:")
    for r in wf_results:
        status = "✓" if r['roi'] > 75 else "△" if r['roi'] > 70 else "✗"
        logger.info(f"  {r['period']}: ROI={r['roi']:.1f}%, 的中率={r['hr']:.1f}%, n={r['n']}, {status}")
    
    avg_roi = np.mean([r['roi'] for r in wf_results])
    std_roi = np.std([r['roi'] for r in wf_results])
    logger.info(f"\n平均ROI: {avg_roi:.1f}% ± {std_roi:.1f}%")
    
    # ==========================================================================
    # 検証4: 特徴量相関分析
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証4】特徴量相関分析")
    logger.info("="*70)
    
    correlations = []
    for col in feature_cols:
        if test_features[col].notna().sum() > 100:
            corr = test_features[[col, 'finish_position']].corr().iloc[0, 1]
            correlations.append((col, corr))
    
    correlations = sorted(correlations, key=lambda x: abs(x[1]), reverse=True)
    
    logger.info("\n高相関特徴量（|r| > 0.1）:")
    high_corr = [c for c in correlations if abs(c[1]) > 0.1]
    for col, corr in high_corr[:10]:
        marker = "⚠" if abs(corr) > 0.3 else ""
        logger.info(f"  {marker}{col}: {corr:.3f}")
    
    # Phase 2特徴量の相関
    logger.info("\nPhase 2特徴量の相関:")
    phase2_features = ['weight_change_trend_3', 'horse_last3f_rank_avg']
    for pf in phase2_features:
        if pf in test_features.columns:
            corr = test_features[[pf, 'finish_position']].corr().iloc[0, 1]
            logger.info(f"  {pf}: {corr:.3f}")
    
    # ==========================================================================
    # 検証5: Phase 2特徴量の個別効果
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【検証5】Phase 2特徴量の個別効果")
    logger.info("="*70)
    
    # Phase 2特徴量を除外したモデル
    feature_cols_no_phase2 = [c for c in feature_cols if c not in phase2_features]
    
    model_no_phase2 = train_model(train_features, valid_features, feature_cols_no_phase2)
    
    test_features_no_phase2 = test_features.copy()
    test_features_no_phase2['score'] = model_no_phase2.predict(test_features_no_phase2[feature_cols_no_phase2].fillna(0))
    test_features_no_phase2['rank'] = test_features_no_phase2.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    no_phase2_roi, no_phase2_hr, _ = calculate_roi(test_features_no_phase2, returns_df)
    
    logger.info(f"\nPhase 2込みTest ROI: {normal_roi:.1f}%")
    logger.info(f"Phase 2除外Test ROI: {no_phase2_roi:.1f}%")
    logger.info(f"Phase 2の効果: {normal_roi - no_phase2_roi:+.1f}%")
    
    # ==========================================================================
    # 総合評価
    # ==========================================================================
    logger.info("\n" + "="*70)
    logger.info("【総合評価】")
    logger.info("="*70)
    
    # 判定基準
    leak_ok = abs(leak_diff) < 5
    shuffle_ok = normal_roi > shuffle_roi
    wf_ok = avg_roi > 70 and std_roi < 20
    phase2_ok = normal_roi >= no_phase2_roi
    
    logger.info(f"\n1. リークテスト: {'✓ PASS' if leak_ok else '✗ FAIL'} (差分={leak_diff:+.1f}%)")
    logger.info(f"2. シャッフルテスト: {'✓ PASS' if shuffle_ok else '✗ FAIL'} (差分={normal_roi - shuffle_roi:+.1f}%)")
    logger.info(f"3. Walk-Forward: {'✓ PASS' if wf_ok else '✗ FAIL'} (平均={avg_roi:.1f}% ± {std_roi:.1f}%)")
    logger.info(f"4. Phase 2効果: {'✓ PASS' if phase2_ok else '△ 効果なし'} (効果={normal_roi - no_phase2_roi:+.1f}%)")
    
    overall = leak_ok and shuffle_ok and wf_ok
    logger.info(f"\n総合判定: {'✓ V8は健全' if overall else '⚠ 要確認'}")


if __name__ == '__main__':
    main()
