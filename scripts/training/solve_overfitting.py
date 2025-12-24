#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4過学習解決策の実装と比較検証

【案1: フィルタリング戦略】
V15の予測結果を使い、「AI順位 < 人気順位」の馬だけ購入

【案2: 残差学習V4.4 v2】
V15が「過小評価」した馬だけをV4.4 v2で学習
target = (actual - v15_pred) * log(1 + odds)

【比較対象】
- V15 Fixed（ベースライン）
- V4.4 オリジナル（過学習問題あり）
- 案1: フィルタリング戦略
- 案2: 残差学習V4.4 v2
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, pred_col):
    """ROIを計算"""
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 70)
    logger.info("V4.4過学習解決策の実装と比較検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 4年Walk-forward検証
    test_years = [2021, 2022, 2023, 2024]
    
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Test Year: {test_year}")
        logger.info("=" * 70)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        if len(train) < 100000 or len(test) < 10000:
            logger.warning(f"  データ不足のためスキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        # 人気を計算
        train_f['popularity'] = train_f.groupby('race_id')['win_odds'].rank(method='first')
        test_f['popularity'] = test_f.groupby('race_id')['win_odds'].rank(method='first')
        
        # ===== V15 Fixed 訓練 =====
        logger.info("  V15 Fixed 訓練中...")
        
        v15_params = {
            'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
            'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
            'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
            'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
        }
        
        y_train = (train_f['finish_position'] == 1).astype(int)
        train_ds = lgb.Dataset(X_train, y_train)
        model_v15 = lgb.train(v15_params, train_ds, num_boost_round=200)
        
        train_f['v15_pred'] = model_v15.predict(X_train)
        test_f['v15_pred'] = model_v15.predict(X_test)
        
        # ===== V4.4 オリジナル 訓練 =====
        logger.info("  V4.4 オリジナル 訓練中...")
        
        v44_params = {
            'objective': 'lambdarank', 'boosting_type': 'gbdt',
            'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
            'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
            'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
            'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
        }
        
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = train_f['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(train_f))
        gain[train_f['finish_position'] == 1] = log_odds[train_f['finish_position'] == 1] * weight_1st
        gain[train_f['finish_position'] == 2] = log_odds[train_f['finish_position'] == 2] * weight_2nd
        gain[train_f['finish_position'] == 3] = log_odds[train_f['finish_position'] == 3] * weight_3rd
        
        train_df = train_f.copy()
        train_df['target'] = gain.astype(int)
        train_df = train_df.sort_values('race_id')
        groups = train_df.groupby('race_id', sort=False).size().to_list()
        
        model_v44_orig = lgb.LGBMRanker(**v44_params, n_estimators=200)
        model_v44_orig.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
        
        train_f['v44_orig_pred'] = model_v44_orig.predict(X_train)
        test_f['v44_orig_pred'] = model_v44_orig.predict(X_test)
        
        # ===== 案1: フィルタリング戦略 =====
        logger.info("  案1: フィルタリング戦略を適用中...")
        
        # V15の順位を計算
        train_f['v15_rank'] = train_f.groupby('race_id')['v15_pred'].rank(ascending=False, method='first')
        test_f['v15_rank'] = test_f.groupby('race_id')['v15_pred'].rank(ascending=False, method='first')
        
        # AI順位 < 人気順位の馬だけに高スコア（人気より高く評価している）
        # それ以外は低スコア
        train_f['filter_score'] = np.where(
            train_f['v15_rank'] < train_f['popularity'],
            train_f['v15_pred'] + 100,  # フィルタを通過した馬は高スコア
            train_f['v15_pred']          # それ以外は元のスコア
        )
        test_f['filter_score'] = np.where(
            test_f['v15_rank'] < test_f['popularity'],
            test_f['v15_pred'] + 100,
            test_f['v15_pred']
        )
        
        # ===== 案2: 残差学習V4.4 v2 =====
        logger.info("  案2: 残差学習V4.4 v2を訓練中...")
        
        # 残差ターゲット: V15が過小評価した馬（実際に勝ったがV15予測が低い）に高スコア
        actual = (train_f['finish_position'] == 1).astype(float)
        residual = actual - train_f['v15_pred']  # V15の予測誤差
        
        # 残差 × オッズ加重 をターゲットに
        # ただし、勝った馬（actual=1）かつV15が低評価（v15_pred < 0.3）の場合に高スコア
        residual_gain = np.zeros(len(train_f))
        residual_gain[train_f['finish_position'] == 1] = residual[train_f['finish_position'] == 1] * log_odds[train_f['finish_position'] == 1] * 10
        residual_gain[train_f['finish_position'] == 2] = residual[train_f['finish_position'] == 2] * log_odds[train_f['finish_position'] == 2] * 5
        residual_gain[train_f['finish_position'] == 3] = residual[train_f['finish_position'] == 3] * log_odds[train_f['finish_position'] == 3] * 2
        
        # 負の値を0に
        residual_gain = np.maximum(residual_gain, 0)
        
        train_residual = train_f.copy()
        train_residual['target'] = residual_gain.astype(int)
        train_residual = train_residual.sort_values('race_id')
        groups_residual = train_residual.groupby('race_id', sort=False).size().to_list()
        
        # V4.4 v2パラメータ（やや強い正則化）
        v44_v2_params = {
            'objective': 'lambdarank', 'boosting_type': 'gbdt',
            'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
            'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
            'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
            'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
        }
        
        model_v44_v2 = lgb.LGBMRanker(**v44_v2_params, n_estimators=150)
        model_v44_v2.fit(train_residual[feature_cols].fillna(0), train_residual['target'], group=groups_residual)
        
        train_f['v44_v2_pred'] = model_v44_v2.predict(X_train)
        test_f['v44_v2_pred'] = model_v44_v2.predict(X_test)
        
        # V15 + V4.4 v2 のアンサンブル
        def normalize(x):
            return (x - x.min()) / (x.max() - x.min() + 1e-8)
        
        train_f['ensemble_v2_pred'] = normalize(train_f['v15_pred']) * 0.5 + normalize(train_f['v44_v2_pred']) * 0.5
        test_f['ensemble_v2_pred'] = normalize(test_f['v15_pred']) * 0.5 + normalize(test_f['v44_v2_pred']) * 0.5
        
        # ===== ROI計算 =====
        train_roi_v15, train_hit_v15, _ = calc_roi(train_f, 'v15_pred')
        test_roi_v15, test_hit_v15, n_bets = calc_roi(test_f, 'v15_pred')
        
        train_roi_v44, train_hit_v44, _ = calc_roi(train_f, 'v44_orig_pred')
        test_roi_v44, test_hit_v44, _ = calc_roi(test_f, 'v44_orig_pred')
        
        train_roi_filter, train_hit_filter, _ = calc_roi(train_f, 'filter_score')
        test_roi_filter, test_hit_filter, n_bets_filter = calc_roi(test_f, 'filter_score')
        
        train_roi_v2, train_hit_v2, _ = calc_roi(train_f, 'ensemble_v2_pred')
        test_roi_v2, test_hit_v2, _ = calc_roi(test_f, 'ensemble_v2_pred')
        
        # 結果まとめ
        results = {
            'year': test_year,
            'n_bets': n_bets,
            'v15_train': train_roi_v15,
            'v15_test': test_roi_v15,
            'v15_gap': train_roi_v15 - test_roi_v15,
            'v44_orig_train': train_roi_v44,
            'v44_orig_test': test_roi_v44,
            'v44_orig_gap': train_roi_v44 - test_roi_v44,
            'filter_train': train_roi_filter,
            'filter_test': test_roi_filter,
            'filter_gap': train_roi_filter - test_roi_filter,
            'v2_train': train_roi_v2,
            'v2_test': test_roi_v2,
            'v2_gap': train_roi_v2 - test_roi_v2,
            'n_bets_filter': n_bets_filter,
        }
        all_results.append(results)
        
        logger.info("")
        logger.info("  結果:")
        logger.info(f"    V15 Fixed:       Train={train_roi_v15:.1f}%, Test={test_roi_v15:.1f}%, Gap={train_roi_v15-test_roi_v15:.1f}%")
        logger.info(f"    V4.4 Orig:       Train={train_roi_v44:.1f}%, Test={test_roi_v44:.1f}%, Gap={train_roi_v44-test_roi_v44:.1f}%")
        logger.info(f"    案1 Filter:      Train={train_roi_filter:.1f}%, Test={test_roi_filter:.1f}%, Gap={train_roi_filter-test_roi_filter:.1f}%")
        logger.info(f"    案2 Residual:    Train={train_roi_v2:.1f}%, Test={test_roi_v2:.1f}%, Gap={train_roi_v2-test_roi_v2:.1f}%")
    
    # ===== サマリー =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("年別結果サマリー")
    logger.info("=" * 70)
    
    logger.info("")
    logger.info("Test ROI比較:")
    logger.info(f"{'年':>6} {'V15 Fixed':>12} {'V4.4 Orig':>12} {'案1 Filter':>12} {'案2 Residual':>12}")
    logger.info("-" * 60)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_test']:>11.1f}% {r['v44_orig_test']:>11.1f}% {r['filter_test']:>11.1f}% {r['v2_test']:>11.1f}%")
    
    if len(all_results) > 1:
        logger.info("-" * 60)
        
        v15_tests = [r['v15_test'] for r in all_results]
        v44_tests = [r['v44_orig_test'] for r in all_results]
        filter_tests = [r['filter_test'] for r in all_results]
        v2_tests = [r['v2_test'] for r in all_results]
        
        logger.info(f"{'平均':>6} {np.mean(v15_tests):>11.1f}% {np.mean(v44_tests):>11.1f}% {np.mean(filter_tests):>11.1f}% {np.mean(v2_tests):>11.1f}%")
        logger.info(f"{'σ':>6} {np.std(v15_tests):>11.1f}% {np.std(v44_tests):>11.1f}% {np.std(filter_tests):>11.1f}% {np.std(v2_tests):>11.1f}%")
    
    # Gap比較
    logger.info("")
    logger.info("Train-Test Gap比較:")
    logger.info(f"{'年':>6} {'V15 Fixed':>12} {'V4.4 Orig':>12} {'案1 Filter':>12} {'案2 Residual':>12}")
    logger.info("-" * 60)
    
    for r in all_results:
        logger.info(f"{r['year']:>6} {r['v15_gap']:>11.1f}% {r['v44_orig_gap']:>11.1f}% {r['filter_gap']:>11.1f}% {r['v2_gap']:>11.1f}%")
    
    if len(all_results) > 1:
        logger.info("-" * 60)
        
        v15_gaps = [r['v15_gap'] for r in all_results]
        v44_gaps = [r['v44_orig_gap'] for r in all_results]
        filter_gaps = [r['filter_gap'] for r in all_results]
        v2_gaps = [r['v2_gap'] for r in all_results]
        
        logger.info(f"{'平均':>6} {np.mean(v15_gaps):>11.1f}% {np.mean(v44_gaps):>11.1f}% {np.mean(filter_gaps):>11.1f}% {np.mean(v2_gaps):>11.1f}%")
    
    # 評価
    logger.info("")
    logger.info("=" * 70)
    logger.info("評価")
    logger.info("=" * 70)
    
    avg_v15 = np.mean([r['v15_test'] for r in all_results])
    avg_v44 = np.mean([r['v44_orig_test'] for r in all_results])
    avg_filter = np.mean([r['filter_test'] for r in all_results])
    avg_v2 = np.mean([r['v2_test'] for r in all_results])
    
    avg_v15_gap = np.mean([r['v15_gap'] for r in all_results])
    avg_v44_gap = np.mean([r['v44_orig_gap'] for r in all_results])
    avg_filter_gap = np.mean([r['filter_gap'] for r in all_results])
    avg_v2_gap = np.mean([r['v2_gap'] for r in all_results])
    
    std_v15 = np.std([r['v15_test'] for r in all_results])
    std_v44 = np.std([r['v44_orig_test'] for r in all_results])
    std_filter = np.std([r['filter_test'] for r in all_results])
    std_v2 = np.std([r['v2_test'] for r in all_results])
    
    logger.info("")
    logger.info("  【総合評価】")
    logger.info(f"  {'モデル':20s} {'平均ROI':>10s} {'平均Gap':>10s} {'安定性σ':>10s}")
    logger.info("-" * 55)
    logger.info(f"  {'V15 Fixed':20s} {avg_v15:>9.1f}% {avg_v15_gap:>9.1f}% {std_v15:>9.1f}%")
    logger.info(f"  {'V4.4 オリジナル':20s} {avg_v44:>9.1f}% {avg_v44_gap:>9.1f}% {std_v44:>9.1f}%")
    logger.info(f"  {'案1: フィルタリング':20s} {avg_filter:>9.1f}% {avg_filter_gap:>9.1f}% {std_filter:>9.1f}%")
    logger.info(f"  {'案2: 残差学習':20s} {avg_v2:>9.1f}% {avg_v2_gap:>9.1f}% {std_v2:>9.1f}%")
    
    # 推奨
    logger.info("")
    logger.info("  【推奨】")
    
    # 最も良いモデルを決定
    best_model = None
    best_score = 0
    
    for name, roi, gap, std in [
        ("V15 Fixed", avg_v15, avg_v15_gap, std_v15),
        ("V4.4 オリジナル", avg_v44, avg_v44_gap, std_v44),
        ("案1: フィルタリング", avg_filter, avg_filter_gap, std_filter),
        ("案2: 残差学習", avg_v2, avg_v2_gap, std_v2),
    ]:
        # スコア = ROI - Gap/2 - std/2 （ROI高く、Gap低く、安定性高いほど良い）
        score = roi - gap/2 - std/2
        if score > best_score:
            best_score = score
            best_model = name
    
    logger.info(f"  最も推奨されるモデル: {best_model}")
    
    # 改善分析
    if avg_filter_gap < avg_v44_gap:
        logger.info(f"  案1（フィルタリング）により、Gap {avg_v44_gap:.1f}% → {avg_filter_gap:.1f}% に改善")
    if avg_v2_gap < avg_v44_gap:
        logger.info(f"  案2（残差学習）により、Gap {avg_v44_gap:.1f}% → {avg_v2_gap:.1f}% に改善")
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("完了")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
