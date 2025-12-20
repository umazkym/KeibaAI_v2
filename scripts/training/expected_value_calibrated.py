#!/usr/bin/env python3
"""
予測勝率キャリブレーション版 期待値モデル

問題: 予測勝率がオッズ高馬に過大評価されている
   - オッズ10倍以上で予測40%, 実勝率5% (8倍過大評価)
   
解決策: Plattスケーリング/Isotonic Regressionで予測を校正
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_all_data():
    """全データ読み込み"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[(races['finish_position'].notna()) & (races['finish_position'] > 0)].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    try:
        pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    except:
        pedigrees = None
    
    try:
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    except:
        race_details = None
    
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    
    corners = []
    for corner in [1, 2, 3, 4]:
        col = f'passing_order_{corner}'
        if col in races.columns:
            temp = races[['race_id', 'horse_number', col]].copy()
            temp = temp[temp[col].notna()]
            temp['corner'] = corner
            temp['position'] = temp[col]
            temp['gap_from_leader'] = 0
            corners.append(temp[['race_id', 'horse_number', 'corner', 'position', 'gap_from_leader']])
    
    corners_df = pd.concat(corners, ignore_index=True) if corners else None
    
    return races, pedigrees, corners_df, race_details, returns


def calibrate_platt(raw_probs, y_true):
    """Plattスケーリングで予測を校正"""
    # ロジスティック回帰でキャリブレーション
    lr = LogisticRegression(C=1e10, solver='lbfgs', max_iter=1000)
    X_cal = raw_probs.reshape(-1, 1)
    lr.fit(X_cal, y_true)
    calibrated = lr.predict_proba(X_cal)[:, 1]
    return calibrated, lr


def calibrate_isotonic(raw_probs, y_true):
    """Isotonic Regressionで予測を校正"""
    ir = IsotonicRegression(out_of_bounds='clip')
    ir.fit(raw_probs, y_true)
    calibrated = ir.predict(raw_probs)
    return calibrated, ir


def analyze_calibration(test_feat, prob_col='pred_win_prob', label=''):
    """キャリブレーション分析"""
    bins = [0, 0.02, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0]
    test_feat['prob_bin'] = pd.cut(test_feat[prob_col], bins=bins)
    
    calibration = test_feat.groupby('prob_bin', observed=True).agg({
        'is_win': ['mean', 'count'],
        prob_col: 'mean'
    }).round(4)
    calibration.columns = ['actual_win_rate', 'count', 'pred_prob_mean']
    
    logger.info(f"\n{label} キャリブレーション:")
    logger.info(f"予測ビン        | 予測平均 | 実勝率 | 件数")
    for idx, row in calibration.iterrows():
        logger.info(f"{str(idx):15s} | {row['pred_prob_mean']*100:5.1f}% | {row['actual_win_rate']*100:5.1f}% | {row['count']:5.0f}")


def calc_tansho_roi(pred_df, returns_df, ev_col='expected_value', top_n=1):
    """単勝ROI計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'].copy()
    
    results = []
    for race_id, group in pred_df.groupby('race_id'):
        top_horses = group.nlargest(top_n, ev_col)
        
        race_tansho = tansho[tansho['race_id'] == race_id]
        if len(race_tansho) == 0:
            continue
        
        winning_horse = race_tansho['horse_number'].values[0]
        payout = race_tansho['payout'].values[0]
        
        total_bet = len(top_horses) * 100
        total_return = 0
        
        for _, horse in top_horses.iterrows():
            if horse['horse_number'] == winning_horse:
                total_return += payout
        
        results.append({
            'race_id': race_id,
            'total_bet': total_bet,
            'total_return': total_return,
            'hit': total_return > 0
        })
    
    if not results:
        return {'roi': 0, 'hit_rate': 0, 'n_races': 0}
    
    df = pd.DataFrame(results)
    return {
        'roi': df['total_return'].sum() / df['total_bet'].sum() * 100 if df['total_bet'].sum() > 0 else 0,
        'hit_rate': df['hit'].sum() / len(df) * 100,
        'n_races': len(df)
    }


def run_calibrated_ev_model():
    """キャリブレーション版期待値モデル"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    races, pedigrees, corners_df, race_details, returns = load_all_data()
    
    years = [2022, 2023, 2024]
    all_results = []
    
    for year in years:
        logger.info(f"\n{'='*60}")
        logger.info(f"Year {year}")
        logger.info(f"{'='*60}")
        
        train_end_year = year - 1
        cal_year = year - 1  # キャリブレーション用（テスト前年）
        
        train_start = f"{year - 8}-01-01"
        train_end = f"{train_end_year - 1}-12-31"  # キャリブレーション年を除く
        cal_start = f"{cal_year}-01-01"
        cal_end = f"{cal_year}-12-31"
        test_start = f"{year}-01-01"
        test_end = f"{year}-12-31"
        
        # データ分割
        train_df = races[(races['race_date'] >= train_start) & (races['race_date'] <= train_end)].copy()
        cal_df = races[(races['race_date'] >= cal_start) & (races['race_date'] <= cal_end)].copy()
        test_df = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        test_race_ids = set(test_df['race_id'].unique())
        test_returns = returns[returns['race_id'].isin(test_race_ids)]
        
        logger.info(f"Train: {len(train_df):,}, Cal: {len(cal_df):,}, Test: {len(test_df):,}")
        
        # V17 Feature Engineering
        v17 = LeakFreeFeatureEngineerV17()
        all_train_data = pd.concat([train_df, cal_df], ignore_index=True)
        v17.fit(all_train_data, pedigrees, corners_df, race_details)
        
        train_feat = v17.transform(train_df)
        cal_feat = v17.transform(cal_df)
        test_feat = v17.transform(test_df)
        
        # ターゲット
        train_feat['is_win'] = (train_feat['finish_position'] == 1).astype(int)
        cal_feat['is_win'] = (cal_feat['finish_position'] == 1).astype(int)
        test_feat['is_win'] = (test_feat['finish_position'] == 1).astype(int)
        
        feature_cols = v17.get_feature_columns()
        available_cols = [c for c in feature_cols if c in train_feat.columns]
        
        X_train = train_feat[available_cols].fillna(-999)
        X_cal = cal_feat[available_cols].fillna(-999)
        X_test = test_feat[available_cols].fillna(-999)
        
        y_train = train_feat['is_win']
        y_cal = cal_feat['is_win']
        
        # LightGBMモデル訓練
        params_cls = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 63,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'seed': 42,
            'is_unbalance': True
        }
        
        train_data = lgb.Dataset(X_train, label=y_train)
        model = lgb.train(params_cls, train_data, num_boost_round=500)
        
        # 生の予測
        raw_cal_prob = model.predict(X_cal)
        raw_test_prob = model.predict(X_test)
        test_feat['raw_prob'] = raw_test_prob
        
        # === キャリブレーション ===
        logger.info(f"\n--- キャリブレーション ---")
        
        # 1. Plattスケーリング
        cal_prob_platt, platt_model = calibrate_platt(raw_cal_prob, y_cal)
        test_feat['prob_platt'] = platt_model.predict_proba(raw_test_prob.reshape(-1, 1))[:, 1]
        
        # 2. Isotonic Regression
        cal_prob_iso, iso_model = calibrate_isotonic(raw_cal_prob, y_cal)
        test_feat['prob_isotonic'] = iso_model.predict(raw_test_prob)
        
        # キャリブレーション比較
        analyze_calibration(test_feat.copy(), 'raw_prob', '生の予測')
        analyze_calibration(test_feat.copy(), 'prob_platt', 'Plattスケーリング')
        analyze_calibration(test_feat.copy(), 'prob_isotonic', 'Isotonic')
        
        # === 期待値計算 ===
        # 生の予測
        test_feat['ev_raw'] = test_feat['raw_prob'] * test_feat['win_odds']
        # Platt校正版
        test_feat['ev_platt'] = test_feat['prob_platt'] * test_feat['win_odds']
        # Isotonic校正版
        test_feat['ev_isotonic'] = test_feat['prob_isotonic'] * test_feat['win_odds']
        
        # === 期待値分析 ===
        logger.info(f"\n--- 期待値分布分析 ---")
        
        logger.info(f"生予測 EV: mean={test_feat['ev_raw'].mean():.2f}, >1.0={len(test_feat[test_feat['ev_raw']>=1.0]):,} ({len(test_feat[test_feat['ev_raw']>=1.0])/len(test_feat)*100:.1f}%)")
        logger.info(f"Platt EV: mean={test_feat['ev_platt'].mean():.2f}, >1.0={len(test_feat[test_feat['ev_platt']>=1.0]):,} ({len(test_feat[test_feat['ev_platt']>=1.0])/len(test_feat)*100:.1f}%)")
        logger.info(f"Isotonic EV: mean={test_feat['ev_isotonic'].mean():.2f}, >1.0={len(test_feat[test_feat['ev_isotonic']>=1.0]):,} ({len(test_feat[test_feat['ev_isotonic']>=1.0])/len(test_feat)*100:.1f}%)")
        
        # === ROI計算 ===
        year_results = []
        
        logger.info(f"\n--- 単勝ROI比較 ---")
        for method, ev_col in [('生予測', 'ev_raw'), ('Platt', 'ev_platt'), ('Isotonic', 'ev_isotonic')]:
            for top_n in [1, 2, 3]:
                roi = calc_tansho_roi(test_feat, test_returns, ev_col, top_n)
                logger.info(f"{method} Top{top_n}: ROI {roi['roi']:.1f}%, 的中率 {roi['hit_rate']:.1f}%")
                year_results.append({
                    'year': year,
                    'strategy': f'{method} Top{top_n}',
                    'roi': roi['roi'],
                    'hit_rate': roi['hit_rate'],
                    'n_races': roi['n_races']
                })
        
        # 着順予測Top1（ベースライン）
        test_feat['pred_rank'] = test_feat.groupby('race_id')['raw_prob'].rank(ascending=False, method='first')
        pred_top1 = test_feat[test_feat['pred_rank'] == 1]
        tansho = test_returns[test_returns['bet_type'] == 'tansho']
        pred_top1_merged = pred_top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                           on=['race_id', 'horse_number'], how='left')
        hits = pred_top1_merged['payout'].notna()
        roi_pred = pred_top1_merged['payout'].sum() / (len(pred_top1) * 100) * 100 if len(pred_top1) > 0 else 0
        hit_rate_pred = hits.sum() / len(pred_top1) * 100 if len(pred_top1) > 0 else 0
        logger.info(f"\n着順予測Top1: ROI {roi_pred:.1f}%, 的中率 {hit_rate_pred:.1f}%")
        year_results.append({
            'year': year,
            'strategy': '着順予測Top1',
            'roi': roi_pred,
            'hit_rate': hit_rate_pred,
            'n_races': len(pred_top1)
        })
        
        all_results.extend(year_results)
    
    # 全体サマリー
    logger.info(f"\n{'='*60}")
    logger.info("年度別ROIサマリー（キャリブレーション版）")
    logger.info(f"{'='*60}")
    
    results_df = pd.DataFrame(all_results)
    pivot = results_df.pivot_table(
        index='strategy',
        columns='year',
        values='roi',
        aggfunc='mean'
    )
    pivot['平均'] = pivot.mean(axis=1)
    pivot['標準偏差'] = pivot[[2022, 2023, 2024]].std(axis=1)
    pivot = pivot.sort_values('平均', ascending=False)
    
    print("\n年度別ROI (%):")
    print(pivot.round(1).to_string())
    
    # 最良戦略
    logger.info(f"\n最良戦略: {pivot.index[0]} (平均ROI: {pivot['平均'].iloc[0]:.1f}%)")


if __name__ == "__main__":
    run_calibrated_ev_model()
