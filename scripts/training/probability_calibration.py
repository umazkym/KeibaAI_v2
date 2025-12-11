# -*- coding: utf-8 -*-
"""
Phase 4 #13: 確率キャリブレーション

LightGBMの予測確率を実際の勝率に近づけるためのキャリブレーション。
Isotonic Regressionを使用。

【背景】
- LightGBMの出力は「確率」ではなく「スコア」
- 高いスコアほど勝率が高いが、数値が実際の勝率と一致しない
- キャリブレーションで予測確率を実勝率に補正

【効果】
- EV計算の精度向上
- より正確な期待値判断が可能
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import calibration_curve
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


def calc_roi_ev_threshold(df, threshold):
    """予測確率Top1馬を選択し、EV閾値でフィルタしてROI計算"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['calibrated_prob'].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    
    bets = top1[top1['expected_value'] > threshold]
    
    if len(bets) == 0:
        return 0, 0, 0
    
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("Phase 4 #13: 確率キャリブレーション")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 期間設定（キャリブレーション用にValid期間を設ける）
    train = races[races['race_date'] <= '2023-12-31'].copy()
    valid = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] <= '2024-12-31')].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2023-12-31)")
    logger.info(f"  Valid: {len(valid):,}件 (2024-01-01~2024-12-31) [キャリブレーション用]")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # モデル学習
    logger.info("")
    logger.info("V15モデル学習中...")
    
    X_train = train_f[feature_cols].fillna(0)
    X_valid = valid_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    y_train = (train_f['finish_position'] == 1).astype(int)
    y_valid = (valid_f['finish_position'] == 1).astype(int)
    y_test = (test_f['finish_position'] == 1).astype(int)
    
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
    
    # 生の予測確率
    raw_valid = model.predict(X_valid)
    raw_test = model.predict(X_test)
    
    # ========== キャリブレーション前の確認 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("キャリブレーション前")
    logger.info("=" * 60)
    
    # 予測確率の分布
    logger.info(f"  Valid予測確率: min={raw_valid.min():.4f}, max={raw_valid.max():.4f}, mean={raw_valid.mean():.4f}")
    logger.info(f"  Valid実勝率: {y_valid.mean():.4f}")
    
    # キャリブレーションカーブ
    prob_true, prob_pred = calibration_curve(y_valid, raw_valid, n_bins=10, strategy='uniform')
    
    logger.info("")
    logger.info("  キャリブレーションカーブ (Before):")
    for pt, pp in zip(prob_true, prob_pred):
        diff = pt - pp
        logger.info(f"    予測={pp:.3f} -> 実際={pt:.3f} (差={diff:+.3f})")
    
    # ========== Isotonic Regression ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("Isotonic Regressionでキャリブレーション")
    logger.info("=" * 60)
    
    iso_reg = IsotonicRegression(out_of_bounds='clip')
    iso_reg.fit(raw_valid, y_valid)
    
    # キャリブレーション適用
    cal_valid = iso_reg.predict(raw_valid)
    cal_test = iso_reg.predict(raw_test)
    
    # キャリブレーション後の確認
    logger.info(f"  Valid補正確率: min={cal_valid.min():.4f}, max={cal_valid.max():.4f}, mean={cal_valid.mean():.4f}")
    
    # キャリブレーションカーブ（After）
    prob_true_cal, prob_pred_cal = calibration_curve(y_valid, cal_valid, n_bins=10, strategy='uniform')
    
    logger.info("")
    logger.info("  キャリブレーションカーブ (After):")
    for pt, pp in zip(prob_true_cal, prob_pred_cal):
        diff = pt - pp
        logger.info(f"    予測={pp:.3f} -> 実際={pt:.3f} (差={diff:+.3f})")
    
    # ========== EV閾値戦略比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("EV閾値戦略比較")
    logger.info("=" * 60)
    
    # 生の確率でのEV
    test_f['raw_prob'] = raw_test
    test_f['calibrated_prob'] = cal_test
    
    # 生確率版
    test_f['expected_value_raw'] = test_f['raw_prob'] * test_f['win_odds'] * 0.80
    # キャリブレーション版
    test_f['expected_value'] = test_f['calibrated_prob'] * test_f['win_odds'] * 0.80
    
    thresholds = [0.8, 0.9, 1.0, 1.1, 1.2, 1.3]
    
    logger.info("")
    logger.info("【生の確率】")
    test_raw = test_f.copy()
    test_raw['calibrated_prob'] = test_raw['raw_prob']
    test_raw['expected_value'] = test_raw['expected_value_raw']
    
    for th in thresholds:
        roi, hit, n_bets = calc_roi_ev_threshold(test_raw, th)
        if n_bets > 0:
            logger.info(f"  EV > {th:.1f}: ベット={n_bets:,}, 的中率={hit:.1f}%, ROI={roi:.1f}%")
    
    logger.info("")
    logger.info("【キャリブレーション後】")
    for th in thresholds:
        roi, hit, n_bets = calc_roi_ev_threshold(test_f, th)
        if n_bets > 0:
            logger.info(f"  EV > {th:.1f}: ベット={n_bets:,}, 的中率={hit:.1f}%, ROI={roi:.1f}%")
    
    # ========== 結果サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    # EV > 1.0 での比較
    roi_raw, hit_raw, n_raw = calc_roi_ev_threshold(test_raw, 1.0)
    roi_cal, hit_cal, n_cal = calc_roi_ev_threshold(test_f, 1.0)
    
    logger.info(f"  EV > 1.0 比較:")
    logger.info(f"    生の確率:      ROI={roi_raw:.1f}%, ベット数={n_raw:,}")
    logger.info(f"    キャリブ後:    ROI={roi_cal:.1f}%, ベット数={n_cal:,}")
    logger.info(f"    改善:          {roi_cal - roi_raw:+.1f}%")
    
    return iso_reg, model


if __name__ == "__main__":
    main()
