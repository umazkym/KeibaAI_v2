# -*- coding: utf-8 -*-
"""
ROI基準ハイパーパラメータ最適化（Optuna）

【概要】
- 従来のAUC最適化ではなく、ROI自体を目的関数にする
- V15をベースに、より高いTest ROIを探索

【リーク防止】
- Optunaの評価はValidation期間で実施
- Test期間は最終評価にのみ使用（探索には使わない）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')
optuna.logging.set_verbosity(optuna.logging.WARNING)

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


def calc_roi(df, preds):
    """ROI計算（Top1予測）"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("ROI基準ハイパーパラメータ最適化（Optuna）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # Period設定
    # Train: ~2023-12-31（Optunaの学習用）
    # Valid: 2024-01-01~2024-12-31（Optunaの評価用）
    # Test: 2025-01-01~（最終評価用、Optunaには見せない）
    train = races[races['race_date'] <= '2023-12-31'].copy()
    valid = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] <= '2024-12-31')].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2023-12-31)")
    logger.info(f"  Valid: {len(valid):,}件 (2024-01-01~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    X_valid = valid_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    # ========== ベースライン（V15相当） ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン（V15相当パラメータ）")
    logger.info("=" * 60)
    
    base_params = {
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
    base_model = lgb.train(base_params, train_ds, num_boost_round=200)
    
    pred_train_base = base_model.predict(X_train)
    pred_valid_base = base_model.predict(X_valid)
    pred_test_base = base_model.predict(X_test)
    
    train_roi_base, train_hit_base = calc_roi(train_f, pred_train_base)
    valid_roi_base, valid_hit_base = calc_roi(valid_f, pred_valid_base)
    test_roi_base, test_hit_base = calc_roi(test_f, pred_test_base)
    
    logger.info(f"  Train: 的中率={train_hit_base:.1f}%, ROI={train_roi_base:.1f}%")
    logger.info(f"  Valid: 的中率={valid_hit_base:.1f}%, ROI={valid_roi_base:.1f}%")
    logger.info(f"  Test:  的中率={test_hit_base:.1f}%, ROI={test_roi_base:.1f}%")
    
    # ========== Optuna最適化 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. Optuna最適化（目的関数=Valid ROI、n_trials=50）")
    logger.info("=" * 60)
    
    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 4, 50),
            'max_depth': trial.suggest_int('max_depth', 2, 6),
            'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
            'reg_alpha': trial.suggest_float('reg_alpha', 0.1, 10.0),
            'reg_lambda': trial.suggest_float('reg_lambda', 0.1, 10.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 0.9),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 0.9),
        }
        
        model = lgb.train(params, train_ds, num_boost_round=200)
        preds = model.predict(X_valid)
        
        valid_roi, _ = calc_roi(valid_f, preds)
        return valid_roi
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50, show_progress_bar=False)
    
    logger.info(f"  Best Valid ROI: {study.best_value:.1f}%")
    logger.info(f"  Best params: {study.best_params}")
    
    # ========== Best paramsでTest評価 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. Optuna Best Params でTest評価")
    logger.info("=" * 60)
    
    best_params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        **study.best_params
    }
    
    best_model = lgb.train(best_params, train_ds, num_boost_round=200)
    
    pred_train_best = best_model.predict(X_train)
    pred_valid_best = best_model.predict(X_valid)
    pred_test_best = best_model.predict(X_test)
    
    train_roi_best, train_hit_best = calc_roi(train_f, pred_train_best)
    valid_roi_best, valid_hit_best = calc_roi(valid_f, pred_valid_best)
    test_roi_best, test_hit_best = calc_roi(test_f, pred_test_best)
    
    logger.info(f"  Train: 的中率={train_hit_best:.1f}%, ROI={train_roi_best:.1f}%")
    logger.info(f"  Valid: 的中率={valid_hit_best:.1f}%, ROI={valid_roi_best:.1f}%")
    logger.info(f"  Test:  的中率={test_hit_best:.1f}%, ROI={test_roi_best:.1f}%")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    logger.info(f"                       ベースライン    Optuna最適化    改善幅")
    logger.info(f"  Test ROI:            {test_roi_base:7.1f}%       {test_roi_best:7.1f}%       {test_roi_best - test_roi_base:+.1f}%")
    logger.info(f"  Test 的中率:         {test_hit_base:7.1f}%       {test_hit_best:7.1f}%       {test_hit_best - test_hit_base:+.1f}%")
    logger.info(f"  Valid ROI:           {valid_roi_base:7.1f}%       {valid_roi_best:7.1f}%")
    logger.info(f"  Train-Test Gap:      {train_roi_base - test_roi_base:7.1f}%       {train_roi_best - test_roi_best:7.1f}%")
    
    # 過適合チェック
    gap = train_roi_best - test_roi_best
    if gap > 50:
        logger.info("")
        logger.info("⚠️ 警告: Train-Test Gapが大きい (>50%)。過学習の可能性あり。")
    
    return base_model, best_model, study


if __name__ == "__main__":
    main()
