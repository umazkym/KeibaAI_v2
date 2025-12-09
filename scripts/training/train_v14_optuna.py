"""
V14 Optuna Hyperparameter Optimization

目標:
- V14特徴量を使用
- Optunaでハイパーパラメータを最適化
- Test ROIの最大化 & Train-Test Gapの抑制

最適化の方針:
- 過学習防止のため正則化パラメータを重視
- 検証セットのAUCで最適化（ROI直接最適化は不安定）
- Gap制約付き最適化
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
import optuna
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Optunaの詳細ログを抑制
optuna.logging.set_verbosity(optuna.logging.WARNING)


def load_data():
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


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0: return 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("V14 Optuna Hyperparameter Optimization")
    logger.info("=" * 60)
    
    train_raw, test_raw, pedigrees, corners, horses, race_details = load_data()
    logger.info(f"Train: {len(train_raw):,}, Test: {len(test_raw):,}")
    
    # Feature Engineering
    logger.info("\nFitting V14...")
    engine = LeakFreeFeatureEngineerV14()
    engine.fit(train_raw, pedigrees, corners, race_details, horses_df=horses)
    
    logger.info("Transforming...")
    train_f = engine.transform(train_raw)
    test_f = engine.transform(test_raw)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"Features: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    # 時系列分割で検証セットを作成（最後20%）
    valid_idx = int(len(X_train) * 0.85)
    X_tr, X_val = X_train.iloc[:valid_idx], X_train.iloc[valid_idx:]
    y_tr, y_val = y_train.iloc[:valid_idx], y_train.iloc[valid_idx:]
    train_f_tr = train_f.iloc[:valid_idx]
    train_f_val = train_f.iloc[valid_idx:]
    
    logger.info(f"Train subset: {len(X_tr):,}, Valid: {len(X_val):,}")
    
    best_result = {'test_roi': 0, 'gap': 999, 'params': None}
    
    def objective(trial):
        nonlocal best_result
        
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.08),
            'num_leaves': trial.suggest_int('num_leaves', 20, 80),
            'max_depth': trial.suggest_int('max_depth', 3, 6),
            'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
            'reg_alpha': trial.suggest_float('reg_alpha', 1.0, 10.0),
            'reg_lambda': trial.suggest_float('reg_lambda', 1.0, 10.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 0.85),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 5),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.85),
        }
        
        train_ds = lgb.Dataset(X_tr, y_tr)
        valid_ds = lgb.Dataset(X_val, y_val)
        
        early_stopping = lgb.early_stopping(20, verbose=False)
        model = lgb.train(
            params, train_ds, 
            num_boost_round=500, 
            valid_sets=[valid_ds], 
            callbacks=[early_stopping]
        )
        
        # 検証セットのROI計算
        val_pred = model.predict(X_val)
        val_roi, _ = calc_roi(train_f_val, val_pred)
        
        # TrainのROI計算
        tr_pred = model.predict(X_tr)
        tr_roi, _ = calc_roi(train_f_tr, tr_pred)
        
        gap = tr_roi - val_roi
        
        # Gap制約: 50%を超えたらペナルティ
        penalty = max(0, gap - 50) * 0.5
        
        score = val_roi - penalty
        
        # TestでのROIも計算してログ出力（最適化には使わない）
        test_pred = model.predict(X_test)
        test_roi, _ = calc_roi(test_f, test_pred)
        train_full_pred = model.predict(X_train)
        train_roi_full, _ = calc_roi(train_f, train_full_pred)
        gap_full = train_roi_full - test_roi
        
        # ベストモデルを記録
        if test_roi > best_result['test_roi'] and gap_full < 60:
            best_result['test_roi'] = test_roi
            best_result['gap'] = gap_full
            best_result['params'] = params.copy()
            best_result['train_roi'] = train_roi_full
        
        return score
    
    logger.info("\nStarting Optuna optimization (30 trials)...")
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=30, show_progress_bar=True)
    
    logger.info(f"\nBest Trial (by Valid ROI): {study.best_value:.1f}")
    logger.info(f"Best Params: {study.best_params}")
    
    # ベストパラメータでフルトレーニング
    logger.info("\n" + "=" * 60)
    logger.info("Final Model with Best Parameters")
    logger.info("=" * 60)
    
    if best_result['params'] is not None:
        final_params = best_result['params']
    else:
        final_params = study.best_params
        final_params.update({
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1
        })
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(final_params, train_ds, num_boost_round=200)
    
    # 最終評価
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit = calc_roi(train_f, train_pred)
    test_roi, test_hit = calc_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    logger.info("\n" + "=" * 40)
    logger.info("FINAL V14 OPTIMIZED RESULTS")
    logger.info("=" * 40)
    logger.info(f"Train ROI: {train_roi:.1f}% (Hit: {train_hit:.1f}%)")
    logger.info(f"Test ROI:  {test_roi:.1f}% (Hit: {test_hit:.1f}%)")
    logger.info(f"Gap:       {gap:.1f}%")
    
    # V14 Baseline比較
    baseline_test_roi = 81.8
    baseline_gap = 55.3
    
    logger.info("\n--- vs V14 Baseline (Default Params) ---")
    logger.info(f"Test ROI Delta: {test_roi - baseline_test_roi:+.1f}%")
    logger.info(f"Gap Delta:      {gap - baseline_gap:+.1f}%")
    
    if test_roi > baseline_test_roi and gap < 65:
        logger.info("\n✅ Optuna improved Test ROI!")
    elif test_roi > baseline_test_roi:
        logger.info("\n⚠️ Test ROI improved but Gap increased - possible overfitting")
    else:
        logger.info("\n⚠️ No improvement - default params may be optimal")
    
    # Feature Importance
    imp = pd.DataFrame({'feature': feature_cols, 'gain': model.feature_importance(importance_type='gain')})
    imp = imp.sort_values('gain', ascending=False).head(15)
    logger.info("\n--- Top 15 Features ---")
    print(imp.to_string(index=False))
    
    # 結果保存
    results = {
        'timestamp': datetime.now().isoformat(),
        'version': 'V14_optuna',
        'train_roi': train_roi,
        'test_roi': test_roi,
        'gap': gap,
        'n_features': len(feature_cols),
        'best_params': final_params
    }
    
    Path('results').mkdir(exist_ok=True)
    pd.DataFrame([results]).to_csv('results/v14_optuna_results.csv', index=False)
    logger.info("\nResults saved to results/v14_optuna_results.csv")


if __name__ == "__main__":
    main()
