# -*- coding: utf-8 -*-
"""
複勝専用モデル V2 - 正則化調整版

【初回結果分析】
- Test ROI: 80.8% (目標88%)
- Best iteration: 2 → 正則化が強すぎて学習が進まず
- Gap: 9.5% → 過学習は全くない

【改善方針】
- 正則化を緩める
- 学習率を上げて学習を進める
- 複勝向けの追加特徴量を検討
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import optuna
import warnings
warnings.filterwarnings('ignore')
optuna.logging.set_verbosity(optuna.logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    logger.info("データ読み込み中...")
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[races_df['race_date'] >= '2020-01-01'].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    logger.info(f"  レコード数: {len(races_df):,}")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def calculate_place_roi(df, pred_col, returns_df):
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_rate = top1['payout'].notna().sum() / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def train_with_optuna(X_train, y_train, X_valid, y_valid, valid_features, returns_df, n_trials=30):
    """Optunaでハイパーパラメータ最適化（複勝ROI基準）"""
    
    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            # 正則化を緩める方向で探索
            'max_depth': trial.suggest_int('max_depth', 3, 8),
            'num_leaves': trial.suggest_int('num_leaves', 8, 63),
            'min_child_samples': trial.suggest_int('min_child_samples', 30, 200),
            'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.15),
            'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 5.0),
            'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 5.0),
            'subsample': trial.suggest_float('subsample', 0.5, 0.9),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 0.9),
            'random_state': 42,
            'n_jobs': -1,
            'verbose': -1
        }
        
        train_ds = lgb.Dataset(X_train, y_train)
        valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
        
        model = lgb.train(
            params,
            train_ds,
            num_boost_round=1000,
            valid_sets=[valid_ds],
            callbacks=[lgb.early_stopping(30, verbose=False)]
        )
        
        valid_features_copy = valid_features.copy()
        valid_features_copy['score'] = model.predict(X_valid)
        
        _, valid_roi = calculate_place_roi(valid_features_copy, 'score', returns_df)
        
        return valid_roi
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    
    logger.info(f"  Best Valid ROI: {study.best_value:.1f}%")
    logger.info(f"  Best params: {study.best_params}")
    
    return study.best_params


def main():
    logger.info("=" * 60)
    logger.info("複勝専用モデル V2 (Optuna最適化版)")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    # 時系列分割
    train_end = '2023-01-01'
    valid_end = '2024-01-01'
    
    train_mask = races_df['race_date'] < train_end
    valid_mask = (races_df['race_date'] >= train_end) & (races_df['race_date'] < valid_end)
    test_mask = races_df['race_date'] >= valid_end
    
    train_df = races_df[train_mask].copy()
    valid_df = races_df[valid_mask].copy()
    test_df = races_df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    # 特徴量生成
    logger.info("特徴量生成中...")
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    
    # ラベル
    train_features['is_place'] = (train_features['finish_position'] <= 3).astype(int)
    valid_features['is_place'] = (valid_features['finish_position'] <= 3).astype(int)
    test_features['is_place'] = (test_features['finish_position'] <= 3).astype(int)
    
    X_train = train_features[feature_cols].fillna(0)
    y_train = train_features['is_place']
    X_valid = valid_features[feature_cols].fillna(0)
    y_valid = valid_features['is_place']
    X_test = test_features[feature_cols].fillna(0)
    
    # Optuna最適化
    logger.info("=" * 60)
    logger.info("Optunaハイパーパラメータ最適化 (n_trials=30)")
    logger.info("=" * 60)
    
    best_params = train_with_optuna(X_train, y_train, X_valid, y_valid, valid_features.copy(), returns_df, n_trials=30)
    
    # 最終モデル学習
    logger.info("")
    logger.info("=" * 60)
    logger.info("最終モデル学習")
    logger.info("=" * 60)
    
    final_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1,
        **best_params
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(
        final_params,
        train_ds,
        num_boost_round=1500,
        valid_sets=[train_ds, valid_ds],
        valid_names=['train', 'valid'],
        callbacks=[
            lgb.early_stopping(50, verbose=False),
            lgb.log_evaluation(300)
        ]
    )
    
    logger.info(f"  Best iteration: {model.best_iteration}")
    
    # 予測
    train_features['score'] = model.predict(X_train)
    valid_features['score'] = model.predict(X_valid)
    test_features['score'] = model.predict(X_test)
    
    # ROI評価
    logger.info("")
    logger.info("=" * 60)
    logger.info("複勝ROI評価 (最終)")
    logger.info("=" * 60)
    
    train_hit, train_roi = calculate_place_roi(train_features, 'score', returns_df)
    valid_hit, valid_roi = calculate_place_roi(valid_features, 'score', returns_df)
    test_hit, test_roi = calculate_place_roi(test_features, 'score', returns_df)
    
    logger.info(f"  Train: 的中率={train_hit:.1f}%, ROI={train_roi:.1f}%")
    logger.info(f"  Valid: 的中率={valid_hit:.1f}%, ROI={valid_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_hit:.1f}%, ROI={test_roi:.1f}%")
    
    gap = train_roi - test_roi
    logger.info(f"  Train-Test Gap: {gap:.1f}%")
    
    # 目標チェック
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    target_roi = 88.0
    if test_roi >= target_roi:
        logger.info(f"  ✅ 目標達成! Test ROI {test_roi:.1f}% >= {target_roi}%")
    else:
        logger.info(f"  ❌ 目標未達 Test ROI {test_roi:.1f}% < {target_roi}%")
        logger.info(f"     あと {target_roi - test_roi:.1f}% 必要")
    
    if gap < 40:
        logger.info(f"  ✅ 過学習許容範囲内 Gap {gap:.1f}% < 40%")
    else:
        logger.info(f"  ⚠️ 過学習注意 Gap {gap:.1f}% >= 40%")
    
    # 特徴量重要度
    logger.info("")
    logger.info("Top 10 特徴量:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model, fe, test_roi


if __name__ == "__main__":
    main()
