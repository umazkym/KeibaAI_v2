# -*- coding: utf-8 -*-
"""
Phase 2 #8: 特徴量削減実験

18_improvement_roadmap.md ロードマップより:
- 目的: Gap縮小（過学習対策）
- 手法: 重要度下位の特徴量を段階的に削除

【実験設計】
1. V15全55特徴量でベースライン測定
2. 重要度下位10特徴量を削除 → V15-lite45
3. 重要度下位20特徴量を削除 → V15-lite35
4. 重要度下位30特徴量を削除 → V15-ultra25

Gap縮小しつつROI維持を目指す
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


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def train_and_eval(X_train, y_train, X_test, train_f, test_f, name, params):
    """学習と評価"""
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    pred_train = model.predict(X_train)
    pred_test = model.predict(X_test)
    
    train_roi, train_hit = calc_roi(train_f, pred_train)
    test_roi, test_hit = calc_roi(test_f, pred_test)
    gap = train_roi - test_roi
    
    logger.info(f"  {name}:")
    logger.info(f"    Train: ROI={train_roi:.1f}%, 的中={train_hit:.1f}%")
    logger.info(f"    Test:  ROI={test_roi:.1f}%, 的中={test_hit:.1f}%")
    logger.info(f"    Gap:   {gap:.1f}%")
    
    return model, test_roi, gap


def main():
    logger.info("=" * 60)
    logger.info("特徴量削減実験 (Gap縮小)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    all_feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  全特徴量数: {len(all_feature_cols)}")
    
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
    
    # ========== ベースライン (V15全55特徴量) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン (V15全55特徴量)")
    logger.info("=" * 60)
    
    X_train_full = train_f[all_feature_cols].fillna(0)
    X_test_full = test_f[all_feature_cols].fillna(0)
    
    model_full, roi_full, gap_full = train_and_eval(
        X_train_full, y_train, X_test_full, train_f, test_f,
        "V15 Full (55)", params
    )
    
    # 重要度取得
    importance = pd.DataFrame({
        'feature': all_feature_cols,
        'importance': model_full.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    logger.info("")
    logger.info("Top 10 特徴量:")
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.0f}")
    
    logger.info("")
    logger.info("Bottom 10 特徴量:")
    for i, row in importance.tail(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.0f}")
    
    # ========== 削減実験 ==========
    experiments = [
        (10, "Lite 45"),
        (20, "Lite 35"),
        (30, "Ultra 25"),
    ]
    
    results = [('V15 Full', len(all_feature_cols), roi_full, gap_full)]
    
    for n_remove, name in experiments:
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"2. {name} (下位{n_remove}特徴量削除)")
        logger.info("=" * 60)
        
        # 重要度上位のみ残す
        top_features = importance.head(len(all_feature_cols) - n_remove)['feature'].tolist()
        
        X_train_lite = train_f[top_features].fillna(0)
        X_test_lite = test_f[top_features].fillna(0)
        
        model_lite, roi_lite, gap_lite = train_and_eval(
            X_train_lite, y_train, X_test_lite, train_f, test_f,
            f"{name} ({len(top_features)})", params
        )
        
        # 削除された特徴量
        removed = importance.tail(n_remove)['feature'].tolist()
        logger.info(f"  削除: {', '.join(removed[:5])}...")
        
        results.append((name, len(top_features), roi_lite, gap_lite))
    
    # ========== 結果サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    logger.info(f"  {'モデル':<15} {'特徴量数':>8} {'Test ROI':>10} {'Gap':>8}")
    logger.info("-" * 50)
    
    best_model = None
    best_score = 0
    
    for name, n_feat, roi, gap in results:
        # スコア = ROI + Gap改善ボーナス（Gapが30%以下で+5%）
        score = roi
        if gap < 30:
            score += 5
        
        status = ""
        if score > best_score:
            best_score = score
            best_model = name
            status = " <- 最良"
        
        logger.info(f"  {name:<15} {n_feat:>8} {roi:>9.1f}% {gap:>7.1f}%{status}")
    
    # 推奨
    logger.info("")
    logger.info(f"推奨: {best_model}")
    
    return results


if __name__ == "__main__":
    main()
