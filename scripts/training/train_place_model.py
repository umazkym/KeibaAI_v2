# -*- coding: utf-8 -*-
"""
複勝専用モデル（モデルB）学習スクリプト

18_improvement_roadmap.md Phase 1 実装

【変更点】
- ラベル: is_win (1着) → is_place (3着以内)
- 目標: 複勝ROI 83.7% → 88%+

【リーク防止】
- win_oddsは特徴量として使用しない（sample_weightも複勝では使用しない）
- 複勝払戻はreturns.parquetから取得（正確なROI計算のため）

【過学習対策】
- Train-Test Gapを監視
- 強力な正則化
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# パスをシステムに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    """データ読み込み"""
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
    logger.info(f"  レース数: {races_df['race_id'].nunique():,}")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def calculate_place_roi(df, pred_col='score', returns_df=None):
    """
    複勝ROIを計算（returns.parquetを使用）
    
    【ロジック】
    - 各レースで予測スコア1位の馬に複勝で賭ける
    - その馬が3着以内なら、returns.parquetの複勝払戻を取得
    - ROI = 総払戻 / 総ベット * 100
    """
    if returns_df is None:
        # returns_dfがない場合は3着以内的中率のみ返す
        df = df.copy()
        df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
        top1 = df[df['pred_rank'] == 1]
        
        hit_rate = (top1['finish_position'] <= 3).mean() * 100
        # 複勝払戻がないので推定ROI（1着150円, 2着200円, 3着250円で概算）
        top1_place = top1[top1['finish_position'] <= 3].copy()
        avg_payout = 150 + (top1_place['finish_position'].mean() - 1) * 50 if len(top1_place) > 0 else 0
        est_roi = hit_rate / 100 * avg_payout / 100 * 100
        return hit_rate, est_roi
    
    # returns.parquetを使用した正確な計算
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    
    # 複勝払戻とマージ
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100  # 1レース100円
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_count = top1['payout'].notna().sum()
    hit_rate = hit_count / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def train_place_model(races_df, pedigrees_df, corners_df, race_details_df, returns_df):
    """複勝専用モデルを学習"""
    
    # 時系列分割
    train_end = '2023-01-01'
    valid_end = '2024-01-01'
    
    train_mask = races_df['race_date'] < train_end
    valid_mask = (races_df['race_date'] >= train_end) & (races_df['race_date'] < valid_end)
    test_mask = races_df['race_date'] >= valid_end
    
    train_df = races_df[train_mask].copy()
    valid_df = races_df[valid_mask].copy()
    test_df = races_df[test_mask].copy()
    
    logger.info("=" * 60)
    logger.info("データ分割")
    logger.info("=" * 60)
    logger.info(f"  Train: {len(train_df):,}件 (~{train_end})")
    logger.info(f"  Valid: {len(valid_df):,}件 ({train_end}~{valid_end})")
    logger.info(f"  Test:  {len(test_df):,}件 ({valid_end}~)")
    
    # 特徴量エンジニア
    logger.info("")
    logger.info("=" * 60)
    logger.info("特徴量生成 (LeakFreeFeatureEngineerV15)")
    logger.info("=" * 60)
    
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # ラベル作成（複勝: 3着以内）
    train_features['is_place'] = (train_features['finish_position'] <= 3).astype(int)
    valid_features['is_place'] = (valid_features['finish_position'] <= 3).astype(int)
    test_features['is_place'] = (test_features['finish_position'] <= 3).astype(int)
    
    logger.info(f"  Train 3着以内率: {train_features['is_place'].mean()*100:.1f}%")
    logger.info(f"  Valid 3着以内率: {valid_features['is_place'].mean()*100:.1f}%")
    logger.info(f"  Test 3着以内率: {test_features['is_place'].mean()*100:.1f}%")
    
    # データ準備
    X_train = train_features[feature_cols].fillna(0)
    y_train = train_features['is_place']
    X_valid = valid_features[feature_cols].fillna(0)
    y_valid = valid_features['is_place']
    X_test = test_features[feature_cols].fillna(0)
    y_test = test_features['is_place']
    
    # モデルパラメータ（正則化強化）
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'max_depth': 3,
        'num_leaves': 15,
        'min_child_samples': 150,
        'learning_rate': 0.02,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'subsample': 0.6,
        'colsample_bytree': 0.6,
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1
    }
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("モデル学習 (LightGBM Binary Classification)")
    logger.info("=" * 60)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(
        params,
        train_ds,
        num_boost_round=2000,
        valid_sets=[train_ds, valid_ds],
        valid_names=['train', 'valid'],
        callbacks=[
            lgb.early_stopping(50, verbose=False),
            lgb.log_evaluation(200)
        ]
    )
    
    logger.info(f"  Best iteration: {model.best_iteration}")
    
    # 予測
    train_features['score'] = model.predict(X_train)
    valid_features['score'] = model.predict(X_valid)
    test_features['score'] = model.predict(X_test)
    
    # ROI計算
    logger.info("")
    logger.info("=" * 60)
    logger.info("複勝ROI評価")
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
    logger.info("目標対比")
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
    logger.info("=" * 60)
    logger.info("特徴量重要度 Top 10")
    logger.info("=" * 60)
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for _, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model, fe, test_roi, gap


def main():
    logger.info("=" * 60)
    logger.info("複勝専用モデル (モデルB) 学習開始")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    model, fe, test_roi, gap = train_place_model(
        races_df, pedigrees_df, corners_df, race_details_df, returns_df
    )
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("完了サマリー")
    logger.info("=" * 60)
    logger.info(f"  Test ROI: {test_roi:.1f}%")
    logger.info(f"  Train-Test Gap: {gap:.1f}%")
    
    return model, fe


if __name__ == "__main__":
    main()
