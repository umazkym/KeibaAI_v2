"""
4層ハイブリッドAI 訓練スクリプト v2

新しい特徴量エンジニア（hybrid_features.py）を使用
Phase 1特徴量: タイム系、上がり3F、位置取り、適性、騎手・調教師、血統、コンディション
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.modules.models.hybrid_betting_ai import HybridHorseRacingAI
from keibaai.src.features.hybrid_features import HybridFeatureEngineer, FeatureConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_all_data(data_dir: Path, start_date: str, end_date: str) -> dict:
    """全データソースを読み込む"""
    data = {}
    
    # レースデータ
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    if races_path.exists():
        df = pd.read_parquet(races_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df[(df['race_date'] >= start_date) & (df['race_date'] <= end_date)]
        data['races'] = df
        logger.info(f"Loaded races: {len(df):,} records")
    
    # 馬情報
    horses_path = data_dir / "parsed/parquet/horses/horses.parquet"
    if horses_path.exists():
        data['horses'] = pd.read_parquet(horses_path)
        logger.info(f"Loaded horses: {len(data['horses']):,} records")
    
    # 血統情報
    pedigrees_path = data_dir / "parsed/parquet/pedigrees/pedigrees.parquet"
    if pedigrees_path.exists():
        data['pedigrees'] = pd.read_parquet(pedigrees_path)
        logger.info(f"Loaded pedigrees: {len(data['pedigrees']):,} records")
    
    # コーナー通過情報
    corners_path = data_dir / "parsed/parquet/corners/corner_positions.parquet"
    if corners_path.exists():
        data['corners'] = pd.read_parquet(corners_path)
        logger.info(f"Loaded corners: {len(data['corners']):,} records")
    
    # レース詳細（ラップ情報）
    details_path = data_dir / "parsed/parquet/race_details/race_details.parquet"
    if details_path.exists():
        data['race_details'] = pd.read_parquet(details_path)
        logger.info(f"Loaded race_details: {len(data['race_details']):,} records")
    
    return data


def time_series_split(df: pd.DataFrame) -> tuple:
    """時系列分割"""
    df = df.sort_values('race_date').reset_index(drop=True)
    max_date = df['race_date'].max()
    
    # Test: 最新6ヶ月
    test_start = max_date - pd.DateOffset(months=6)
    test_df = df[df['race_date'] >= test_start]
    
    # Valid: test前6ヶ月（ギャップ1ヶ月）
    val_end = test_start - pd.DateOffset(months=1)
    val_start = val_end - pd.DateOffset(months=6)
    val_df = df[(df['race_date'] >= val_start) & (df['race_date'] < val_end)]
    
    # Train: valid前
    train_end = val_start - pd.DateOffset(months=1)
    train_df = df[df['race_date'] < train_end]
    train_df = train_df[train_df['race_date'] >= train_end - pd.DateOffset(years=3)]
    
    return train_df, val_df, test_df


def get_feature_columns(df: pd.DataFrame) -> list:
    """特徴量カラムを取得（禁止カラム除外）"""
    exclude_patterns = [
        'race_id', 'horse_id', 'jockey_id', 'trainer_id', 'sire_id',
        'horse_name', 'jockey_name', 'trainer_name', 'owner_name',
        'finish_position', 'finish_time', 'win_odds', 'popularity',
        'payout', 'prize', 'morning_odds', 'morning_popularity',
        'race_date', 'race_name', 'post_time', 'birth_date',
        'ancestor_id', 'ancestor_name', 'breeder_name', 'producing_area',
        'career_stats', 'last_5_finishes', 'margin', 'weather',
        'sex_age', 'track_condition', 'track_surface', 'distance_category', 'venue'
    ]
    
    feature_cols = []
    for col in df.columns:
        exclude = any(pattern in col.lower() for pattern in [p.lower() for p in exclude_patterns])
        if not exclude and df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            feature_cols.append(col)
    
    return feature_cols


def main():
    parser = argparse.ArgumentParser(description='Train Hybrid AI v2 with new features')
    parser.add_argument('--data_dir', type=str, default='keibaai/data')
    parser.add_argument('--output_dir', type=str, default='keibaai/data/models/hybrid_v2')
    parser.add_argument('--start_date', type=str, default='2020-01-01')
    parser.add_argument('--end_date', type=str, default='2024-12-31')
    parser.add_argument('--ev_threshold', type=float, default=1.05)
    parser.add_argument('--kelly_fraction', type=float, default=0.25)
    parser.add_argument('--backtest', action='store_true')
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Hybrid Horse Racing AI v2.0 Training")
    logger.info("=" * 70)
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"EV threshold: {args.ev_threshold}")
    
    data_dir = Path(args.data_dir)
    
    # データ読み込み
    logger.info("\n[Step 1/5] データ読み込み")
    all_data = load_all_data(data_dir, args.start_date, args.end_date)
    
    if 'races' not in all_data:
        logger.error("レースデータが見つかりません")
        return
    
    races_df = all_data['races']
    
    # 欠損除去
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    logger.info(f"After cleaning: {len(races_df):,} records")
    
    # 人気カラム作成（禁止だがバックテスト評価用）
    if 'popularity' not in races_df.columns:
        races_df['popularity'] = races_df.groupby('race_id')['win_odds'].rank()
    
    # 特徴量エンジニア
    logger.info("\n[Step 2/5] 特徴量生成")
    
    feature_config = FeatureConfig(
        lookback_races=5,
    )
    
    feature_engineer = HybridFeatureEngineer(config=feature_config)
    
    # fit（統計計算）
    feature_engineer.fit(
        races_df=races_df,
        horses_df=all_data.get('horses'),
        pedigrees_df=all_data.get('pedigrees'),
        corners_df=all_data.get('corners'),
        race_details_df=all_data.get('race_details')
    )
    
    # transform（特徴量生成） - ターゲット変数を保持
    logger.info("特徴量変換中...")
    
    # 必要なカラムを先に保持
    preserved_cols = ['race_id', 'horse_id', 'race_date', 'finish_position', 
                      'win_odds', 'popularity', 'horse_number']
    preserved_data = races_df[preserved_cols].copy()
    
    df_with_features = feature_engineer.transform(races_df)
    
    # 重複行を除去してからマージ
    df_with_features = df_with_features.drop_duplicates(subset=['race_id', 'horse_id'])
    
    # 保持したカラムをマージで復元
    for col in preserved_cols:
        if col not in df_with_features.columns:
            temp = preserved_data[['race_id', 'horse_id', col]].drop_duplicates()
            df_with_features = df_with_features.merge(temp, on=['race_id', 'horse_id'], how='left')
    
    logger.info(f"特徴量生成完了: {len(df_with_features)} records, {len(df_with_features.columns)} カラム")
    
    # 時系列分割
    logger.info("\n[Step 3/5] 時系列分割")
    train_df, val_df, test_df = time_series_split(df_with_features)
    
    logger.info(f"Train: {len(train_df):,} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Valid: {len(val_df):,} records ({val_df['race_date'].min()} - {val_df['race_date'].max()})")
    logger.info(f"Test: {len(test_df):,} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # 特徴量カラムを取得
    feature_cols = get_feature_columns(train_df)
    logger.info(f"Using {len(feature_cols)} feature columns")
    logger.info(f"Feature samples: {feature_cols[:10]}")
    
    # 準備
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df['finish_position']
    groups_val = val_df['race_id']
    
    # モデル初期化・学習
    logger.info("\n[Step 4/5] モデル学習")
    
    ai = HybridHorseRacingAI(
        ev_threshold=args.ev_threshold,
        kelly_fraction=args.kelly_fraction,
        bankroll=100000
    )
    
    ai.train(X_train, y_train, groups_train, X_val, y_val, groups_val)
    
    # モデル保存
    output_dir = Path(args.output_dir)
    ai.save(str(output_dir))
    logger.info(f"Model saved to {output_dir}")
    
    # 特徴量重要度
    if hasattr(ai.layer1, 'feature_importance_') and ai.layer1.feature_importance_ is not None:
        importance = ai.layer1.feature_importance_
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:20]
        logger.info("\nTop 20 Feature Importances:")
        for i, (feat, imp) in enumerate(top_features, 1):
            logger.info(f"  {i:2d}. {feat}: {imp:.4f}")
    
    # バックテスト
    if args.backtest:
        logger.info("\n[Step 5/5] バックテスト")
        
        result = ai.backtest(
            test_df,
            feature_cols,
            race_id_col='race_id',
            rank_col='finish_position',
            odds_col='win_odds',
            horse_number_col='horse_number',
            popularity_col='popularity'
        )
        
        logger.info("\n" + "=" * 70)
        logger.info("FINAL RESULTS")
        logger.info("=" * 70)
        logger.info(f"ROI: {result.roi:.1f}%")
        logger.info(f"Hit Rate: {result.hit_rate:.1f}%")
        logger.info(f"1番人気選択率: {result.favorite_selection_rate:.1f}%")
        logger.info(f"Total Bet: ¥{result.total_bet:,}")
        logger.info(f"Total Payout: ¥{result.total_payout:,}")
        
        # Phase 1 目標との比較
        logger.info("\n" + "-" * 50)
        logger.info("Phase 1 Target Comparison")
        logger.info("-" * 50)
        
        if result.roi >= 85:
            logger.info(f"ROI >= 85%: ✓ PASS")
        else:
            logger.info(f"ROI >= 85%: ✗ FAIL ({85 - result.roi:.1f}% short)")
        
        if result.favorite_selection_rate <= 60:
            logger.info(f"1番人気選択率 <= 60%: ✓ PASS")
        else:
            logger.info(f"1番人気選択率 <= 60%: ✗ FAIL")
        
        # 月別ROI
        if result.monthly_rois:
            logger.info("\nMonthly ROI:")
            for month, roi in sorted(result.monthly_rois.items()):
                logger.info(f"  {month}: {roi:.1f}%")
            
            monthly_values = list(result.monthly_rois.values())
            std = np.std(monthly_values)
            logger.info(f"\nMonthly ROI Std: {std:.2f} {'(PASS)' if std < 30 else '(FAIL)'}")
    
    logger.info("\n" + "=" * 70)
    logger.info("Training Complete!")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
