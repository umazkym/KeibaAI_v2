"""
Train Hybrid v1.0 - 4層ハイブリッドアンサンブルAI訓練スクリプト

Usage:
    python scripts/training/train_hybrid_v1.py
    python scripts/training/train_hybrid_v1.py --backtest
    python scripts/training/train_hybrid_v1.py --config config.json

Features:
- CatBoostサポート（オプション）
- 厳格なWalk-Forward検証（ギャップ期間あり）
- v11.0との比較評価
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.models.hybrid_betting_ai import HybridHorseRacingAI

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data(data_dir: Path, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    レースデータと特徴量を読み込み
    
    パーティション化された特徴量データ（year=YYYY/month=MM/）に対応
    """
    logger.info(f"Loading data from {data_dir}")
    
    # レース結果
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    if not races_path.exists():
        raise FileNotFoundError(f"Races file not found: {races_path}")
    
    df = pd.read_parquet(races_path)
    logger.info(f"Loaded {len(df)} race records")
    
    # 日付フィルタ
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        if start_date:
            df = df[df['race_date'] >= start_date]
        if end_date:
            df = df[df['race_date'] <= end_date]
        
        logger.info(f"After date filter: {len(df)} records")
    
    # 特徴量ファイルがあればマージ
    features_path = data_dir / "features/parquet"
    if features_path.exists():
        features_loaded = False
        
        # パーティション化されたデータの読み込みを試行
        year_dirs = sorted(features_path.glob('year=*'))
        if year_dirs:
            logger.info(f"Found {len(year_dirs)} partitioned year directories")
            feature_dfs = []
            for year_dir in year_dirs:
                try:
                    year_df = pd.read_parquet(year_dir)
                    feature_dfs.append(year_df)
                    logger.debug(f"  {year_dir.name}: {len(year_df)} rows")
                except Exception as e:
                    logger.warning(f"  {year_dir.name}: Error - {e}")
            
            if feature_dfs:
                features_df = pd.concat(feature_dfs, ignore_index=True)
                logger.info(f"Loaded {len(features_df)} feature records from partitions")
                
                # ★重要: 重複を除去（race_id, horse_numberの組み合わせで最初の1件を保持）
                merge_keys = []
                for key in ['race_id', 'horse_id', 'horse_number']:
                    if key in df.columns and key in features_df.columns:
                        merge_keys.append(key)
                
                if merge_keys:
                    # 重複を除去
                    features_df_unique = features_df.drop_duplicates(subset=merge_keys, keep='first')
                    logger.info(f"After dedup: {len(features_df_unique)} records (removed {len(features_df) - len(features_df_unique)} duplicates)")
                    
                    df = df.merge(features_df_unique, on=merge_keys, how='left', suffixes=('', '_feat'))
                    logger.info(f"Merged features on {merge_keys}: {len(df.columns)} total columns")
                    features_loaded = True
                else:
                    logger.warning(f"No common merge keys found. DF keys: {df.columns[:10].tolist()}, Feature keys: {features_df.columns[:10].tolist()}")
        
        # 通常のparquetファイルとして読み込みを試行
        if not features_loaded:
            try:
                features_df = pd.read_parquet(features_path)
                df = df.merge(features_df, on=['race_id', 'horse_number'], how='left')
                logger.info(f"Merged features (single file): {len(df.columns)} total columns")
            except Exception as e:
                logger.warning(f"Could not load features as single file: {e}")
                logger.info("Proceeding with race data only (no additional features)")
    
    return df


def get_feature_columns(df: pd.DataFrame) -> list:
    """
    使用可能な特徴量カラムを取得
    
    禁止カラム（オッズ・結果・ID系）を除外
    """
    exclude_patterns = [
        'race_id', 'horse_id', 'jockey_id', 'trainer_id',
        'horse_name', 'jockey_name', 'trainer_name', 'owner_name',
        'finish_position', 'finish_time', 'win_odds', 'popularity',
        'payout', 'prize', 'morning_odds', 'morning_popularity',
        'passing_order', 'last_3f', 'margin', 'pace_index',
        'race_date', 'race_name', 'post_time'
    ]
    
    feature_cols = []
    for col in df.columns:
        exclude = False
        for pattern in exclude_patterns:
            if pattern in col.lower():
                exclude = True
                break
        
        # 数値カラムのみ
        if not exclude and df[col].dtype in ['int64', 'float64', 'int32', 'float32', 'Int8', 'Int16']:
            feature_cols.append(col)
    
    return feature_cols


def time_series_split(
    df: pd.DataFrame, 
    train_years: int = 3,
    val_months: int = 6,
    test_months: int = 6,
    gap_months: int = 1
) -> tuple:
    """
    時系列分割（ギャップ期間あり）
    
    Returns:
        (train_df, val_df, test_df)
    """
    df = df.sort_values('race_date').reset_index(drop=True)
    
    max_date = df['race_date'].max()
    
    # Test: 最新 test_months
    test_start = max_date - pd.DateOffset(months=test_months)
    test_df = df[df['race_date'] >= test_start]
    
    # Valid: test前 val_months（ギャップ考慮）
    val_end = test_start - pd.DateOffset(months=gap_months)
    val_start = val_end - pd.DateOffset(months=val_months)
    val_df = df[(df['race_date'] >= val_start) & (df['race_date'] < val_end)]
    
    # Train: valid前
    train_end = val_start - pd.DateOffset(months=gap_months)
    train_df = df[df['race_date'] < train_end]
    
    # 最低 train_years 分を確保
    min_train_date = train_end - pd.DateOffset(years=train_years)
    train_df = train_df[train_df['race_date'] >= min_train_date]
    
    logger.info(f"Train: {len(train_df)} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Valid: {len(val_df)} records ({val_df['race_date'].min()} - {val_df['race_date'].max()})")
    logger.info(f"Test: {len(test_df)} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    return train_df, val_df, test_df


def main():
    parser = argparse.ArgumentParser(description='Train Hybrid Horse Racing AI v1.0')
    parser.add_argument('--data_dir', type=str, default='keibaai/data',
                        help='Data directory path')
    parser.add_argument('--output_dir', type=str, default='keibaai/data/models/hybrid_v1',
                        help='Output directory for trained model')
    parser.add_argument('--start_date', type=str, default='2020-01-01',
                        help='Training start date')
    parser.add_argument('--end_date', type=str, default=None,
                        help='Training end date')
    parser.add_argument('--backtest', action='store_true',
                        help='Run backtest after training')
    parser.add_argument('--ev_threshold', type=float, default=1.10,
                        help='EV threshold for betting')
    parser.add_argument('--kelly_fraction', type=float, default=0.25,
                        help='Kelly criterion fraction')
    parser.add_argument('--bankroll', type=int, default=100000,
                        help='Bankroll for betting (yen)')
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Hybrid Horse Racing AI v1.0 Training")
    logger.info("=" * 70)
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"EV threshold: {args.ev_threshold}")
    logger.info(f"Kelly fraction: {args.kelly_fraction}")
    
    # データ読み込み
    data_dir = Path(args.data_dir)
    df = load_data(data_dir, args.start_date, args.end_date)
    
    # 必要なカラムの存在チェック
    required_cols = ['race_id', 'horse_number', 'finish_position', 'win_odds', 'race_date']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # 欠損値を含む行を除外
    df = df.dropna(subset=['finish_position', 'win_odds'])
    
    # 特徴量カラム取得
    feature_cols = get_feature_columns(df)
    logger.info(f"Using {len(feature_cols)} feature columns")
    
    if len(feature_cols) < 10:
        logger.warning(f"Very few features detected. Check data loading.")
        logger.info(f"Available columns: {df.columns.tolist()}")
    
    # 時系列分割
    train_df, val_df, test_df = time_series_split(df)
    
    # 特徴量と目標変数
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df['finish_position']
    groups_val = val_df['race_id']
    
    # モデル初期化
    ai = HybridHorseRacingAI(
        ev_threshold=args.ev_threshold,
        kelly_fraction=args.kelly_fraction,
        bankroll=args.bankroll
    )
    
    # 学習
    ai.train(
        X_train, y_train, groups_train,
        X_val, y_val, groups_val
    )
    
    # モデル保存
    output_dir = Path(args.output_dir)
    ai.save(str(output_dir))
    logger.info(f"Model saved to {output_dir}")
    
    # 特徴量重要度表示
    logger.info("\nTop 20 Feature Importances:")
    importance = ai.layer1.get_feature_importance(top_n=20)
    for i, (name, imp) in enumerate(importance.items(), 1):
        logger.info(f"  {i:2d}. {name}: {imp:.4f}")
    
    # バックテスト
    if args.backtest:
        logger.info("\n" + "=" * 70)
        logger.info("Running Backtest")
        logger.info("=" * 70)
        
        # 人気カラムがなければ作成
        if 'popularity' not in test_df.columns:
            test_df['popularity'] = test_df.groupby('race_id')['win_odds'].rank()
        
        result = ai.backtest(
            test_df,
            feature_cols,
            race_id_col='race_id',
            rank_col='finish_position',
            odds_col='win_odds',
            horse_number_col='horse_number',
            popularity_col='popularity'
        )
        
        # 結果サマリー
        logger.info("\n" + "=" * 70)
        logger.info("FINAL RESULTS")
        logger.info("=" * 70)
        logger.info(f"ROI: {result.roi:.1f}%")
        logger.info(f"Hit Rate: {result.hit_rate:.1f}%")
        logger.info(f"1番人気選択率: {result.favorite_selection_rate:.1f}%")
        logger.info(f"Total Bet: ¥{result.total_bet:,}")
        logger.info(f"Total Payout: ¥{result.total_payout:,}")
        
        # Phase 1目標との比較
        logger.info("\n" + "-" * 50)
        logger.info("Phase 1 Target Comparison")
        logger.info("-" * 50)
        
        roi_target = 85.0
        fav_target = 60.0
        
        roi_pass = "✓ PASS" if result.roi >= roi_target else f"✗ FAIL ({roi_target - result.roi:.1f}% short)"
        fav_pass = "✓ PASS" if result.favorite_selection_rate <= fav_target else f"✗ FAIL ({result.favorite_selection_rate - fav_target:.1f}% over)"
        
        logger.info(f"ROI >= 85%: {roi_pass}")
        logger.info(f"1番人気選択率 <= 60%: {fav_pass}")
        
        # 月別ROI
        if result.monthly_rois:
            logger.info("\nMonthly ROI:")
            for month, roi in sorted(result.monthly_rois.items()):
                logger.info(f"  {month}: {roi:.1f}%")
            
            # 安定性チェック
            rois = list(result.monthly_rois.values())
            if len(rois) >= 2:
                std = np.std(rois)
                logger.info(f"\nMonthly ROI Std: {std:.2f} ({'PASS' if std < 30 else 'FAIL'})")
    
    logger.info("\n" + "=" * 70)
    logger.info("Training Complete!")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
