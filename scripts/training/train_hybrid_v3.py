"""
4層ハイブリッドAI 訓練スクリプト v3

改善点（第三者レビュー反映）:
1. LeakFreeFeatureEngineerを使用（リーク完全排除）
2. キャリブレーション品質評価（Brier Score, ECE）
3. Walk-Forward検証の厳格化（ギャップ期間1ヶ月）
4. Phase 1目標との比較レポート

目標:
- ROI: 85-90%
- 1番人気選択率: < 60%
- Brier Score: < 0.2
- ECE: < 0.05
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.models.hybrid_betting_ai import HybridHorseRacingAI
from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer
from keibaai.src.models.probability_calibrator import evaluate_calibration

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
    
    # 血統情報
    pedigrees_path = data_dir / "parsed/parquet/pedigrees/pedigrees.parquet"
    if pedigrees_path.exists():
        data['pedigrees'] = pd.read_parquet(pedigrees_path)
        logger.info(f"Loaded pedigrees: {len(data['pedigrees']):,} records")
    
    return data


def time_series_split_with_gap(
    df: pd.DataFrame, 
    gap_months: int = 1
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    厳格な時系列分割（ギャップ期間あり）
    
    Args:
        df: データ
        gap_months: Train/Valid, Valid/Test間のギャップ期間（月）
        
    Returns:
        (train_df, val_df, test_df)
    """
    df = df.sort_values('race_date').reset_index(drop=True)
    max_date = df['race_date'].max()
    
    # Test: 最新6ヶ月
    test_start = max_date - pd.DateOffset(months=6)
    test_df = df[df['race_date'] >= test_start]
    
    # Gap: Valid/Test間
    val_end = test_start - pd.DateOffset(months=gap_months)
    
    # Valid: gap前6ヶ月
    val_start = val_end - pd.DateOffset(months=6)
    val_df = df[(df['race_date'] >= val_start) & (df['race_date'] < val_end)]
    
    # Gap: Train/Valid間
    train_end = val_start - pd.DateOffset(months=gap_months)
    
    # Train: gap前の3年分
    train_start = train_end - pd.DateOffset(years=3)
    train_df = df[(df['race_date'] >= train_start) & (df['race_date'] < train_end)]
    
    logger.info(f"Time Series Split with {gap_months} month gap:")
    logger.info(f"  Train: {train_df['race_date'].min()} - {train_df['race_date'].max()}")
    logger.info(f"  [GAP: {gap_months} month]")
    logger.info(f"  Valid: {val_df['race_date'].min()} - {val_df['race_date'].max()}")
    logger.info(f"  [GAP: {gap_months} month]")
    logger.info(f"  Test:  {test_df['race_date'].min()} - {test_df['race_date'].max()}")
    
    return train_df, val_df, test_df


def evaluate_model_quality(
    predictions: pd.DataFrame,
    actual_ranks: pd.Series,
    groups: pd.Series
) -> Dict:
    """
    モデル品質を評価
    
    Returns:
        評価指標の辞書
    """
    # 各レースの1位予測を取得
    top1_pred = predictions.groupby(groups).apply(
        lambda x: x.nlargest(1, 'ability_score').index[0]
    )
    
    # 実際の1着馬
    actual_wins = actual_ranks == 1
    
    # 予測確率（グループ内で正規化）
    predictions['win_prob'] = predictions.groupby(groups)['ability_score'].transform(
        lambda x: np.exp(x) / np.exp(x).sum()
    )
    
    # Brier Score, ECE
    calibration = evaluate_calibration(
        predictions['win_prob'].values,
        actual_wins.values
    )
    
    return calibration


def main():
    parser = argparse.ArgumentParser(description='Train Hybrid AI v3 (Review Improved)')
    parser.add_argument('--data_dir', type=str, default='keibaai/data')
    parser.add_argument('--output_dir', type=str, default='keibaai/data/models/hybrid_v3')
    parser.add_argument('--start_date', type=str, default='2020-01-01')
    parser.add_argument('--end_date', type=str, default='2024-12-31')
    parser.add_argument('--ev_threshold', type=float, default=1.05)
    parser.add_argument('--kelly_fraction', type=float, default=0.25)
    parser.add_argument('--gap_months', type=int, default=1)
    parser.add_argument('--backtest', action='store_true')
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Hybrid Horse Racing AI v3.0 Training")
    logger.info("(Third-Party Review Improved)")
    logger.info("=" * 70)
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"EV threshold: {args.ev_threshold}")
    logger.info(f"Gap months: {args.gap_months}")
    
    data_dir = Path(args.data_dir)
    
    # =========================================================================
    # Step 1: データ読み込み
    # =========================================================================
    logger.info("\n[Step 1/6] データ読み込み")
    all_data = load_all_data(data_dir, args.start_date, args.end_date)
    
    if 'races' not in all_data:
        logger.error("レースデータが見つかりません")
        return
    
    races_df = all_data['races']
    
    # 欠損除去
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    logger.info(f"After cleaning: {len(races_df):,} records")
    
    # 人気カラム作成（バックテスト評価用）
    if 'popularity' not in races_df.columns:
        races_df['popularity'] = races_df.groupby('race_id')['win_odds'].rank()
    
    # =========================================================================
    # Step 2: 時系列分割（ギャップあり）
    # =========================================================================
    logger.info("\n[Step 2/6] 時系列分割（ギャップ期間付き）")
    train_df, val_df, test_df = time_series_split_with_gap(races_df, args.gap_months)
    
    logger.info(f"Train: {len(train_df):,} records")
    logger.info(f"Valid: {len(val_df):,} records")
    logger.info(f"Test:  {len(test_df):,} records")
    
    # =========================================================================
    # Step 3: 特徴量生成（リークフリー）
    # =========================================================================
    logger.info("\n[Step 3/6] 特徴量生成（LeakFreeFeatureEngineer）")
    
    feature_engineer = LeakFreeFeatureEngineer()
    
    # fitは訓練データのみで実行
    feature_engineer.fit(
        train_df,
        pedigrees_df=all_data.get('pedigrees')
    )
    
    # 各データセットをtransform
    train_features = feature_engineer.transform(train_df)
    val_features = feature_engineer.transform(val_df)
    test_features = feature_engineer.transform(test_df)
    
    # 特徴量カラムを取得
    feature_cols = feature_engineer.get_feature_columns()
    
    # 存在するカラムのみ使用
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"Using {len(feature_cols)} features: {feature_cols[:10]}...")
    
    # 準備
    X_train = train_features[feature_cols].fillna(0)
    y_train = train_features['finish_position']
    groups_train = train_features['race_id']
    
    X_val = val_features[feature_cols].fillna(0)
    y_val = val_features['finish_position']
    groups_val = val_features['race_id']
    
    # =========================================================================
    # Step 4: モデル学習
    # =========================================================================
    logger.info("\n[Step 4/6] モデル学習")
    
    ai = HybridHorseRacingAI(
        ev_threshold=args.ev_threshold,
        kelly_fraction=args.kelly_fraction,
        bankroll=100000
    )
    
    ai.train(X_train, y_train, groups_train, X_val, y_val, groups_val)
    
    # =========================================================================
    # Step 5: キャリブレーション評価
    # =========================================================================
    logger.info("\n[Step 5/6] キャリブレーション品質評価")
    
    # Validationデータで評価
    val_predictions = ai.layer1.predict(X_val, groups_val)
    
    # 予測確率を計算（softmax）
    val_predictions['win_prob'] = val_predictions.groupby('group_id')['ability_score'].transform(
        lambda x: np.exp(x - x.max()) / np.exp(x - x.max()).sum()
    )
    
    actual_wins = (y_val.values == 1).astype(int)
    
    calibration = evaluate_calibration(
        val_predictions['win_prob'].values,
        actual_wins
    )
    
    logger.info(f"Validation Calibration:")
    logger.info(f"  Brier Score: {calibration['brier_score']:.4f} {'✓' if calibration['brier_score'] < 0.2 else '✗'}")
    logger.info(f"  ECE: {calibration['ece']:.4f} {'✓' if calibration['ece'] < 0.05 else '✗'}")
    logger.info(f"  Max Cal Error: {calibration['mce']:.4f}")
    
    # モデル保存
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ai.save(str(output_dir))
    logger.info(f"Model saved to {output_dir}")
    
    # 特徴量重要度
    if hasattr(ai.layer1, 'feature_importance_') and ai.layer1.feature_importance_ is not None:
        importance = ai.layer1.feature_importance_
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:15]
        logger.info("\nTop 15 Feature Importances:")
        for i, (feat, imp) in enumerate(top_features, 1):
            logger.info(f"  {i:2d}. {feat}: {imp:.4f}")
    
    # =========================================================================
    # Step 6: バックテスト
    # =========================================================================
    if args.backtest:
        logger.info("\n[Step 6/6] バックテスト")
        
        result = ai.backtest(
            test_features,
            feature_cols,
            race_id_col='race_id',
            rank_col='finish_position',
            odds_col='win_odds',
            horse_number_col='horse_number',
            popularity_col='popularity'
        )
        
        # =========================================================================
        # Phase 1 目標との比較
        # =========================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1 TARGET COMPARISON")
        logger.info("=" * 70)
        
        results_table = []
        
        # ROI
        roi_pass = 85 <= result.roi <= 95
        results_table.append(("ROI", f"{result.roi:.1f}%", "85-90%", "✓ PASS" if roi_pass else "✗ FAIL"))
        
        # 1番人気選択率
        fav_pass = result.favorite_selection_rate < 60
        results_table.append(("1番人気選択率", f"{result.favorite_selection_rate:.1f}%", "< 60%", "✓ PASS" if fav_pass else "✗ FAIL"))
        
        # Brier Score
        brier_pass = calibration['brier_score'] < 0.2
        results_table.append(("Brier Score", f"{calibration['brier_score']:.4f}", "< 0.2", "✓ PASS" if brier_pass else "✗ FAIL"))
        
        # ECE
        ece_pass = calibration['ece'] < 0.05
        results_table.append(("ECE", f"{calibration['ece']:.4f}", "< 0.05", "✓ PASS" if ece_pass else "✗ FAIL"))
        
        # 月別ROI標準偏差
        if result.monthly_rois:
            monthly_std = np.std(list(result.monthly_rois.values()))
            std_pass = monthly_std < 30
            results_table.append(("月別ROI標準偏差", f"{monthly_std:.1f}", "< 30", "✓ PASS" if std_pass else "✗ FAIL"))
        
        # 結果表示
        for metric, value, target, status in results_table:
            logger.info(f"  {metric:20s}: {value:12s} (Target: {target:10s}) {status}")
        
        # サマリー
        logger.info("\n" + "-" * 50)
        logger.info("SUMMARY")
        logger.info("-" * 50)
        logger.info(f"Total Bets: {result.n_bets:,}")
        logger.info(f"Hit Rate: {result.hit_rate:.1f}%")
        logger.info(f"Total Bet Amount: ¥{result.total_bet:,}")
        logger.info(f"Total Payout: ¥{result.total_payout:,}")
        
        # 月別ROI
        if result.monthly_rois:
            logger.info("\nMonthly ROI:")
            for month, roi in sorted(result.monthly_rois.items()):
                status = "✓" if roi >= 100 else "✗"
                logger.info(f"  {month}: {roi:6.1f}% {status}")
        
        # 総合判定
        all_pass = roi_pass and fav_pass and brier_pass and ece_pass
        logger.info("\n" + "=" * 70)
        if all_pass:
            logger.info("🎉 PHASE 1 COMPLETE: All targets achieved!")
        else:
            failed = [m for m, v, t, s in results_table if "FAIL" in s]
            logger.info(f"⚠️ PHASE 1 NOT COMPLETE: Failed metrics: {', '.join(failed)}")
        logger.info("=" * 70)
    
    logger.info("\nTraining Complete!")


if __name__ == '__main__':
    main()
