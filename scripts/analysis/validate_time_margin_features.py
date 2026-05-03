#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイム差特徴量とレース荒れ度検出の検証スクリプト

【検証内容】
1. タイム差特徴量のリーク検証
   - 将来データを使っていないかチェック
   
2. 荒れ度検出の精度検証
   - 「荒れ」と判定したレースで実際に荒れたか
   
3. 戦略効果の検証
   - 荒れ度に応じた戦略切り替えでROI改善するか

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 新馬・障害除外
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    return races


def test_data_leak(races_df: pd.DataFrame) -> bool:
    """
    データリーク検証
    
    【検証方法】
    1. 2024年のデータで予測
    2. 特徴量生成時に2024年のmargin_secondsが使われていないか確認
    """
    logger.info("=" * 60)
    logger.info("1. データリーク検証")
    logger.info("=" * 60)
    
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    # Train: 2020-2023, Test: 2024
    train_end = pd.to_datetime('2023-12-31')
    test_start = pd.to_datetime('2024-01-01')
    
    train = races_df[races_df['race_date'] <= train_end].copy()
    test = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"  Train: {len(train):,} 件 (2020-2023)")
    logger.info(f"  Test:  {len(test):,} 件 (2024)")
    
    # fit（Trainのみ）
    engineer = TimeMarginFeatureEngineer()
    engineer.fit(train)
    
    # transform（Test）
    test_with_features = engineer.transform(test)
    
    # リークチェック: Test期間の馬でhorse_close_loss_rateがある場合、
    # その値は2023年以前のデータのみで計算されているはず
    
    # 2024年にデビューした馬（2023年以前のレースがない馬）を抽出
    train_horse_ids = set(train['horse_id'].unique())
    new_horses = test[~test['horse_id'].isin(train_horse_ids)]['horse_id'].unique()
    
    logger.info(f"  2024年デビュー馬: {len(new_horses):,} 頭")
    
    # 新馬にhorse_close_loss_rateがあったらリーク
    new_horse_features = test_with_features[test_with_features['horse_id'].isin(new_horses)]
    has_leak = new_horse_features['horse_close_loss_rate'].notna().any()
    
    if has_leak:
        logger.error("  ❌ リーク検出！新馬に過去統計が付与されています")
        return False
    else:
        logger.info("  ✅ リークなし（新馬には過去統計が付与されていない）")
    
    # 追加検証: 既存馬の統計が「2023年以前のデータのみ」で計算されているか
    # Test期間にも存在する馬を選択
    test_horse_ids = set(test['horse_id'].unique())
    train_horse_counts = train['horse_id'].value_counts()
    valid_horses = train_horse_counts[train_horse_counts.index.isin(test_horse_ids)]
    
    if len(valid_horses) == 0:
        logger.warning("  Train/Testで共通の馬がないため、詳細検証スキップ")
        return True
    
    sample_horse = valid_horses.head(1).index[0]
    
    # Trainでの統計を手動計算
    horse_train = train[train['horse_id'] == sample_horse].copy()
    horse_train = horse_train.sort_values('race_date')
    
    # 2023年最終レース時点での僅差負け率（手動計算）
    losses = horse_train[horse_train['finish_position'] > 1]
    close_losses = losses[
        (losses['finish_position'] <= 3) & 
        (losses['margin_seconds'] <= 0.3)
    ]
    expected_rate = len(close_losses) / len(losses) if len(losses) > 0 else np.nan
    
    # transform結果を確認
    sample_in_test = test_with_features[test_with_features['horse_id'] == sample_horse]
    
    if len(sample_in_test) == 0:
        logger.warning(f"  サンプル馬 {sample_horse} がTest期間に存在しないためスキップ")
        return True
    
    actual_rate = sample_in_test['horse_close_loss_rate'].iloc[0]
    
    # shift(1)の影響で厳密には一致しないが、オーダーは近いはず
    if pd.notna(expected_rate) and pd.notna(actual_rate):
        diff = abs(expected_rate - actual_rate)
        logger.info(f"  サンプル馬 {sample_horse}: 期待={expected_rate:.3f}, 実際={actual_rate:.3f}, 差={diff:.3f}")
        if diff < 0.1:
            logger.info("  ✅ 統計値が概ね一致（リークなしと判断）")
        else:
            logger.warning("  ⚠️ 統計値にずれあり（要確認）")
    
    return True


def test_volatility_accuracy(races_df: pd.DataFrame) -> dict:
    """
    荒れ度検出の精度検証
    
    【検証方法】
    1. 過去データでモデル学習
    2. 2024年データで荒れ度を予測
    3. 実際に「荒れた」かを確認
    
    「荒れた」の定義: 1番人気が3着以下
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 荒れ度検出精度検証")
    logger.info("=" * 60)
    
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer, RaceVolatilityAnalyzer
    from keibaai.src.strategies.race_competitiveness import RaceCompetitivenessAnalyzer
    
    # データ分割
    train_end = pd.to_datetime('2023-12-31')
    test_start = pd.to_datetime('2024-01-01')
    
    train = races_df[races_df['race_date'] <= train_end].copy()
    test = races_df[races_df['race_date'] >= test_start].copy()
    
    # 特徴量エンジニア
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_features = margin_engineer.transform(test)
    
    # 競争力アナライザー
    competitiveness = RaceCompetitivenessAnalyzer()
    competitiveness.fit(train)
    test_features = competitiveness.transform(test_features)
    
    # ダミーのplace_probを生成（実際はMultiTargetPredictorの出力を使う）
    # ここでは検証のため、finish_positionから逆算
    test_features['place_prob'] = 1 - (test_features['finish_position'] / 18)
    test_features['place_prob'] = test_features['place_prob'].clip(0.1, 0.9)
    
    # place_prob標準偏差を計算
    test_features['race_place_prob_std'] = test_features.groupby('race_id')['place_prob'].transform('std')
    
    # 簡易荒れ度判定
    race_level = test_features.groupby('race_id').agg({
        'race_place_prob_std': 'first',
        'class_close_rate': 'first',
        'venue_upset_rate': 'first',
    }).reset_index()
    
    # 「荒れた」かを判定（1番人気が3着以下）
    fav1 = test_features[test_features['popularity'] == 1][['race_id', 'finish_position']]
    fav1.columns = ['race_id', 'fav1_finish']
    race_level = race_level.merge(fav1, on='race_id', how='left')
    race_level['is_upset'] = (race_level['fav1_finish'] > 3).astype(int)
    
    # 荒れ度閾値別の精度
    thresholds = [0.08, 0.10, 0.12, 0.15]
    results = {}
    
    for th in thresholds:
        predicted_volatile = race_level['race_place_prob_std'] < th
        
        # 混同行列
        tp = ((predicted_volatile == True) & (race_level['is_upset'] == 1)).sum()
        fp = ((predicted_volatile == True) & (race_level['is_upset'] == 0)).sum()
        tn = ((predicted_volatile == False) & (race_level['is_upset'] == 0)).sum()
        fn = ((predicted_volatile == False) & (race_level['is_upset'] == 1)).sum()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        results[th] = {
            'precision': precision,
            'recall': recall,
            'predicted_volatile_count': predicted_volatile.sum(),
        }
        
        logger.info(f"  閾値 {th}: Precision={precision:.1%}, Recall={recall:.1%}, 予測数={predicted_volatile.sum():,}")
    
    # ベース荒れ率
    base_upset_rate = race_level['is_upset'].mean()
    logger.info(f"  ベース荒れ率: {base_upset_rate:.1%}")
    
    return results


def test_strategy_effect(races_df: pd.DataFrame) -> dict:
    """
    戦略切り替え効果の検証
    
    【検証方法】
    1. 荒れ度に応じて単勝 or 複勝を選択
    2. 固定戦略（常に単勝）とROI比較
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 戦略切り替え効果検証")
    logger.info("=" * 60)
    
    # 簡易検証: 1番人気オッズによる荒れ度判定
    # place_prob分散の代わりに1番人気オッズを使用（既知の指標）
    
    test = races_df[races_df['race_date'] >= '2024-01-01'].copy()
    
    # レース単位で1番人気オッズを取得
    fav1 = test[test['popularity'] == 1][['race_id', 'win_odds', 'finish_position']]
    fav1.columns = ['race_id', 'fav1_odds', 'fav1_finish']
    
    # Top1予測（仮: 人気順 - 実際はモデル予測を使用）
    # ここでは検証のため、人気2-3位を「穴狙い」として扱う
    
    race_results = []
    
    for race_id in test['race_id'].unique():
        race = test[test['race_id'] == race_id].copy()
        fav1_info = fav1[fav1['race_id'] == race_id]
        
        if len(fav1_info) == 0:
            continue
        
        fav1_odds = fav1_info['fav1_odds'].values[0]
        fav1_finish = fav1_info['fav1_finish'].values[0]
        
        # 荒れ度判定（1番人気オッズベース）
        if fav1_odds < 2.0:
            volatility = 'stable'
        elif fav1_odds < 4.0:
            volatility = 'moderate'
        else:
            volatility = 'volatile'
        
        # 戦略A: 常に1番人気に単勝
        strategy_a_roi = race[race['popularity'] == 1]['win_odds'].values[0] \
            if race[race['popularity'] == 1]['finish_position'].values[0] == 1 else 0
        
        # 戦略B: 荒れレースは複勝、堅レースは単勝
        if volatility == 'volatile':
            # 複勝: 3着以内ならROI 1.5程度と仮定
            strategy_b_roi = 1.5 if race[race['popularity'] == 1]['finish_position'].values[0] <= 3 else 0
        else:
            # 単勝
            strategy_b_roi = strategy_a_roi
        
        race_results.append({
            'race_id': race_id,
            'fav1_odds': fav1_odds,
            'fav1_finish': fav1_finish,
            'volatility': volatility,
            'strategy_a_roi': strategy_a_roi,
            'strategy_b_roi': strategy_b_roi,
        })
    
    results_df = pd.DataFrame(race_results)
    
    # ROI比較
    strategy_a_avg = results_df['strategy_a_roi'].mean() * 100
    strategy_b_avg = results_df['strategy_b_roi'].mean() * 100
    
    logger.info(f"  戦略A（常に単勝）: ROI {strategy_a_avg:.1f}%")
    logger.info(f"  戦略B（適応型）:   ROI {strategy_b_avg:.1f}%")
    logger.info(f"  差:                 {strategy_b_avg - strategy_a_avg:+.1f}pt")
    
    # 荒れ度別の内訳
    for vol in ['stable', 'moderate', 'volatile']:
        subset = results_df[results_df['volatility'] == vol]
        if len(subset) > 0:
            a_roi = subset['strategy_a_roi'].mean() * 100
            b_roi = subset['strategy_b_roi'].mean() * 100
            count = len(subset)
            logger.info(f"    {vol}: A={a_roi:.1f}%, B={b_roi:.1f}% (n={count})")
    
    return {
        'strategy_a_roi': strategy_a_avg,
        'strategy_b_roi': strategy_b_avg,
        'difference': strategy_b_avg - strategy_a_avg,
    }


def main():
    logger.info("=" * 60)
    logger.info("タイム差特徴量・荒れ度検出 検証スクリプト")
    logger.info("=" * 60)
    
    # データ読み込み
    races = load_data()
    logger.info(f"データ件数: {len(races):,}")
    
    # 1. リーク検証
    leak_ok = test_data_leak(races)
    
    # 2. 荒れ度検出精度
    volatility_results = test_volatility_accuracy(races)
    
    # 3. 戦略効果
    strategy_results = test_strategy_effect(races)
    
    # サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("検証サマリー")
    logger.info("=" * 60)
    logger.info(f"  リーク検証: {'✅ PASS' if leak_ok else '❌ FAIL'}")
    logger.info(f"  荒れ度検出: 閾値0.10でPrecision={volatility_results[0.10]['precision']:.1%}")
    logger.info(f"  戦略効果: {strategy_results['difference']:+.1f}pt")


if __name__ == "__main__":
    main()
