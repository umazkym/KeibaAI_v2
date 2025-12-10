"""
新特徴量候補 データ分析スクリプト

【分析対象】
1. 騎手×馬の相性（jockey_horse_synergy）
2. 調教師の休養明けパターン（trainer_rest_pattern）
3. 距離変更パフォーマンス（distance_change_performance）

【分析観点】
- 人気/オッズとの相関（低いほど有望）
- ROI改善可能性
- カバレッジ（データ量）
- リークリスク

使用方法:
python scripts/analysis/analyze_new_features.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    logger.info("=" * 60)
    logger.info("データ読み込み")
    logger.info("=" * 60)
    
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    
    logger.info(f"  レース: {len(races):,}件")
    return races


def prepare_data(races):
    """データ前処理"""
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.sort_values(['race_date', 'race_id', 'horse_number'])
    
    races['is_win'] = (races['finish_position'] == 1).astype(int)
    
    # 前走情報
    races['prev_jockey'] = races.groupby('horse_id')['jockey_id'].shift(1)
    races['days_since_last'] = races.groupby('horse_id')['race_date'].diff().dt.days
    races['prev_distance'] = races.groupby('horse_id')['distance_m'].shift(1)
    races['distance_change'] = races['distance_m'] - races['prev_distance']
    
    # Train/Test分割
    train = races[races['race_date'] < '2024-01-01'].copy()
    test = races[(races['race_date'] >= '2024-07-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件, Test: {len(test):,}件")
    return train, test


def analyze_jockey_horse_synergy(train, test):
    """騎手×馬の相性分析"""
    logger.info("\n" + "=" * 60)
    logger.info("1. 騎手×馬の相性（jockey_horse_synergy）")
    logger.info("=" * 60)
    
    # 継続騎乗 vs 騎手変更
    train['is_same_jockey'] = (train['jockey_id'] == train['prev_jockey']).astype(int)
    test['is_same_jockey'] = (test['jockey_id'] == test['prev_jockey']).astype(int)
    
    # 継続騎乗の基本統計
    same_jockey = train[train['is_same_jockey'] == 1]
    diff_jockey = train[train['is_same_jockey'] == 0]
    
    logger.info("\n  【Train期間】継続騎乗 vs 騎手変更:")
    logger.info(f"    継続騎乗: 件数={len(same_jockey):,}, 勝率={same_jockey['is_win'].mean()*100:.2f}%")
    logger.info(f"    騎手変更: 件数={len(diff_jockey):,}, 勝率={diff_jockey['is_win'].mean()*100:.2f}%")
    
    # ROI計算
    same_roi = (same_jockey['is_win'] * same_jockey['win_odds']).sum() / len(same_jockey) * 100
    diff_roi = (diff_jockey['is_win'] * diff_jockey['win_odds']).sum() / len(diff_jockey) * 100
    logger.info(f"    継続騎乗ROI: {same_roi:.1f}%, 騎手変更ROI: {diff_roi:.1f}%")
    
    # 騎手×馬コンビの過去勝率
    logger.info("\n  【騎手×馬コンビの過去勝率】")
    
    # trainで累積統計を構築
    train['jh_key'] = train['jockey_id'] + '_' + train['horse_id']
    train['jh_cum_wins'] = train.groupby('jh_key')['is_win'].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )
    train['jh_cum_races'] = train.groupby('jh_key')['is_win'].transform(
        lambda x: x.expanding().count().shift(1).fillna(0)
    )
    train['jh_win_rate'] = np.where(
        train['jh_cum_races'] >= 2,
        train['jh_cum_wins'] / train['jh_cum_races'],
        np.nan
    )
    
    # カバレッジ
    valid = train[train['jh_win_rate'].notna()]
    logger.info(f"    カバレッジ: {len(valid):,}/{len(train):,} ({len(valid)/len(train)*100:.1f}%)")
    
    # 人気との相関
    corr_pop = valid['jh_win_rate'].corr(valid['popularity'])
    corr_odds = valid['jh_win_rate'].corr(valid['win_odds'])
    logger.info(f"    人気との相関: {corr_pop:.3f}")
    logger.info(f"    オッズとの相関: {corr_odds:.3f}")
    
    # ROI by jh_win_rate
    valid['jh_cat'] = pd.cut(
        valid['jh_win_rate'],
        bins=[-0.01, 0.2, 0.4, 0.6, 0.8, 1.01],
        labels=['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
    )
    
    logger.info("\n  騎手×馬コンビ勝率別 ROI:")
    stats = valid.groupby('jh_cat', observed=True).agg(
        count=('is_win', 'count'),
        win_rate=('is_win', 'mean'),
        avg_odds=('win_odds', 'mean'),
        avg_pop=('popularity', 'mean'),
    ).reset_index()
    stats['roi'] = stats['win_rate'] * stats['avg_odds'] * 100
    print(stats.to_string(index=False))
    
    return train


def analyze_trainer_rest_pattern(train, test):
    """調教師の休養明けパターン分析"""
    logger.info("\n" + "=" * 60)
    logger.info("2. 調教師の休養明けパターン（trainer_rest_pattern）")
    logger.info("=" * 60)
    
    # 休養明け定義: 60日以上
    train['is_rest'] = (train['days_since_last'] >= 60).astype(int)
    
    # 全体の休養明け成績
    rest = train[train['is_rest'] == 1]
    no_rest = train[train['is_rest'] == 0]
    
    logger.info("\n  【Train期間】休養明け vs 通常:")
    logger.info(f"    休養明け(60日+): 件数={len(rest):,}, 勝率={rest['is_win'].mean()*100:.2f}%")
    logger.info(f"    通常: 件数={len(no_rest):,}, 勝率={no_rest['is_win'].mean()*100:.2f}%")
    
    # 調教師別休養明け勝率
    trainer_rest_stats = (
        train[train['is_rest'] == 1]
        .groupby('trainer_id')
        .agg(
            rest_wins=('is_win', 'sum'),
            rest_races=('is_win', 'count')
        )
        .reset_index()
    )
    trainer_rest_stats = trainer_rest_stats[trainer_rest_stats['rest_races'] >= 10]
    trainer_rest_stats['rest_win_rate'] = trainer_rest_stats['rest_wins'] / trainer_rest_stats['rest_races']
    
    logger.info(f"\n  調教師別休養明け勝率（10回以上）: {len(trainer_rest_stats)}調教師")
    logger.info(f"    平均勝率: {trainer_rest_stats['rest_win_rate'].mean()*100:.2f}%")
    logger.info(f"    最高勝率: {trainer_rest_stats['rest_win_rate'].max()*100:.2f}%")
    logger.info(f"    最低勝率: {trainer_rest_stats['rest_win_rate'].min()*100:.2f}%")
    
    # 上位調教師の休養明けROI
    top_trainers = trainer_rest_stats.nlargest(20, 'rest_win_rate')['trainer_id'].tolist()
    train['is_top_rest_trainer'] = train['trainer_id'].isin(top_trainers).astype(int)
    
    top_rest = train[(train['is_rest'] == 1) & (train['is_top_rest_trainer'] == 1)]
    other_rest = train[(train['is_rest'] == 1) & (train['is_top_rest_trainer'] == 0)]
    
    if len(top_rest) > 0:
        top_roi = (top_rest['is_win'] * top_rest['win_odds']).sum() / len(top_rest) * 100
        other_roi = (other_rest['is_win'] * other_rest['win_odds']).sum() / len(other_rest) * 100
        logger.info(f"\n  【休養明け上位調教師TOP20】")
        logger.info(f"    上位調教師ROI: {top_roi:.1f}%")
        logger.info(f"    その他調教師ROI: {other_roi:.1f}%")
        
        # 人気との相関をチェック
        corr = top_rest['win_odds'].corr(train[train['is_rest'] == 1]['popularity'])
        logger.info(f"    人気との相関: {corr:.3f}")
    
    return train


def analyze_distance_change(train, test):
    """距離変更パフォーマンス分析"""
    logger.info("\n" + "=" * 60)
    logger.info("3. 距離変更パフォーマンス（distance_change_performance）")
    logger.info("=" * 60)
    
    # 距離変更カテゴリ
    train['dist_change_cat'] = pd.cut(
        train['distance_change'],
        bins=[-np.inf, -400, -200, 0, 200, 400, np.inf],
        labels=['大短縮(<-400)', '短縮(-400~-200)', '微短縮(-200~0)', '微延長(0~200)', '延長(200~400)', '大延長(>400)']
    )
    
    valid = train[train['dist_change_cat'].notna()]
    
    logger.info("\n  距離変更カテゴリ別 勝率・ROI:")
    stats = valid.groupby('dist_change_cat', observed=True).agg(
        count=('is_win', 'count'),
        win_rate=('is_win', 'mean'),
        avg_odds=('win_odds', 'mean'),
        avg_pop=('popularity', 'mean'),
    ).reset_index()
    stats['win_rate_pct'] = stats['win_rate'] * 100
    stats['roi'] = stats['win_rate'] * stats['avg_odds'] * 100
    print(stats.to_string(index=False))
    
    # 馬ごとの距離延長時勝率を計算
    logger.info("\n  【馬ごとの距離延長時勝率】")
    
    # 距離延長(+200m以上)のフラグ
    train['is_dist_up'] = (train['distance_change'] >= 200).astype(int)
    train['is_dist_down'] = (train['distance_change'] <= -200).astype(int)
    
    # 累積統計
    train['horse_dist_up_wins'] = train.groupby('horse_id').apply(
        lambda g: (g['is_win'] * g['is_dist_up']).cumsum().shift(1).fillna(0)
    ).reset_index(level=0, drop=True)
    train['horse_dist_up_races'] = train.groupby('horse_id').apply(
        lambda g: g['is_dist_up'].cumsum().shift(1).fillna(0)
    ).reset_index(level=0, drop=True)
    
    train['horse_dist_up_win_rate'] = np.where(
        train['horse_dist_up_races'] >= 2,
        train['horse_dist_up_wins'] / train['horse_dist_up_races'],
        np.nan
    )
    
    # カバレッジ
    valid_dist = train[train['horse_dist_up_win_rate'].notna()]
    logger.info(f"    カバレッジ: {len(valid_dist):,}/{len(train):,} ({len(valid_dist)/len(train)*100:.1f}%)")
    
    if len(valid_dist) > 0:
        # 人気との相関
        corr_pop = valid_dist['horse_dist_up_win_rate'].corr(valid_dist['popularity'])
        corr_odds = valid_dist['horse_dist_up_win_rate'].corr(valid_dist['win_odds'])
        logger.info(f"    人気との相関: {corr_pop:.3f}")
        logger.info(f"    オッズとの相関: {corr_odds:.3f}")
    
    return train


def analyze_feature_potential(train):
    """特徴量候補のポテンシャル総括"""
    logger.info("\n" + "=" * 60)
    logger.info("特徴量候補 ポテンシャル総括")
    logger.info("=" * 60)
    
    summary = []
    
    # 1. jockey_horse_synergy
    if 'jh_win_rate' in train.columns:
        valid = train[train['jh_win_rate'].notna()]
        coverage = len(valid) / len(train) * 100
        corr = valid['jh_win_rate'].corr(valid['popularity'])
        summary.append({
            'feature': 'jockey_horse_synergy',
            'coverage': f"{coverage:.1f}%",
            'pop_corr': f"{corr:.3f}",
            'assessment': '⚠️ 要検証' if abs(corr) > 0.2 else '✅ 有望'
        })
    
    # 2. trainer_rest_pattern
    summary.append({
        'feature': 'trainer_rest_pattern',
        'coverage': 'fit時固定',
        'pop_corr': '低（推定）',
        'assessment': '✅ 有望'
    })
    
    # 3. distance_change_performance
    if 'horse_dist_up_win_rate' in train.columns:
        valid = train[train['horse_dist_up_win_rate'].notna()]
        coverage = len(valid) / len(train) * 100
        if len(valid) > 0:
            corr = valid['horse_dist_up_win_rate'].corr(valid['popularity'])
        else:
            corr = 0
        summary.append({
            'feature': 'distance_change_performance',
            'coverage': f"{coverage:.1f}%",
            'pop_corr': f"{corr:.3f}",
            'assessment': '⚠️ カバレッジ低' if coverage < 20 else '✅ 有望'
        })
    
    logger.info("\n  特徴量候補サマリー:")
    for s in summary:
        logger.info(f"    {s['feature']}: カバレッジ={s['coverage']}, 人気相関={s['pop_corr']}, 評価={s['assessment']}")


def main():
    """メイン処理"""
    logger.info("新特徴量候補 データ分析開始")
    
    # データ読み込み
    races = load_data()
    train, test = prepare_data(races)
    
    # 分析実行
    train = analyze_jockey_horse_synergy(train, test)
    train = analyze_trainer_rest_pattern(train, test)
    train = analyze_distance_change(train, test)
    
    # 総括
    analyze_feature_potential(train)
    
    logger.info("\n" + "=" * 60)
    logger.info("分析完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
