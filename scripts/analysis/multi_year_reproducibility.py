#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
複数年再現性検証 & 個別レース深層分析

【目的】
1. 最適化パラメータの5年間再現性を検証
2. 個別レースを分析してパターンを発見
3. さらなる改善ポイントを特定

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


# 最適化されたパラメータ
OPTIMIZED_VOLATILE_TH = 0.08
OPTIMIZED_MODERATE_TH = 0.20
OPTIMIZED_V44_VOLATILE_W = 0.35
OPTIMIZED_V44_MODERATE_W = 0.30
OPTIMIZED_V44_STABLE_W = 0.25


def run_year_prediction(races, pedigrees, corners, race_details, horses, returns, test_year):
    """1年分の予測を実行"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_start = f'{test_year}-01-01'
    test_end = f'{test_year}-12-31'
    test_start_dt = pd.to_datetime(test_start)
    test_end_dt = pd.to_datetime(test_end)
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    if len(test) < 5000:
        return None
    
    # 特徴量エンジン
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # タイム差特徴量
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_f = margin_engineer.transform(test_f)
    
    # モデル学習
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    # 予測
    test_preds = predictor.predict(test_f)
    
    # 実績データをマージ
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                'horse_close_loss_rate', 'horse_margin_std', 'race_name', 'venue', 'race_date']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 払戻をマージ
    fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    test_preds = test_preds.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    test_preds['fukusho_payout'] = test_preds['payout'].fillna(0) / 100
    
    test_preds['year'] = test_year
    
    return test_preds


def apply_optimized_params(preds_df):
    """最適化パラメータを適用"""
    df = preds_df.copy()
    
    # 荒れ度を再分類（最適化パラメータ使用）
    df['optimized_vol_type'] = np.where(
        df['race_place_prob_std'] < OPTIMIZED_VOLATILE_TH, 'volatile',
        np.where(df['race_place_prob_std'] < OPTIMIZED_MODERATE_TH, 'moderate', 'stable')
    )
    
    # V4.4ウェイト
    df['opt_v44_w'] = np.where(
        df['optimized_vol_type'] == 'volatile', OPTIMIZED_V44_VOLATILE_W,
        np.where(df['optimized_vol_type'] == 'moderate', OPTIMIZED_V44_MODERATE_W, OPTIMIZED_V44_STABLE_W)
    )
    
    # 適応型スコア（最適化版）
    df['opt_adaptive_score'] = (
        df['win_prob'] * (1 - df['opt_v44_w'] - 0.20) +
        df['v44_score'] * df['opt_v44_w'] +
        df['top2_prob'] * 0.10 +
        df['place_prob'] * 0.10
    )
    
    # ランキング
    df['rank_opt'] = df.groupby('race_id')['opt_adaptive_score'].rank(ascending=False, method='first')
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    return df


def analyze_by_year(preds_df):
    """年別のROI分析"""
    results = []
    
    for year in sorted(preds_df['year'].unique()):
        year_df = preds_df[preds_df['year'] == year]
        
        # 最適化パラメータ適用
        year_df = apply_optimized_params(year_df)
        
        # Top1を取得
        top1_opt = year_df[year_df['rank_opt'] == 1]
        top1_ens = year_df[year_df['rank_ensemble'] == 1]
        top1_place = year_df[year_df['rank_place'] == 1]
        
        # 単勝ROI（最適化版）
        hits_opt = top1_opt[top1_opt['finish_position'] == 1]
        opt_roi = hits_opt['win_odds'].sum() / len(top1_opt) * 100 if len(top1_opt) > 0 else 0
        
        # 単勝ROI（従来）
        hits_ens = top1_ens[top1_ens['finish_position'] == 1]
        ens_roi = hits_ens['win_odds'].sum() / len(top1_ens) * 100 if len(top1_ens) > 0 else 0
        
        # 複勝ROI
        place_roi = top1_place['fukusho_payout'].sum() / len(top1_place) * 100 if len(top1_place) > 0 else 0
        
        # 荒れ度別ROI（最適化パラメータ）
        stable_top1 = top1_opt[top1_opt['optimized_vol_type'] == 'stable']
        stable_hits = stable_top1[stable_top1['finish_position'] == 1]
        stable_roi = stable_hits['win_odds'].sum() / len(stable_top1) * 100 if len(stable_top1) > 0 else 0
        
        volatile_top1 = top1_opt[top1_opt['optimized_vol_type'] == 'volatile']
        volatile_hits = volatile_top1[volatile_top1['finish_position'] == 1]
        volatile_roi = volatile_hits['win_odds'].sum() / len(volatile_top1) * 100 if len(volatile_top1) > 0 else 0
        
        moderate_top1 = top1_opt[top1_opt['optimized_vol_type'] == 'moderate']
        moderate_hits = moderate_top1[moderate_top1['finish_position'] == 1]
        moderate_roi = moderate_hits['win_odds'].sum() / len(moderate_top1) * 100 if len(moderate_top1) > 0 else 0
        
        results.append({
            'year': year,
            'opt_roi': opt_roi,
            'ens_roi': ens_roi,
            'place_roi': place_roi,
            'stable_roi': stable_roi,
            'stable_count': len(stable_top1),
            'volatile_roi': volatile_roi,
            'volatile_count': len(volatile_top1),
            'moderate_roi': moderate_roi,
            'moderate_count': len(moderate_top1),
        })
    
    return pd.DataFrame(results)


def analyze_individual_races(preds_df, sample_size=50):
    """
    個別レース分析
    
    的中/外れの両方をサンプリングしてパターンを発見
    """
    logger.info("\n" + "="*70)
    logger.info("個別レース深層分析")
    logger.info("="*70)
    
    df = apply_optimized_params(preds_df)
    top1 = df[df['rank_opt'] == 1].copy()
    
    # 的中レース
    hits = top1[top1['finish_position'] == 1].copy()
    hits['is_hit'] = True
    
    # 外れレース
    misses = top1[top1['finish_position'] != 1].copy()
    misses['is_hit'] = False
    
    # 高配当的中
    high_odds_hits = hits[hits['win_odds'] >= 5.0].head(sample_size // 2)
    # 低配当的中
    low_odds_hits = hits[hits['win_odds'] < 5.0].head(sample_size // 4)
    # 人気馬外れ
    fav_misses = misses[misses['popularity'] <= 3].head(sample_size // 4)
    
    samples = pd.concat([high_odds_hits, low_odds_hits, fav_misses])
    
    logger.info(f"\n【サンプルレース分析】（{len(samples)}件）")
    logger.info("-" * 70)
    
    patterns = {
        'high_odds_hit': [],   # 高配当的中のパターン
        'low_odds_hit': [],    # 低配当的中のパターン
        'fav_miss': [],        # 人気馬外れのパターン
    }
    
    for _, row in samples.iterrows():
        race_id = row['race_id']
        race_df = df[df['race_id'] == race_id].sort_values('rank_opt')
        
        # レース情報
        vol_type = row['optimized_vol_type']
        place_prob_std = row['race_place_prob_std']
        predicted_rank = int(row['rank_opt'])
        actual_rank = int(row['finish_position']) if pd.notna(row['finish_position']) else '-'
        odds = row['win_odds']
        pop = int(row['popularity']) if pd.notna(row['popularity']) else '-'
        
        # 特徴量
        v44_score = row['v44_score']
        win_prob = row['win_prob']
        place_prob = row['place_prob']
        
        # 分類
        if row['is_hit'] and odds >= 5.0:
            patterns['high_odds_hit'].append({
                'vol_type': vol_type,
                'place_prob_std': place_prob_std,
                'v44_score': v44_score,
                'win_prob': win_prob,
                'odds': odds,
                'pop': pop,
            })
        elif row['is_hit'] and odds < 5.0:
            patterns['low_odds_hit'].append({
                'vol_type': vol_type,
                'place_prob_std': place_prob_std,
                'v44_score': v44_score,
                'win_prob': win_prob,
                'odds': odds,
                'pop': pop,
            })
        else:
            patterns['fav_miss'].append({
                'vol_type': vol_type,
                'place_prob_std': place_prob_std,
                'v44_score': v44_score,
                'win_prob': win_prob,
                'odds': odds,
                'pop': pop,
                'actual_rank': actual_rank,
            })
    
    return patterns


def find_improvement_opportunities(preds_df):
    """
    改善機会の特定
    
    外れレースを分析して共通パターンを発見
    """
    logger.info("\n" + "="*70)
    logger.info("改善機会の特定")
    logger.info("="*70)
    
    df = apply_optimized_params(preds_df)
    top1 = df[df['rank_opt'] == 1].copy()
    
    # 外れレースの特徴分析
    misses = top1[top1['finish_position'] != 1].copy()
    hits = top1[top1['finish_position'] == 1].copy()
    
    # 1. 荒れ度別の的中率
    logger.info("\n【荒れ度別 的中率】")
    for vol_type in ['volatile', 'moderate', 'stable']:
        vol_top1 = top1[top1['optimized_vol_type'] == vol_type]
        vol_hits = vol_top1[vol_top1['finish_position'] == 1]
        hit_rate = len(vol_hits) / len(vol_top1) * 100 if len(vol_top1) > 0 else 0
        logger.info(f"  {vol_type}: {hit_rate:.1f}% ({len(vol_hits)}/{len(vol_top1)})")
    
    # 2. 人気別の的中率
    logger.info("\n【人気別 的中率】")
    for pop_range in [(1, 1), (2, 3), (4, 6), (7, 18)]:
        pop_top1 = top1[(top1['popularity'] >= pop_range[0]) & (top1['popularity'] <= pop_range[1])]
        pop_hits = pop_top1[pop_top1['finish_position'] == 1]
        hit_rate = len(pop_hits) / len(pop_top1) * 100 if len(pop_top1) > 0 else 0
        roi = pop_hits['win_odds'].sum() / len(pop_top1) * 100 if len(pop_top1) > 0 else 0
        logger.info(f"  人気{pop_range[0]}-{pop_range[1]}: 的中率 {hit_rate:.1f}%, ROI {roi:.1f}% (n={len(pop_top1)})")
    
    # 3. オッズ範囲別のROI
    logger.info("\n【オッズ範囲別 ROI】")
    for odds_range in [(1.0, 2.0), (2.0, 4.0), (4.0, 8.0), (8.0, 20.0), (20.0, 100.0)]:
        odds_top1 = top1[(top1['win_odds'] >= odds_range[0]) & (top1['win_odds'] < odds_range[1])]
        odds_hits = odds_top1[odds_top1['finish_position'] == 1]
        roi = odds_hits['win_odds'].sum() / len(odds_top1) * 100 if len(odds_top1) > 0 else 0
        hit_rate = len(odds_hits) / len(odds_top1) * 100 if len(odds_top1) > 0 else 0
        logger.info(f"  {odds_range[0]:.1f}-{odds_range[1]:.1f}倍: ROI {roi:.1f}%, 的中率 {hit_rate:.1f}% (n={len(odds_top1)})")
    
    # 4. 会場別のROI
    logger.info("\n【会場別 ROI（Top5/Worst5）】")
    venue_stats = []
    for venue in top1['venue'].unique():
        venue_top1 = top1[top1['venue'] == venue]
        venue_hits = venue_top1[venue_top1['finish_position'] == 1]
        roi = venue_hits['win_odds'].sum() / len(venue_top1) * 100 if len(venue_top1) > 0 else 0
        venue_stats.append({'venue': venue, 'roi': roi, 'count': len(venue_top1)})
    
    venue_df = pd.DataFrame(venue_stats).sort_values('roi', ascending=False)
    logger.info("  【高ROI】")
    for _, row in venue_df.head(5).iterrows():
        if row['count'] >= 50:
            logger.info(f"    {row['venue']}: ROI {row['roi']:.1f}% (n={row['count']})")
    
    logger.info("  【低ROI】")
    for _, row in venue_df.tail(5).iterrows():
        if row['count'] >= 50:
            logger.info(f"    {row['venue']}: ROI {row['roi']:.1f}% (n={row['count']})")
    
    # 5. V4.4スコアが高い時の的中率
    logger.info("\n【V4.4スコア四分位別 ROI】")
    try:
        top1['v44_quartile'] = pd.qcut(top1['v44_score'], q=4, labels=False, duplicates='drop')
        for q in sorted(top1['v44_quartile'].dropna().unique()):
            q_top1 = top1[top1['v44_quartile'] == q]
            q_hits = q_top1[q_top1['finish_position'] == 1]
            roi = q_hits['win_odds'].sum() / len(q_top1) * 100 if len(q_top1) > 0 else 0
            label = {0: 'Q1(低)', 1: 'Q2', 2: 'Q3', 3: 'Q4(高)'}.get(q, f'Q{q}')
            logger.info(f"  {label}: ROI {roi:.1f}% (n={len(q_top1)})")
    except Exception as e:
        logger.warning(f"  V4.4分析エラー: {e}")
    
    # 6. stable×高V4.4の組み合わせ
    logger.info("\n【stable × V4.4高スコアの効果】")
    stable_top1 = top1[top1['optimized_vol_type'] == 'stable'].copy()
    if len(stable_top1) > 0:
        try:
            v44_median = stable_top1['v44_score'].median()
            high_v44_stable = stable_top1[stable_top1['v44_score'] >= v44_median]
            low_v44_stable = stable_top1[stable_top1['v44_score'] < v44_median]
            
            high_hits = high_v44_stable[high_v44_stable['finish_position'] == 1]
            high_roi = high_hits['win_odds'].sum() / len(high_v44_stable) * 100 if len(high_v44_stable) > 0 else 0
            
            low_hits = low_v44_stable[low_v44_stable['finish_position'] == 1]
            low_roi = low_hits['win_odds'].sum() / len(low_v44_stable) * 100 if len(low_v44_stable) > 0 else 0
            
            logger.info(f"  stable + V4.4高: ROI {high_roi:.1f}% (n={len(high_v44_stable)})")
            logger.info(f"  stable + V4.4低: ROI {low_roi:.1f}% (n={len(low_v44_stable)})")
        except Exception as e:
            logger.warning(f"  分析エラー: {e}")
    
    return


def main():
    logger.info("="*70)
    logger.info("複数年再現性検証 & 個別レース深層分析")
    logger.info(f"最適化パラメータ: volatile_th={OPTIMIZED_VOLATILE_TH}, moderate_th={OPTIMIZED_MODERATE_TH}")
    logger.info("="*70)
    
    # データ読み込み
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 新馬・障害除外
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    logger.info(f"データ件数: {len(races):,}")
    
    # 各年の予測を生成
    test_years = [2020, 2021, 2022, 2023, 2024]
    all_predictions = []
    
    for year in test_years:
        logger.info(f"\n{year}年の予測を生成中...")
        preds = run_year_prediction(races, pedigrees, corners, race_details, horses, returns, year)
        if preds is not None:
            all_predictions.append(preds)
    
    all_predictions = pd.concat(all_predictions, ignore_index=True)
    logger.info(f"\n全予測件数: {len(all_predictions):,}")
    
    # 年別分析
    logger.info("\n" + "="*70)
    logger.info("年別 再現性検証（最適化パラメータ適用）")
    logger.info("="*70)
    
    year_results = analyze_by_year(all_predictions)
    
    logger.info("\n【年別ROI比較】")
    logger.info("-" * 70)
    logger.info(f"{'年':>6} | {'最適化ROI':>10} | {'従来ROI':>10} | {'差':>8} | {'stable ROI':>12} | {'stable件数':>10}")
    logger.info("-" * 70)
    
    for _, row in year_results.iterrows():
        diff = row['opt_roi'] - row['ens_roi']
        logger.info(f"{row['year']:>6} | {row['opt_roi']:>10.1f}% | {row['ens_roi']:>10.1f}% | {diff:>+8.1f} | {row['stable_roi']:>12.1f}% | {int(row['stable_count']):>10}")
    
    # 5年平均
    avg_opt = year_results['opt_roi'].mean()
    avg_ens = year_results['ens_roi'].mean()
    avg_stable = year_results['stable_roi'].mean()
    avg_stable_count = year_results['stable_count'].mean()
    avg_diff = avg_opt - avg_ens
    
    logger.info("-" * 70)
    logger.info(f"{'平均':>6} | {avg_opt:>10.1f}% | {avg_ens:>10.1f}% | {avg_diff:>+8.1f} | {avg_stable:>12.1f}% | {int(avg_stable_count):>10}")
    
    # 再現性判定
    logger.info("\n【再現性評価】")
    stable_std = year_results['stable_roi'].std()
    opt_std = year_results['opt_roi'].std()
    logger.info(f"  stable ROI標準偏差: {stable_std:.1f}pt（低いほど安定）")
    logger.info(f"  最適化ROI標準偏差: {opt_std:.1f}pt")
    
    if stable_std < 10:
        logger.info("  → stable戦略は再現性あり ✅")
    else:
        logger.info("  → stable戦略は変動が大きい ⚠️")
    
    # 改善機会の特定
    find_improvement_opportunities(all_predictions)
    
    # 個別レース分析
    patterns = analyze_individual_races(all_predictions)
    
    # パターン分析結果
    logger.info("\n" + "="*70)
    logger.info("パターン分析結果")
    logger.info("="*70)
    
    if patterns['high_odds_hit']:
        high_df = pd.DataFrame(patterns['high_odds_hit'])
        logger.info("\n【高配当的中パターン】")
        logger.info(f"  平均オッズ: {high_df['odds'].mean():.1f}倍")
        logger.info(f"  平均人気: {high_df['pop'].mean():.1f}")
        logger.info(f"  荒れ度分布: {high_df['vol_type'].value_counts().to_dict()}")
        logger.info(f"  平均V4.4スコア: {high_df['v44_score'].mean():.3f}")
    
    if patterns['fav_miss']:
        miss_df = pd.DataFrame(patterns['fav_miss'])
        logger.info("\n【人気馬外れパターン】")
        logger.info(f"  平均オッズ: {miss_df['odds'].mean():.1f}倍")
        logger.info(f"  平均人気: {miss_df['pop'].mean():.1f}")
        logger.info(f"  荒れ度分布: {miss_df['vol_type'].value_counts().to_dict()}")
        logger.info(f"  平均実着順: {miss_df['actual_rank'].mean():.1f}")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    year_results.to_csv(output_dir / "multi_year_reproducibility.csv", index=False, encoding='utf-8-sig')
    
    logger.info(f"\n結果保存完了")


if __name__ == "__main__":
    main()
