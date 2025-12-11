# -*- coding: utf-8 -*-
"""
2着・3着予測パターン分析 & 馬連・三連複戦略

【目的】
1. 予測2位・3位の馬がどのオッズ帯で2着・3着に入りやすいか分析
2. その傾向を使って馬連・馬単・三連複の最適戦略を構築
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import itertools
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
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def analyze_place_patterns(df):
    """予測順位別×オッズ帯別の着順傾向分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 予測順位別×オッズ帯別の着順傾向")
    logger.info("=" * 60)
    
    # オッズ帯を定義
    df = df.copy()
    df['odds_cat'] = pd.cut(
        df['win_odds'],
        bins=[0, 5, 10, 20, 50, float('inf')],
        labels=['~5x', '5-10x', '10-20x', '20-50x', '50x+']
    )
    
    for pred_r in range(1, 4):
        logger.info(f"\n  【予測{pred_r}位】")
        logger.info(f"  {'オッズ帯':8} {'1着率':>8} {'2着率':>8} {'3着率':>8} {'3着内率':>8} {'n':>6}")
        logger.info("  " + "-" * 55)
        
        for cat in ['~5x', '5-10x', '10-20x', '20-50x', '50x+']:
            subset = df[(df['pred_rank'] == pred_r) & (df['odds_cat'] == cat)]
            n = len(subset)
            if n < 50:
                continue
            
            win = len(subset[subset['finish_position'] == 1]) / n * 100
            place2 = len(subset[subset['finish_position'] == 2]) / n * 100
            place3 = len(subset[subset['finish_position'] == 3]) / n * 100
            top3 = len(subset[subset['finish_position'] <= 3]) / n * 100
            
            logger.info(f"  {cat:8} {win:>7.1f}% {place2:>7.1f}% {place3:>7.1f}% {top3:>7.1f}% {n:>6,}")


def analyze_umaren_patterns(df, returns_df):
    """馬連オッズ帯戦略分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 馬連戦略 - Top1+Top2のオッズ帯別ROI")
    logger.info("=" * 60)
    
    # 馬連払戻データ
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren = umaren.dropna(subset=['horse_1', 'horse_2'])
    umaren['winner_set'] = umaren.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2'])]),
        axis=1
    )
    payout_dict = dict(zip(
        zip(umaren['race_id'], umaren['winner_set']),
        umaren['payout']
    ))
    
    # Top1のオッズ帯別
    df = df.copy()
    df['odds_cat'] = pd.cut(
        df['win_odds'],
        bins=[0, 5, 10, 20, 50, float('inf')],
        labels=['~5x', '5-10x', '10-20x', '20-50x', '50x+']
    )
    
    logger.info(f"\n  【Top1オッズ帯別】")
    logger.info(f"  {'Top1オッズ':10} {'ROI':>8} {'的中率':>8} {'n':>6}")
    logger.info("  " + "-" * 40)
    
    for cat in ['~5x', '5-10x', '10-20x', '20-50x', '50x+']:
        # Top1がこのオッズ帯のレース
        top1_cat = df[(df['pred_rank'] == 1) & (df['odds_cat'] == cat)]
        race_ids = top1_cat['race_id'].unique()
        
        total_bet = 0
        total_payout = 0
        hit_count = 0
        
        for race_id in race_ids:
            race_df = df[df['race_id'] == race_id]
            top2 = race_df[race_df['pred_rank'] <= 2]['horse_number'].astype(int).tolist()
            if len(top2) < 2:
                continue
            
            bet_set = frozenset(top2)
            payout = payout_dict.get((race_id, bet_set), 0)
            
            total_bet += 100
            total_payout += payout
            if payout > 0:
                hit_count += 1
        
        if total_bet > 0:
            roi = total_payout / total_bet * 100
            hit_rate = hit_count / len(race_ids) * 100
            logger.info(f"  {cat:10} {roi:>7.1f}% {hit_rate:>7.1f}% {len(race_ids):>6,}")
    
    # Top1+Top2両方のオッズ帯別
    logger.info(f"\n  【Top1×Top2オッズ帯組み合わせ】")
    logger.info(f"  {'Top1':8} {'Top2':8} {'ROI':>8} {'的中率':>8} {'n':>6}")
    logger.info("  " + "-" * 50)
    
    for cat1 in ['~5x', '5-10x', '10-20x', '20-50x']:
        for cat2 in ['~5x', '5-10x', '10-20x', '20-50x', '50x+']:
            # Top1がcat1、Top2がcat2のレース
            top1_cat = df[(df['pred_rank'] == 1) & (df['odds_cat'] == cat1)]
            race_ids_1 = set(top1_cat['race_id'].unique())
            top2_cat = df[(df['pred_rank'] == 2) & (df['odds_cat'] == cat2)]
            race_ids_2 = set(top2_cat['race_id'].unique())
            race_ids = race_ids_1 & race_ids_2
            
            if len(race_ids) < 30:
                continue
            
            total_bet = 0
            total_payout = 0
            hit_count = 0
            
            for race_id in race_ids:
                race_df = df[df['race_id'] == race_id]
                top2 = race_df[race_df['pred_rank'] <= 2]['horse_number'].astype(int).tolist()
                if len(top2) < 2:
                    continue
                
                bet_set = frozenset(top2)
                payout = payout_dict.get((race_id, bet_set), 0)
                
                total_bet += 100
                total_payout += payout
                if payout > 0:
                    hit_count += 1
            
            if total_bet > 0:
                roi = total_payout / total_bet * 100
                hit_rate = hit_count / len(race_ids) * 100
                if roi > 80:  # 有望な組み合わせのみ表示
                    logger.info(f"  {cat1:8} {cat2:8} {roi:>7.1f}% {hit_rate:>7.1f}% {len(race_ids):>6,}")


def analyze_sanrenpuku_patterns(df, returns_df):
    """三連複オッズ帯戦略分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 三連複戦略 - オッズ帯組み合わせ別ROI")
    logger.info("=" * 60)
    
    # 三連複払戻データ
    sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
    sanrenpuku = sanrenpuku.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku['winner_set'] = sanrenpuku.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])]),
        axis=1
    )
    payout_dict = dict(zip(
        zip(sanrenpuku['race_id'], sanrenpuku['winner_set']),
        sanrenpuku['payout']
    ))
    
    df = df.copy()
    df['odds_cat'] = pd.cut(
        df['win_odds'],
        bins=[0, 5, 10, 20, 50, float('inf')],
        labels=['~5x', '5-10x', '10-20x', '20-50x', '50x+']
    )
    
    # Top1オッズ帯別
    logger.info(f"\n  【Top1オッズ帯別（Top3を購入）】")
    logger.info(f"  {'Top1オッズ':10} {'ROI':>8} {'的中率':>8} {'n':>6}")
    logger.info("  " + "-" * 40)
    
    for cat in ['~5x', '5-10x', '10-20x', '20-50x', '50x+']:
        top1_cat = df[(df['pred_rank'] == 1) & (df['odds_cat'] == cat)]
        race_ids = top1_cat['race_id'].unique()
        
        total_bet = 0
        total_payout = 0
        hit_count = 0
        
        for race_id in race_ids:
            race_df = df[df['race_id'] == race_id]
            top3 = race_df[race_df['pred_rank'] <= 3]['horse_number'].astype(int).tolist()
            if len(top3) < 3:
                continue
            
            bet_set = frozenset(top3)
            payout = payout_dict.get((race_id, bet_set), 0)
            
            total_bet += 100
            total_payout += payout
            if payout > 0:
                hit_count += 1
        
        if total_bet > 0:
            roi = total_payout / total_bet * 100
            hit_rate = hit_count / len(race_ids) * 100
            logger.info(f"  {cat:10} {roi:>7.1f}% {hit_rate:>7.1f}% {len(race_ids):>6,}")
    
    # 合計オッズ帯別（Top3の合計オッズ）
    logger.info(f"\n  【Top3合計オッズ帯別】")
    
    race_odds = df.groupby('race_id').apply(
        lambda x: x[x['pred_rank'] <= 3]['win_odds'].sum()
    ).reset_index()
    race_odds.columns = ['race_id', 'total_odds']
    
    race_odds['total_cat'] = pd.cut(
        race_odds['total_odds'],
        bins=[0, 15, 30, 50, 100, float('inf')],
        labels=['~15x', '15-30x', '30-50x', '50-100x', '100x+']
    )
    
    logger.info(f"  {'合計オッズ':10} {'ROI':>8} {'的中率':>8} {'n':>6}")
    logger.info("  " + "-" * 40)
    
    for cat in ['~15x', '15-30x', '30-50x', '50-100x', '100x+']:
        race_ids = race_odds[race_odds['total_cat'] == cat]['race_id'].values
        
        total_bet = 0
        total_payout = 0
        hit_count = 0
        
        for race_id in race_ids:
            race_df = df[df['race_id'] == race_id]
            top3 = race_df[race_df['pred_rank'] <= 3]['horse_number'].astype(int).tolist()
            if len(top3) < 3:
                continue
            
            bet_set = frozenset(top3)
            payout = payout_dict.get((race_id, bet_set), 0)
            
            total_bet += 100
            total_payout += payout
            if payout > 0:
                hit_count += 1
        
        if total_bet > 0 and len(race_ids) >= 50:
            roi = total_payout / total_bet * 100
            hit_rate = hit_count / len(race_ids) * 100
            logger.info(f"  {cat:10} {roi:>7.1f}% {hit_rate:>7.1f}% {len(race_ids):>6,}")


def test_combined_strategy(df, returns_df):
    """最適組み合わせ戦略のテスト"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 最適組み合わせ戦略テスト")
    logger.info("=" * 60)
    
    df = df.copy()
    df['odds_cat'] = pd.cut(
        df['win_odds'],
        bins=[0, 5, 10, 20, 50, float('inf')],
        labels=['~5x', '5-10x', '10-20x', '20-50x', '50x+']
    )
    
    # 馬連: Top1がオッズ10-50倍の場合
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren = umaren.dropna(subset=['horse_1', 'horse_2'])
    umaren['winner_set'] = umaren.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2'])]),
        axis=1
    )
    umaren_dict = dict(zip(
        zip(umaren['race_id'], umaren['winner_set']),
        umaren['payout']
    ))
    
    # 三連複払戻
    sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
    sanrenpuku = sanrenpuku.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku['winner_set'] = sanrenpuku.apply(
        lambda x: frozenset([int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])]),
        axis=1
    )
    sanrenpuku_dict = dict(zip(
        zip(sanrenpuku['race_id'], sanrenpuku['winner_set']),
        sanrenpuku['payout']
    ))
    
    strategies = [
        ("馬連_ベースライン", "umaren", lambda x: True),
        ("馬連_Top1オッズ10-50", "umaren", lambda x: x['top1_odds'] >= 10 and x['top1_odds'] < 50),
        ("馬連_Top1オッズ5-20", "umaren", lambda x: x['top1_odds'] >= 5 and x['top1_odds'] < 20),
        ("三連複_ベースライン", "sanrenpuku", lambda x: True),
        ("三連複_Top1オッズ10-50", "sanrenpuku", lambda x: x['top1_odds'] >= 10 and x['top1_odds'] < 50),
        ("三連複_合計オッズ30-100", "sanrenpuku", lambda x: x['total_odds'] >= 30 and x['total_odds'] < 100),
    ]
    
    race_ids = df['race_id'].unique()
    
    logger.info(f"\n  {'戦略':25} {'ROI':>8} {'的中率':>8} {'n':>6}")
    logger.info("  " + "-" * 55)
    
    for name, bet_type, condition in strategies:
        total_bet = 0
        total_payout = 0
        hit_count = 0
        bet_count = 0
        
        for race_id in race_ids:
            race_df = df[df['race_id'] == race_id]
            
            top1 = race_df[race_df['pred_rank'] == 1]
            if len(top1) == 0:
                continue
            top1_odds = top1.iloc[0]['win_odds']
            
            if bet_type == "umaren":
                top_n = race_df[race_df['pred_rank'] <= 2]['horse_number'].astype(int).tolist()
                if len(top_n) < 2:
                    continue
                total_odds = race_df[race_df['pred_rank'] <= 2]['win_odds'].sum()
                payout_dict = umaren_dict
                n_horses = 2
            else:
                top_n = race_df[race_df['pred_rank'] <= 3]['horse_number'].astype(int).tolist()
                if len(top_n) < 3:
                    continue
                total_odds = race_df[race_df['pred_rank'] <= 3]['win_odds'].sum()
                payout_dict = sanrenpuku_dict
                n_horses = 3
            
            race_info = {'top1_odds': top1_odds, 'total_odds': total_odds}
            if not condition(race_info):
                continue
            
            bet_set = frozenset(top_n)
            payout = payout_dict.get((race_id, bet_set), 0)
            
            total_bet += 100
            total_payout += payout
            bet_count += 1
            if payout > 0:
                hit_count += 1
        
        if bet_count > 0:
            roi = total_payout / total_bet * 100
            hit_rate = hit_count / bet_count * 100
            logger.info(f"  {name:25} {roi:>7.1f}% {hit_rate:>7.1f}% {bet_count:>6,}")


def main():
    logger.info("=" * 60)
    logger.info("2着・3着予測パターン分析 & 複合馬券戦略")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
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
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_f['pred'] = model.predict(X_test)
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # 各種分析
    analyze_place_patterns(test_f)
    analyze_umaren_patterns(test_f, returns)
    analyze_sanrenpuku_patterns(test_f, returns)
    test_combined_strategy(test_f, returns)


if __name__ == "__main__":
    main()
