# -*- coding: utf-8 -*-
"""
全券種ROI評価

V15モデルを使って全券種（単勝/複勝/馬連/馬単/三連複/三連単）のROIを計算。
各券種に最適な購入戦略を検討する。
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
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def calc_tansho_roi(df):
    """単勝ROI"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate, len(bets)


def calc_fukusho_roi(df, returns_df):
    """複勝ROI"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    
    # 複勝払戻を取得
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
    fukusho = fukusho.dropna(subset=['horse_number'])
    fukusho['horse_number'] = fukusho['horse_number'].astype(int)
    
    # race_id + horse_numberで払戻を取得
    fukusho_dict = dict(zip(
        zip(fukusho['race_id'], fukusho['horse_number']),
        fukusho['payout']
    ))
    
    bets['payout'] = bets.apply(
        lambda x: fukusho_dict.get((x['race_id'], int(x['horse_number'])), 0),
        axis=1
    )
    
    total_bet = len(bets) * 100
    total_payout = bets['payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate, len(bets)


def calc_umaren_roi(df, returns_df):
    """馬連ROI（Top2選択）"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    top2 = df[df['rank'] <= 2].copy()
    bets = top2.groupby('race_id').agg({
        'horse_number': lambda x: tuple(sorted(x.astype(int)))
    }).reset_index()
    bets.columns = ['race_id', 'bet_set']
    
    # 馬連払戻
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren = umaren.dropna(subset=['horse_1', 'horse_2'])
    umaren['winner_set'] = umaren.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2'])])),
        axis=1
    )
    umaren_dict = dict(zip(
        zip(umaren['race_id'], umaren['winner_set']),
        umaren['payout']
    ))
    
    bets['payout'] = bets.apply(
        lambda x: umaren_dict.get((x['race_id'], x['bet_set']), 0),
        axis=1
    )
    
    total_bet = len(bets) * 100
    total_payout = bets['payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate, len(bets)


def calc_sanrenpuku_roi(df, returns_df):
    """三連複ROI（Top3選択）"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    top3 = df[df['rank'] <= 3].copy()
    bets = top3.groupby('race_id').agg({
        'horse_number': lambda x: tuple(sorted(x.astype(int)))
    }).reset_index()
    bets.columns = ['race_id', 'bet_set']
    bets = bets[bets['bet_set'].apply(len) == 3]
    
    # 三連複払戻
    sanrenpuku = returns_df[returns_df['bet_type'] == 'sanrenpuku'].copy()
    sanrenpuku = sanrenpuku.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku['winner_set'] = sanrenpuku.apply(
        lambda x: tuple(sorted([int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])])),
        axis=1
    )
    sanrenpuku_dict = dict(zip(
        zip(sanrenpuku['race_id'], sanrenpuku['winner_set']),
        sanrenpuku['payout']
    ))
    
    bets['payout'] = bets.apply(
        lambda x: sanrenpuku_dict.get((x['race_id'], x['bet_set']), 0),
        axis=1
    )
    
    total_bet = len(bets) * 100
    total_payout = bets['payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate, len(bets)


def calc_sanrentan_roi(df, returns_df):
    """三連単ROI（Top3を順番に選択）"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    top3 = df[df['rank'] <= 3].copy()
    top3 = top3.sort_values(['race_id', 'rank'])
    
    bets = top3.groupby('race_id').agg({
        'horse_number': list
    }).reset_index()
    bets['bet_tuple'] = bets['horse_number'].apply(
        lambda x: tuple([int(h) for h in x]) if len(x) == 3 else None
    )
    bets = bets[bets['bet_tuple'].notna()]
    
    # 三連単払戻
    sanrentan = returns_df[returns_df['bet_type'] == 'sanrentan'].copy()
    sanrentan = sanrentan.dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrentan['winner_tuple'] = sanrentan.apply(
        lambda x: (int(x['horse_1']), int(x['horse_2']), int(x['horse_3'])),
        axis=1
    )
    sanrentan_dict = dict(zip(
        zip(sanrentan['race_id'], sanrentan['winner_tuple']),
        sanrentan['payout']
    ))
    
    bets['payout'] = bets.apply(
        lambda x: sanrentan_dict.get((x['race_id'], x['bet_tuple']), 0),
        axis=1
    )
    
    total_bet = len(bets) * 100
    total_payout = bets['payout'].sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0
    
    return roi, hit_rate, len(bets)


def main():
    logger.info("=" * 60)
    logger.info("全券種ROI評価")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量生成
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
    
    # モデル学習
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    test_f['pred'] = model.predict(X_test)
    
    # ========== 各券種のROI計算 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("V15モデルでの各券種ROI（Top1選択戦略）")
    logger.info("=" * 60)
    
    # 単勝
    tansho_roi, tansho_hit, tansho_n = calc_tansho_roi(test_f)
    logger.info(f"\n  【単勝】TopN=1")
    logger.info(f"    ROI: {tansho_roi:.1f}%, 的中率: {tansho_hit:.1f}%, ベット数: {tansho_n:,}")
    
    # 複勝
    fukusho_roi, fukusho_hit, fukusho_n = calc_fukusho_roi(test_f, returns)
    logger.info(f"\n  【複勝】TopN=1")
    logger.info(f"    ROI: {fukusho_roi:.1f}%, 的中率: {fukusho_hit:.1f}%, ベット数: {fukusho_n:,}")
    
    # 馬連
    umaren_roi, umaren_hit, umaren_n = calc_umaren_roi(test_f, returns)
    logger.info(f"\n  【馬連】Top2選択")
    logger.info(f"    ROI: {umaren_roi:.1f}%, 的中率: {umaren_hit:.1f}%, ベット数: {umaren_n:,}")
    
    # 三連複
    sanrenpuku_roi, sanrenpuku_hit, sanrenpuku_n = calc_sanrenpuku_roi(test_f, returns)
    logger.info(f"\n  【三連複】Top3選択")
    logger.info(f"    ROI: {sanrenpuku_roi:.1f}%, 的中率: {sanrenpuku_hit:.1f}%, ベット数: {sanrenpuku_n:,}")
    
    # 三連単
    sanrentan_roi, sanrentan_hit, sanrentan_n = calc_sanrentan_roi(test_f, returns)
    logger.info(f"\n  【三連単】Top3順番選択")
    logger.info(f"    ROI: {sanrentan_roi:.1f}%, 的中率: {sanrentan_hit:.1f}%, ベット数: {sanrentan_n:,}")
    
    # ========== サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    logger.info(f"\n  {'券種':8} {'ROI':>8} {'的中率':>8} {'ベット数':>10}")
    logger.info("  " + "-" * 40)
    logger.info(f"  {'単勝':8} {tansho_roi:>7.1f}% {tansho_hit:>7.1f}% {tansho_n:>10,}")
    logger.info(f"  {'複勝':8} {fukusho_roi:>7.1f}% {fukusho_hit:>7.1f}% {fukusho_n:>10,}")
    logger.info(f"  {'馬連':8} {umaren_roi:>7.1f}% {umaren_hit:>7.1f}% {umaren_n:>10,}")
    logger.info(f"  {'三連複':8} {sanrenpuku_roi:>7.1f}% {sanrenpuku_hit:>7.1f}% {sanrenpuku_n:>10,}")
    logger.info(f"  {'三連単':8} {sanrentan_roi:>7.1f}% {sanrentan_hit:>7.1f}% {sanrentan_n:>10,}")
    
    # 推奨
    best = max([
        ('単勝', tansho_roi),
        ('複勝', fukusho_roi),
        ('馬連', umaren_roi),
        ('三連複', sanrenpuku_roi),
        ('三連単', sanrentan_roi),
    ], key=lambda x: x[1])
    
    logger.info(f"\n  ★ 最良券種: {best[0]} (ROI={best[1]:.1f}%)")
    
    return test_f


if __name__ == "__main__":
    main()
