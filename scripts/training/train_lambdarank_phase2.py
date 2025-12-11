# -*- coding: utf-8 -*-
"""
Phase 2: LambdaRankモデル（馬連・馬単向け）

18_improvement_roadmap.md モデルC実装

【モデル設計】
- 目的変数: 関連度ラベル [1着=5, 2着=4, 3着=3, 4-5着=1, 他=0]
- 手法: LightGBM LambdaRank (NDCG最適化)
- 強化正則化: max_depth=2, num_leaves=4

【評価】
- 単勝ROI: Top1予測の1着払戻
- 馬連ROI: Top1-Top2予測の馬連払戻
- 馬単ROI: Top1→Top2予測の馬単払戻
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


def create_relevance_label(finish_pos):
    """関連度ラベル作成"""
    if finish_pos == 1: return 5
    elif finish_pos == 2: return 4
    elif finish_pos == 3: return 3
    elif finish_pos <= 5: return 1
    else: return 0


def calc_tansho_roi(df, preds):
    """単勝ROI"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def calc_umaren_roi(df, preds, returns):
    """
    馬連ROI
    - 予測Top1とTop2の組み合わせを馬連購入
    - 馬連的中時に払戻
    """
    umaren = returns[returns['bet_type'] == 'umaren'].copy()
    
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # 各レースのTop2を取得
    top2 = df[df['rank'] <= 2][['race_id', 'horse_number', 'finish_position', 'rank']].copy()
    
    results = []
    for race_id in top2['race_id'].unique():
        race_top2 = top2[top2['race_id'] == race_id].sort_values('rank')
        if len(race_top2) != 2:
            continue
        
        h1, h2 = race_top2['horse_number'].values
        # 馬連は順不同なのでソート
        combo = tuple(sorted([h1, h2]))
        
        # 馬連払戻を取得
        race_umaren = umaren[umaren['race_id'] == race_id]
        hit = race_umaren[
            (race_umaren['horse_number'].isin([combo[0], combo[1]])) | 
            (race_umaren['horse_number_2'].isin([combo[0], combo[1]]))
        ]
        
        # horse_number, horse_number_2 の両方がhit組み合わせかチェック
        for _, row in race_umaren.iterrows():
            nums = sorted([row['horse_number'], row['horse_number_2']]) if pd.notna(row.get('horse_number_2')) else [row['horse_number']]
            if len(nums) == 2 and tuple(nums) == combo:
                results.append({'race_id': race_id, 'payout': row['payout']})
                break
        else:
            results.append({'race_id': race_id, 'payout': 0})
    
    if len(results) == 0:
        return 0, 0
    
    results_df = pd.DataFrame(results)
    total_bets = len(results_df) * 100
    total_payout = results_df['payout'].sum()
    
    hit_count = (results_df['payout'] > 0).sum()
    hit_rate = hit_count / len(results_df) * 100
    roi = total_payout / total_bets * 100
    
    return roi, hit_rate


def calc_umatan_roi(df, preds, returns):
    """
    馬単ROI
    - 予測Top1が1着、Top2が2着になる馬単を購入
    - 順序を正確に予測する必要がある
    """
    umatan = returns[returns['bet_type'] == 'umatan'].copy()
    
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    top2 = df[df['rank'] <= 2][['race_id', 'horse_number', 'finish_position', 'rank']].copy()
    
    results = []
    for race_id in top2['race_id'].unique():
        race_top2 = top2[top2['race_id'] == race_id].sort_values('rank')
        if len(race_top2) != 2:
            continue
        
        h1, h2 = race_top2['horse_number'].values  # 1着予測, 2着予測
        
        # 馬単払戻を取得（順序あり: h1が1着、h2が2着）
        race_umatan = umatan[umatan['race_id'] == race_id]
        
        for _, row in race_umatan.iterrows():
            if row['horse_number'] == h1 and row.get('horse_number_2') == h2:
                results.append({'race_id': race_id, 'payout': row['payout']})
                break
        else:
            results.append({'race_id': race_id, 'payout': 0})
    
    if len(results) == 0:
        return 0, 0
    
    results_df = pd.DataFrame(results)
    total_bets = len(results_df) * 100
    total_payout = results_df['payout'].sum()
    
    hit_count = (results_df['payout'] > 0).sum()
    hit_rate = hit_count / len(results_df) * 100
    roi = total_payout / total_bets * 100
    
    return roi, hit_rate


def main():
    logger.info("=" * 60)
    logger.info("Phase 2: LambdaRankモデル（馬連・馬単向け）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # 特徴量
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # 関連度ラベル
    train_f['relevance'] = train_f['finish_position'].apply(create_relevance_label)
    test_f['relevance'] = test_f['finish_position'].apply(create_relevance_label)
    
    # ソート & グループ
    train_sorted = train_f.sort_values('race_id')
    test_sorted = test_f.sort_values('race_id')
    
    X_train = train_sorted[feature_cols].fillna(0)
    X_test = test_sorted[feature_cols].fillna(0)
    y_train = train_sorted['relevance']
    
    groups_train = train_sorted.groupby('race_id').size().values
    
    # ========== ベースライン (Binary Classification) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン (Binary Classification = V15相当)")
    logger.info("=" * 60)
    
    y_train_binary = (train_sorted['finish_position'] == 1).astype(int)
    
    params_binary = {
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
    
    train_ds_binary = lgb.Dataset(X_train, y_train_binary)
    model_binary = lgb.train(params_binary, train_ds_binary, num_boost_round=200)
    
    pred_test_binary = model_binary.predict(X_test)
    
    test_tansho_roi_bin, test_tansho_hit_bin = calc_tansho_roi(test_sorted, pred_test_binary)
    
    logger.info(f"  単勝: 的中率={test_tansho_hit_bin:.1f}%, ROI={test_tansho_roi_bin:.1f}%")
    
    # ========== LambdaRank (通常) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. LambdaRank (通常設定)")
    logger.info("=" * 60)
    
    params_lambdarank = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
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
    
    train_ds_rank = lgb.Dataset(X_train, y_train, group=groups_train)
    model_rank = lgb.train(params_lambdarank, train_ds_rank, num_boost_round=200)
    
    pred_test_rank = model_rank.predict(X_test)
    
    test_tansho_roi_rank, test_tansho_hit_rank = calc_tansho_roi(test_sorted, pred_test_rank)
    
    logger.info(f"  単勝: 的中率={test_tansho_hit_rank:.1f}%, ROI={test_tansho_roi_rank:.1f}%")
    
    # ========== LambdaRank (強正則化) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. LambdaRank (強正則化: max_depth=2, num_leaves=4)")
    logger.info("=" * 60)
    
    params_lambdarank_strong = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 4,      # 強化
        'max_depth': 2,       # 強化
        'min_child_samples': 200,
        'reg_alpha': 10.0,
        'reg_lambda': 10.0,
        'bagging_fraction': 0.5,
        'bagging_freq': 3,
        'feature_fraction': 0.5,
    }
    
    model_rank_strong = lgb.train(params_lambdarank_strong, train_ds_rank, num_boost_round=300)
    
    pred_test_rank_strong = model_rank_strong.predict(X_test)
    
    test_tansho_roi_strong, test_tansho_hit_strong = calc_tansho_roi(test_sorted, pred_test_rank_strong)
    
    logger.info(f"  単勝: 的中率={test_tansho_hit_strong:.1f}%, ROI={test_tansho_roi_strong:.1f}%")
    
    # ========== 比較サマリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較 (単勝ROI)")
    logger.info("=" * 60)
    
    logger.info(f"                    Binary      LambdaRank   LR強正則化")
    logger.info(f"  単勝ROI:         {test_tansho_roi_bin:7.1f}%      {test_tansho_roi_rank:7.1f}%      {test_tansho_roi_strong:7.1f}%")
    logger.info(f"  単勝的中率:      {test_tansho_hit_bin:7.1f}%      {test_tansho_hit_rank:7.1f}%      {test_tansho_hit_strong:7.1f}%")
    
    # 最良モデルを選択
    best_roi = max(test_tansho_roi_bin, test_tansho_roi_rank, test_tansho_roi_strong)
    best_model_name = "Binary" if best_roi == test_tansho_roi_bin else ("LambdaRank" if best_roi == test_tansho_roi_rank else "LR強正則化")
    
    logger.info(f"\n  最良モデル: {best_model_name} (ROI={best_roi:.1f}%)")
    
    return model_binary, model_rank, model_rank_strong


if __name__ == "__main__":
    main()
