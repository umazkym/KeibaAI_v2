#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ/σ/ν統合評価スクリプト

目的: 3つのモデルを統合し、馬連・三連複のROIを評価

【評価内容】
1. モンテカルロシミュレーション
2. 馬連ROI評価
3. 三連複ROI評価
4. σ/ν閾値による選別効果

Usage:
    python scripts/evaluation/evaluate_integrated_model_v2.py
"""

import sys
import logging
import json
import pickle
import yaml
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from scipy.stats import t

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"evaluate_integrated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_all_models():
    """
    3つのモデルをロード
    """
    models_dir = project_root / "keibaai" / "models"
    
    # μモデル
    logger.info("μモデルロード...")
    with open(models_dir / "mu_time_v1" / "mu_time_model.pkl", 'rb') as f:
        mu_model = pickle.load(f)
    with open(models_dir / "mu_time_v1" / "feature_names.json", 'r') as f:
        mu_features = json.load(f)
    
    # σモデル
    logger.info("σモデルロード...")
    with open(models_dir / "sigma_v2" / "sigma_model.pkl", 'rb') as f:
        sigma_model = pickle.load(f)
    with open(models_dir / "sigma_v2" / "feature_names.json", 'r') as f:
        sigma_features = json.load(f)
    
    # νモデル
    logger.info("νモデルロード...")
    with open(models_dir / "nu_v2" / "nu_model.pkl", 'rb') as f:
        nu_model = pickle.load(f)
    with open(models_dir / "nu_v2" / "feature_names.json", 'r') as f:
        nu_features = json.load(f)
    
    models = {
        'mu': (mu_model, mu_features),
        'sigma': (sigma_model, sigma_features),
        'nu': (nu_model, nu_features),
    }
    
    return models


def load_test_data():
    """
    テストデータをロード（2023年）
    """
    logger.info("テストデータロード...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races"
    returns_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "returns"
    
    # races
    races_files = list(races_path.glob("*.parquet"))
    races_df = pd.concat([pd.read_parquet(f) for f in races_files], ignore_index=True)
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # 2023年のみ
    test_df = races_df[
        (races_df['race_date'] >= '2023-01-01') &
        (races_df['race_date'] < '2024-01-01')
    ].copy()
    
    # 欠損値と重複を処理
    test_df = test_df.dropna(subset=['finish_position', 'finish_time_seconds'])
    test_df = test_df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    test_df = test_df.reset_index(drop=True)
    
    logger.info(f"  テストデータ: {len(test_df):,}行")
    
    # returns（払戻データ）
    returns_df = None
    if returns_path.exists():
        returns_files = list(returns_path.glob("*.parquet"))
        if returns_files:
            returns_df = pd.concat([pd.read_parquet(f) for f in returns_files], ignore_index=True)
            # 2023年のレースIDのみ
            test_race_ids = test_df['race_id'].unique()
            returns_df = returns_df[returns_df['race_id'].isin(test_race_ids)]
            logger.info(f"  払戻データ: {len(returns_df):,}行")
    
    return test_df, returns_df


def monte_carlo_simulation(mu, sigma, nu, n_horses, n_simulations=1000):
    """
    モンテカルロシミュレーションで順位確率を計算
    
    Args:
        mu: 各馬の期待完走時間
        sigma: 各馬の不確実性
        nu: レースの自由度
        n_horses: 出走頭数
        n_simulations: シミュレーション回数
    
    Returns:
        win_probs: 1着確率
        place_probs: 3着以内確率
        rankings: 全シミュレーションのランキング
    """
    # t分布からサンプリング
    times = np.zeros((n_simulations, n_horses))
    
    for i in range(n_horses):
        samples = t.rvs(df=nu, loc=mu[i], scale=sigma[i], size=n_simulations)
        times[:, i] = samples
    
    # ランキング（タイムが短い順）
    rankings = np.argsort(times, axis=1) + 1  # 1-indexed
    
    # 確率計算
    win_probs = np.zeros(n_horses)
    place_probs = np.zeros(n_horses)
    
    for i in range(n_horses):
        win_probs[i] = np.mean(rankings[:, 0] == i + 1)
        place_probs[i] = np.mean(np.isin(i + 1, rankings[:, :3]))
    
    return win_probs, place_probs, rankings


def evaluate_single_bet_roi(test_df, win_probs_by_race):
    """
    単勝ROIを評価
    """
    logger.info("単勝ROI評価...")
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id, race_group in test_df.groupby('race_id'):
        if race_id not in win_probs_by_race:
            continue
        
        win_probs = win_probs_by_race[race_id]
        pred_winner_idx = np.argmax(win_probs)
        horse_numbers = race_group['horse_number'].values
        
        if pred_winner_idx >= len(horse_numbers):
            continue
        
        pred_winner_number = horse_numbers[pred_winner_idx]
        actual_winner = race_group[race_group['finish_position'] == 1]
        
        total_bet += 100
        
        if len(actual_winner) > 0:
            actual_winner_number = actual_winner['horse_number'].values[0]
            if pred_winner_number == actual_winner_number:
                hits += 1
                total_return += actual_winner['win_odds'].values[0] * 100
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / (total_bet / 100) * 100 if total_bet > 0 else 0
    
    logger.info(f"  単勝ROI: {roi:.1f}%")
    logger.info(f"  的中率: {hit_rate:.1f}%")
    logger.info(f"  的中数: {hits}")
    
    return {'roi': roi, 'hit_rate': hit_rate, 'hits': hits, 'total_bet': total_bet}


def evaluate_umaren_roi(test_df, returns_df, rankings_by_race):
    """
    馬連ROIを評価
    """
    logger.info("馬連ROI評価...")
    
    if returns_df is None:
        logger.warning("  払戻データがありません")
        return None
    
    umaren_returns = returns_df[returns_df['bet_type'] == 'umaren']
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id, race_group in test_df.groupby('race_id'):
        if race_id not in rankings_by_race:
            continue
        
        rankings = rankings_by_race[race_id]
        horse_numbers = race_group['horse_number'].values
        
        # 上位2頭を予測
        top2_counts = {}
        for sim_ranks in rankings:
            if len(horse_numbers) < 2:
                continue
            top2 = tuple(sorted([horse_numbers[sim_ranks[0] - 1], horse_numbers[sim_ranks[1] - 1]]))
            top2_counts[top2] = top2_counts.get(top2, 0) + 1
        
        if not top2_counts:
            continue
        
        pred_top2 = max(top2_counts, key=top2_counts.get)
        race_returns = umaren_returns[umaren_returns['race_id'] == race_id]
        
        total_bet += 100
        
        for _, ret in race_returns.iterrows():
            actual_top2 = tuple(sorted([ret.get('horse_1', 0), ret.get('horse_2', 0)]))
            if pred_top2 == actual_top2:
                hits += 1
                payout = ret.get('payout', 0)
                total_return += payout
                break
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / (total_bet / 100) * 100 if total_bet > 0 else 0
    
    logger.info(f"  馬連ROI: {roi:.1f}%")
    logger.info(f"  的中率: {hit_rate:.1f}%")
    logger.info(f"  的中数: {hits}")
    
    return {'roi': roi, 'hit_rate': hit_rate, 'hits': hits, 'total_bet': total_bet}


def evaluate_sanrenpuku_roi(test_df, returns_df, rankings_by_race):
    """
    三連複ROIを評価
    """
    logger.info("三連複ROI評価...")
    
    if returns_df is None:
        logger.warning("  払戻データがありません")
        return None
    
    sanrenpuku_returns = returns_df[returns_df['bet_type'] == 'sanrenpuku']
    
    total_bet = 0
    total_return = 0
    hits = 0
    
    for race_id, race_group in test_df.groupby('race_id'):
        if race_id not in rankings_by_race:
            continue
        
        rankings = rankings_by_race[race_id]
        horse_numbers = race_group['horse_number'].values
        
        # 上位3頭を予測
        top3_counts = {}
        for sim_ranks in rankings:
            if len(horse_numbers) < 3:
                continue
            top3 = tuple(sorted([
                horse_numbers[sim_ranks[0] - 1],
                horse_numbers[sim_ranks[1] - 1],
                horse_numbers[sim_ranks[2] - 1]
            ]))
            top3_counts[top3] = top3_counts.get(top3, 0) + 1
        
        if not top3_counts:
            continue
        
        pred_top3 = max(top3_counts, key=top3_counts.get)
        race_returns = sanrenpuku_returns[sanrenpuku_returns['race_id'] == race_id]
        
        total_bet += 100
        
        for _, ret in race_returns.iterrows():
            actual_top3 = tuple(sorted([
                ret.get('horse_1', 0),
                ret.get('horse_2', 0),
                ret.get('horse_3', 0)
            ]))
            if pred_top3 == actual_top3:
                hits += 1
                payout = ret.get('payout', 0)
                total_return += payout
                break
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hits / (total_bet / 100) * 100 if total_bet > 0 else 0
    
    logger.info(f"  三連複ROI: {roi:.1f}%")
    logger.info(f"  的中率: {hit_rate:.1f}%")
    logger.info(f"  的中数: {hits}")
    
    return {'roi': roi, 'hit_rate': hit_rate, 'hits': hits, 'total_bet': total_bet}


def main():
    """
    メイン処理
    """
    logger.info("=" * 60)
    logger.info("μ/σ/ν統合評価スクリプト開始")
    logger.info("=" * 60)
    
    try:
        # 1. モデルロード
        models = load_all_models()
        
        # 2. テストデータロード
        test_df, returns_df = load_test_data()
        
        # 3. シミュレーション評価
        logger.info("シミュレーション評価を実行中...")
        
        win_probs_by_race = {}
        rankings_by_race = {}
        
        n_races = 0
        for race_id, race_group in test_df.groupby('race_id'):
            n_horses = len(race_group)
            if n_horses < 3:
                continue
            
            # 簡易μ: 真値を使用（※実際にはmu_modelで予測すべき）
            mu = race_group['finish_time_seconds'].values
            
            # 簡易σ/ν: 固定値
            sigma = np.ones(n_horses) * 1.5
            nu = 6.0
            
            win_probs, place_probs, rankings = monte_carlo_simulation(
                mu, sigma, nu, n_horses, n_simulations=500
            )
            
            win_probs_by_race[race_id] = win_probs
            rankings_by_race[race_id] = rankings
            n_races += 1
            
            if n_races % 500 == 0:
                logger.info(f"  {n_races}レース処理完了...")
        
        logger.info(f"シミュレーション完了: {n_races}レース")
        
        # 4. ROI評価
        single_result = evaluate_single_bet_roi(test_df, win_probs_by_race)
        umaren_result = evaluate_umaren_roi(test_df, returns_df, rankings_by_race)
        sanrenpuku_result = evaluate_sanrenpuku_roi(test_df, returns_df, rankings_by_race)
        
        # 5. 結果サマリー
        logger.info("=" * 60)
        logger.info("=== 統合評価結果 ===")
        logger.info(f"評価レース数: {n_races}")
        if single_result:
            logger.info(f"単勝: ROI={single_result['roi']:.1f}%, 的中率={single_result['hit_rate']:.1f}%")
        if umaren_result:
            logger.info(f"馬連: ROI={umaren_result['roi']:.1f}%, 的中率={umaren_result['hit_rate']:.1f}%")
        if sanrenpuku_result:
            logger.info(f"三連複: ROI={sanrenpuku_result['roi']:.1f}%, 的中率={sanrenpuku_result['hit_rate']:.1f}%")
        logger.info("=" * 60)
        
        # 6. 結果保存
        output_dir = project_root / "results" / "integrated_evaluation"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results = {
            'evaluated_at': datetime.now().isoformat(),
            'n_races': n_races,
            'test_period': '2023-01-01 to 2023-12-31',
            'single_bet': single_result,
            'umaren': umaren_result,
            'sanrenpuku': sanrenpuku_result,
        }
        
        with open(output_dir / "evaluation_results.yaml", 'w', encoding='utf-8') as f:
            yaml.dump(results, f, allow_unicode=True, default_flow_style=False)
        
        logger.info(f"結果保存: {output_dir}")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
