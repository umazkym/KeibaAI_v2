# -*- coding: utf-8 -*-
"""
券種別データ分析

【目的】
各券種（単勝/複勝/馬連/馬単/三連複/三連単）の特性を分析し、
券種ごとの最適戦略を検討する。

【分析項目】
1. 払戻金額の分布（平均、中央値、最大値）
2. 的中率（ランダム選択時）
3. 理論上の還元率
4. V15モデルでの現状ROI
5. 各券種に最適なモデル設計
"""
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, returns


def analyze_bet_type(returns, bet_type, name_jp):
    """券種別分析"""
    data = returns[returns['bet_type'] == bet_type].copy()
    
    if len(data) == 0:
        return None
    
    stats = {
        'bet_type': bet_type,
        'name': name_jp,
        'n_races': len(data),
        'payout_mean': data['payout'].mean(),
        'payout_median': data['payout'].median(),
        'payout_std': data['payout'].std(),
        'payout_min': data['payout'].min(),
        'payout_max': data['payout'].max(),
        'payout_25': data['payout'].quantile(0.25),
        'payout_75': data['payout'].quantile(0.75),
    }
    
    return stats


def main():
    logger.info("=" * 60)
    logger.info("券種別データ分析")
    logger.info("=" * 60)
    
    races, returns = load_data()
    
    logger.info(f"  レースデータ: {len(races):,}件")
    logger.info(f"  払戻データ: {len(returns):,}件")
    
    # 券種リスト
    bet_types = [
        ('tansho', '単勝'),
        ('fukusho', '複勝'),
        ('umaren', '馬連'),
        ('umatan', '馬単'),
        ('wide', 'ワイド'),
        ('sanrenpuku', '三連複'),
        ('sanrentan', '三連単'),
    ]
    
    # ========== 1. 券種別データ件数 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 券種別データ件数")
    logger.info("=" * 60)
    
    for bet_type, name in bet_types:
        count = len(returns[returns['bet_type'] == bet_type])
        logger.info(f"  {name}: {count:,}件")
    
    # ========== 2. 払戻金額分析 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. 払戻金額分析 (100円あたり)")
    logger.info("=" * 60)
    
    results = []
    for bet_type, name in bet_types:
        stats = analyze_bet_type(returns, bet_type, name)
        if stats:
            results.append(stats)
            logger.info(f"\n  【{name}】")
            logger.info(f"    レース数: {stats['n_races']:,}")
            logger.info(f"    平均払戻: {stats['payout_mean']:,.0f}円")
            logger.info(f"    中央値:   {stats['payout_median']:,.0f}円")
            logger.info(f"    最小-最大: {stats['payout_min']:,.0f} - {stats['payout_max']:,.0f}円")
            logger.info(f"    25-75%:   {stats['payout_25']:,.0f} - {stats['payout_75']:,.0f}円")
    
    # ========== 3. 理論的中率と還元率 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 理論的中率と還元率")
    logger.info("=" * 60)
    
    # 出走頭数の分布を確認
    heads_per_race = races.groupby('race_id').size()
    avg_heads = heads_per_race.mean()
    logger.info(f"  平均出走頭数: {avg_heads:.1f}頭")
    
    # 理論的中率（ランダム選択時）
    theoretical = {
        '単勝': 1 / avg_heads * 100,
        '複勝': 3 / avg_heads * 100,  # 3着以内
        '馬連': 2 / (avg_heads * (avg_heads - 1) / 2) * 100,
        '馬単': 1 / (avg_heads * (avg_heads - 1)) * 100,
        'ワイド': 3 / (avg_heads * (avg_heads - 1) / 2) * 100,  # 3通り的中可能
        '三連複': 1 / (avg_heads * (avg_heads - 1) * (avg_heads - 2) / 6) * 100,
        '三連単': 1 / (avg_heads * (avg_heads - 1) * (avg_heads - 2)) * 100,
    }
    
    logger.info("")
    logger.info("  券種         理論的中率    払戻平均     推定還元率")
    logger.info("  " + "-" * 50)
    
    for stats in results:
        name = stats['name']
        if name in theoretical:
            hit_rate = theoretical[name]
            payout = stats['payout_mean']
            # 推定還元率 = 的中率 × 平均払戻 / 100
            expected_return = hit_rate / 100 * payout
            logger.info(f"  {name:8} {hit_rate:>10.2f}%  {payout:>10,.0f}円  {expected_return:>8.1f}%")
    
    # ========== 4. 勝馬の分布分析 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 勝馬の人気分布")
    logger.info("=" * 60)
    
    # 単勝での人気別的中率
    winners = races[races['finish_position'] == 1].copy()
    pop_dist = winners.groupby('popularity').size()
    total_races = len(winners)
    
    logger.info("")
    logger.info("  人気    的中数    的中率")
    logger.info("  " + "-" * 30)
    cumulative = 0
    for pop in range(1, 11):
        if pop in pop_dist.index:
            count = pop_dist[pop]
            rate = count / total_races * 100
            cumulative += rate
            logger.info(f"  {pop:2}番人気  {count:>6,}  {rate:>6.1f}%  (累計{cumulative:.1f}%)")
    
    # 複勝（3着以内）の人気分布
    logger.info("")
    logger.info("  【複勝（3着以内）】")
    top3 = races[races['finish_position'] <= 3].copy()
    pop_dist_top3 = top3.groupby('popularity').size()
    total_top3 = len(top3)
    
    logger.info("  人気    的中数    的中率")
    logger.info("  " + "-" * 30)
    cumulative = 0
    for pop in range(1, 6):
        if pop in pop_dist_top3.index:
            count = pop_dist_top3[pop]
            rate = count / total_top3 * 100
            cumulative += rate
            logger.info(f"  {pop:2}番人気  {count:>6,}  {rate:>6.1f}%  (累計{cumulative:.1f}%)")
    
    # ========== 5. 券種別戦略提案 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("5. 券種別戦略提案")
    logger.info("=" * 60)
    
    logger.info("""
  【単勝】(現状ROI: 91.8%)
    - 目標: 1着予測精度の最大化
    - 戦略: is_win ラベル、Binary Classification
    - 改善案: EV閾値でベット選別

  【複勝】
    - 目標: 3着以内予測精度の最大化
    - 戦略: is_place ラベル（Top3）
    - 特性: 的中率高い、配当低い → 安定志向

  【馬連】(現状ROI: 79.4%)
    - 目標: Top2の順位予測精度
    - 戦略: Top1モデル + Top2モデルの組み合わせ
    - 改善案: 2着予測に特化したモデル

  【三連複】
    - 目標: Top3の予測精度
    - 戦略: 複勝モデル（Top3確率）を活用
    - 特性: 配当高い、的中難しい → 高配当狙い
    - 改善案: Top3確率の上位馬をBOX購入

  【三連単】
    - 目標: Top3の順位を完全予測
    - 戦略: LambdaRankでランキング学習
    - 特性: 最も配当高い、最も難しい
    - 改善案: 1着固定で流し購入
""")
    
    return results


if __name__ == "__main__":
    main()
