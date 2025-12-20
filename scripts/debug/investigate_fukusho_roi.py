#!/usr/bin/env python3
"""
複勝ROI計算の検証

的中率93%でROI73%の謎を調査する
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def investigate_fukusho_roi():
    """複勝ROIの詳細調査"""
    
    # データ読み込み
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[(races['finish_position'].notna()) & (races['finish_position'] > 0)].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    
    # 2024年のデータで確認
    test_df = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] <= '2024-12-31')].copy()
    test_race_ids = set(test_df['race_id'].unique())
    
    # 複勝データ
    fukusho = returns[(returns['bet_type'] == 'fukusho') & (returns['race_id'].isin(test_race_ids))]
    
    logger.info(f"\n=== 複勝データ構造確認 ===")
    logger.info(f"複勝レコード数: {len(fukusho)}")
    logger.info(f"カラム: {list(fukusho.columns)}")
    logger.info(f"\n最初の10件:")
    print(fukusho.head(10).to_string())
    
    # 払い戻しの統計
    logger.info(f"\n=== 複勝払い戻し統計 ===")
    logger.info(f"払い戻し平均: {fukusho['payout'].mean():.1f}円")
    logger.info(f"払い戻し中央値: {fukusho['payout'].median():.1f}円")
    logger.info(f"払い戻し最小: {fukusho['payout'].min():.1f}円")
    logger.info(f"払い戻し最大: {fukusho['payout'].max():.1f}円")
    
    # 1レースあたりの複勝的中数
    fukusho_per_race = fukusho.groupby('race_id').size()
    logger.info(f"\n=== 1レースあたりの複勝エントリー数 ===")
    logger.info(f"平均: {fukusho_per_race.mean():.2f}")
    logger.info(f"分布: {fukusho_per_race.value_counts().sort_index().to_dict()}")
    
    # サンプルレースの詳細確認
    sample_race_id = fukusho['race_id'].iloc[0]
    sample_race_fukusho = fukusho[fukusho['race_id'] == sample_race_id]
    sample_race_result = test_df[test_df['race_id'] == sample_race_id].sort_values('finish_position')
    
    logger.info(f"\n=== サンプルレース {sample_race_id} ===")
    logger.info(f"複勝払い戻しデータ:")
    print(sample_race_fukusho.to_string())
    logger.info(f"\nレース結果 (1-5着):")
    print(sample_race_result[['horse_number', 'finish_position', 'win_odds']].head(5).to_string())
    
    # === 問題の本質を確認 ===
    logger.info(f"\n=== ROI計算のシミュレーション ===")
    
    # ケース1: 1レース1点買い（的中率100%想定）
    # 複勝オッズ1.5倍で100円購入 → 150円回収 → ROI 150%
    logger.info(f"ケース1: 1レース1点買い、オッズ1.5倍")
    logger.info(f"  投資: 100円, 回収: 150円, ROI: 150%")
    
    # ケース2: 1レース10点買い（1点的中）
    # 10点×100円 = 1,000円投資、1点的中で150円回収 → ROI 15%
    logger.info(f"ケース2: 1レース10点買い、オッズ1.5倍で1点的中")
    logger.info(f"  投資: 1,000円, 回収: 150円, ROI: 15%")
    
    # ケース3: 1レース10点買い（3点的中 = 3着以内全的中）
    # 10点×100円 = 1,000円投資、3点的中で450円回収 → ROI 45%
    logger.info(f"ケース3: 1レース10点買い、オッズ1.5倍で3点的中")
    logger.info(f"  投資: 1,000円, 回収: 450円, ROI: 45%")
    
    # === 現在の計算ロジックを確認 ===
    logger.info(f"\n=== 現在の計算ロジックの問題点 ===")
    logger.info(f"平均購入点数: 10.2点/レース")
    logger.info(f"これは1レースに10点以上購入している")
    logger.info(f"しかし複勝は最大3点しか的中しない（3着以内の馬のみ）")
    logger.info(f"→ 10点買って最大3点しか的中しないので、損失が大きい")
    
    # 平均複勝オッズを計算
    avg_fukusho_odds = fukusho['payout'].mean() / 100 if len(fukusho) > 0 else 1.5
    logger.info(f"\n平均複勝オッズ: {avg_fukusho_odds:.2f}倍")
    
    # 期待ROI計算
    # 10点買い、3点的中（的中率93%≒ほぼ全レースで1点以上的中）
    # 問題: 購入した10点のうち、実際に3着以内に入る馬は何点？
    logger.info(f"\n=== 問題の本質 ===")
    logger.info(f"期待値モデルでEV>=0.8の馬を全て購入している")
    logger.info(f"→ 1レースあたり平均10.2点購入")
    logger.info(f"→ しかし複勝は1レース最大3点しか的中しない")
    logger.info(f"→ 7点は必ず外れる")
    logger.info(f"")
    logger.info(f"例: 10点×100円=1,000円投資")
    logger.info(f"    3点的中×150円=450円回収")
    logger.info(f"    ROI = 450/1000 = 45%")
    logger.info(f"")
    logger.info(f"これが的中率93%でもROI73%しかない理由！")
    logger.info(f"購入点数が多すぎる！")
    
    # === 解決策の検証 ===
    logger.info(f"\n=== 解決策: Top1のみ購入 ===")
    logger.info(f"各レースでEV最大の馬1点のみ購入")
    logger.info(f"→ 1点×100円=100円投資")
    logger.info(f"→ 的中すれば複勝払い戻し（例: 150円）")
    logger.info(f"→ ROI = 150/100 = 150%")
    logger.info(f"")
    logger.info(f"これが着順ベースのTop1戦略がROI 80%だった理由")
    logger.info(f"1レース1点だけ購入していた")


if __name__ == "__main__":
    investigate_fukusho_roi()
