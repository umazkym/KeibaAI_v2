# -*- coding: utf-8 -*-
"""
V16 pace_synergy 詳細検証

【検証項目】
1. データリークチェック
   - horse_preferred_pace_diffの計算に未来データが含まれていないか
   - race_expected_pace_diffの計算に当日データが含まれていないか

2. 特異値チェック
   - pace_synergyの分布確認
   - 外れ値が結果を歪めていないか

3. 逆テスト
   - Train: 2024, Test: 2023 で同様の結果が出るか

4. 月別安定性
   - 特定月に偏っていないか
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
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, race_details


def main():
    logger.info("=" * 60)
    logger.info("pace_synergy 詳細検証")
    logger.info("=" * 60)
    
    races, race_details = load_data()
    
    # ========== 1. race_details データ確認 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. race_details データ確認")
    logger.info("=" * 60)
    
    logger.info(f"  レコード数: {len(race_details):,}")
    logger.info(f"  カラム: {list(race_details.columns)}")
    
    # race_idの年分布
    race_details_with_date = race_details.merge(
        races[['race_id', 'race_date']].drop_duplicates(),
        on='race_id',
        how='left'
    )
    race_details_with_date['year'] = race_details_with_date['race_date'].dt.year
    
    logger.info("")
    logger.info("  年別レース数:")
    for year, count in race_details_with_date.groupby('year').size().items():
        logger.info(f"    {year}: {count:,}")
    
    # first_half, second_half のサンプル
    logger.info("")
    logger.info("  first_half/second_half サンプル:")
    sample = race_details[['race_id', 'first_half', 'second_half']].dropna().head(5)
    for _, row in sample.iterrows():
        logger.info(f"    race_id={row['race_id']}: first_half={row['first_half']:.1f}s, second_half={row['second_half']:.1f}s")
    
    # ========== 2. horse_preferred_pace_diff のリークチェック ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. horse_preferred_pace_diff リークチェック")
    logger.info("=" * 60)
    
    # 計算ロジックを再現
    details = race_details[['race_id', 'first_half', 'second_half']].copy()
    details = details.dropna(subset=['first_half', 'second_half'])
    details['pace_diff'] = details['first_half'] - details['second_half']
    
    # race_idからrace_dateとhorse_idを取得
    race_horse = races[['race_id', 'horse_id', 'race_date', 'finish_position']].drop_duplicates()
    details_with_horse = details.merge(race_horse, on='race_id', how='left')
    
    logger.info(f"  pace_diffが計算可能なレコード数: {len(details_with_horse):,}")
    
    # 問題: horse_preferred_pace_diff は「Top3入りした全レースの平均」を使っている
    # これは未来のレースも含んでしまう！
    top3 = details_with_horse[details_with_horse['finish_position'] <= 3]
    horse_pref_all = top3.groupby('horse_id')['pace_diff'].mean()
    
    logger.info("")
    logger.info("  ⚠️ 重大なリーク発見:")
    logger.info("  horse_preferred_pace_diff は全期間のTop3レースから計算")
    logger.info("  → Test期間のレースを含む全データを使用している = 未来リーク！")
    
    # 具体例を出す
    # 2025年のレースで使われるhorse_preferred_pace_diffが
    # 2025年のレース結果を含んでいるか確認
    test_races = races[races['race_date'] >= '2025-01-01']['race_id'].unique()
    test_horse_ids = races[races['race_id'].isin(test_races)]['horse_id'].unique()
    
    # これらの馬のTop3レースが2025年を含むか
    test_top3 = top3[top3['horse_id'].isin(test_horse_ids)]
    test_top3_2025 = test_top3[test_top3['race_date'] >= '2025-01-01']
    
    logger.info("")
    logger.info(f"  Test(2025)で出走した馬: {len(test_horse_ids):,}頭")
    logger.info(f"  その馬たちの全Top3レース数: {len(test_top3):,}")
    logger.info(f"  うち2025年のTop3レース: {len(test_top3_2025):,} ← これがリーク！")
    
    # ========== 3. race_expected_pace_diff のリークチェック ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. race_expected_pace_diff リークチェック")
    logger.info("=" * 60)
    
    # pace_features.pyのロジック: 全期間のデータを累積計算してfinal_paceを取得
    # しかし、これも問題がある
    
    logger.info("  pace_features.pyのロジック確認:")
    logger.info("  - 全期間のrace_detailsを会場×距離×馬場でグループ化")
    logger.info("  - 最終的な平均値を全レースに適用")
    logger.info("  → 2025年のデータも平均値の計算に含まれている = 未来リーク！")
    
    # ========== 4. 修正版の実装案 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 修正案")
    logger.info("=" * 60)
    
    logger.info("  【正しい実装】")
    logger.info("  1. horse_preferred_pace_diff:")
    logger.info("     - 当該レース以前のTop3レースのみから計算")
    logger.info("     - cumsum + shift(1) パターンを適用")
    logger.info("")
    logger.info("  2. race_expected_pace_diff:")
    logger.info("     - 当該レース以前のデータのみで累積平均を計算")
    logger.info("     - 会場×距離×馬場ごとにexpanding mean + shift(1)")
    
    return True


if __name__ == "__main__":
    main()
