"""
統合データ前処理パイプライン
- 全Parquetファイルから障害レースを除外
- races/shutubaを一致させる
- 中止レースを除外

Usage: python scripts/pipelines/preprocess_data.py
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    data_dir = Path('keibaai/data/parsed/parquet')
    
    log.info("=" * 80)
    log.info("統合データ前処理パイプライン")
    log.info("=" * 80)
    
    # ==========================================================================
    # 1. racesを読み込み、障害レースを除外
    # ==========================================================================
    log.info("\n【Step 1】races.parquetから障害レースを除外")
    
    races_raw = pd.read_parquet(data_dir / 'races/races.parquet')
    log.info(f"  元のraces: {len(races_raw):,}件")
    
    # track_surface分布
    log.info(f"  track_surface分布: {races_raw['track_surface'].value_counts().to_dict()}")
    
    # 障害レースを除外
    races = races_raw[races_raw['track_surface'] != '障害'].copy()
    excluded_obstacle = len(races_raw) - len(races)
    log.info(f"  障害レース除外: {excluded_obstacle:,}件")
    log.info(f"  除外後のraces: {len(races):,}件")
    
    # 有効なrace_idをセット
    valid_race_ids = set(races['race_id'].unique())
    log.info(f"  有効なrace_id数: {len(valid_race_ids):,}")
    
    # ==========================================================================
    # 2. shutubaをracesと一致させる（障害+中止除外）
    # ==========================================================================
    log.info("\n【Step 2】shutuba.parquetをracesと一致させる")
    
    shutuba_raw = pd.read_parquet(data_dir / 'shutuba/shutuba.parquet')
    log.info(f"  元のshutuba: {len(shutuba_raw):,}件")
    
    # racesに存在するrace_idのみ残す
    shutuba = shutuba_raw[shutuba_raw['race_id'].isin(valid_race_ids)].copy()
    excluded_shutuba = len(shutuba_raw) - len(shutuba)
    log.info(f"  除外（障害+中止等）: {excluded_shutuba:,}件")
    log.info(f"  除外後のshutuba: {len(shutuba):,}件")
    
    # ==========================================================================
    # 3. returnsをracesと一致させる
    # ==========================================================================
    log.info("\n【Step 3】returns.parquetをracesと一致させる")
    
    returns_raw = pd.read_parquet(data_dir / 'returns/returns.parquet')
    log.info(f"  元のreturns: {len(returns_raw):,}件")
    
    returns = returns_raw[returns_raw['race_id'].isin(valid_race_ids)].copy()
    excluded_returns = len(returns_raw) - len(returns)
    log.info(f"  除外: {excluded_returns:,}件")
    log.info(f"  除外後のreturns: {len(returns):,}件")
    
    # ==========================================================================
    # 4. race_detailsをracesと一致させる
    # ==========================================================================
    log.info("\n【Step 4】race_details.parquetをracesと一致させる")
    
    race_details_raw = pd.read_parquet(data_dir / 'race_details/race_details.parquet')
    log.info(f"  元のrace_details: {len(race_details_raw):,}件")
    
    race_details = race_details_raw[race_details_raw['race_id'].isin(valid_race_ids)].copy()
    excluded_rd = len(race_details_raw) - len(race_details)
    log.info(f"  除外: {excluded_rd:,}件")
    log.info(f"  除外後のrace_details: {len(race_details):,}件")
    
    # ==========================================================================
    # 5. cornersをracesと一致させる
    # ==========================================================================
    log.info("\n【Step 5】corner_positions.parquetをracesと一致させる")
    
    corners_raw = pd.read_parquet(data_dir / 'corners/corner_positions.parquet')
    log.info(f"  元のcorners: {len(corners_raw):,}件")
    
    corners = corners_raw[corners_raw['race_id'].isin(valid_race_ids)].copy()
    excluded_corners = len(corners_raw) - len(corners)
    log.info(f"  除外: {excluded_corners:,}件")
    log.info(f"  除外後のcorners: {len(corners):,}件")
    
    # ==========================================================================
    # 6. horses/pedigreesはそのまま（馬情報は全馬必要）
    # ==========================================================================
    log.info("\n【Step 6】horses/pedigreesはそのまま維持")
    
    horses = pd.read_parquet(data_dir / 'horses/horses.parquet')
    pedigrees = pd.read_parquet(data_dir / 'pedigrees/pedigrees.parquet')
    log.info(f"  horses: {len(horses):,}件")
    log.info(f"  pedigrees: {len(pedigrees):,}件")
    
    # ==========================================================================
    # 7. 保存
    # ==========================================================================
    log.info("\n【Step 7】処理済みデータを保存")
    
    # 上書き保存
    races.to_parquet(data_dir / 'races/races.parquet', index=False)
    shutuba.to_parquet(data_dir / 'shutuba/shutuba.parquet', index=False)
    returns.to_parquet(data_dir / 'returns/returns.parquet', index=False)
    race_details.to_parquet(data_dir / 'race_details/race_details.parquet', index=False)
    corners.to_parquet(data_dir / 'corners/corner_positions.parquet', index=False)
    
    log.info("  保存完了")
    
    # ==========================================================================
    # サマリー
    # ==========================================================================
    log.info("\n" + "=" * 80)
    log.info("処理完了サマリー")
    log.info("=" * 80)
    
    log.info(f"""
| ファイル | 処理前 | 処理後 | 除外数 |
|---------|--------|--------|-------|
| races | {len(races_raw):,} | {len(races):,} | {excluded_obstacle:,} |
| shutuba | {len(shutuba_raw):,} | {len(shutuba):,} | {excluded_shutuba:,} |
| returns | {len(returns_raw):,} | {len(returns):,} | {excluded_returns:,} |
| race_details | {len(race_details_raw):,} | {len(race_details):,} | {excluded_rd:,} |
| corners | {len(corners_raw):,} | {len(corners):,} | {excluded_corners:,} |
| horses | {len(horses):,} | {len(horses):,} | 0 |
| pedigrees | {len(pedigrees):,} | {len(pedigrees):,} | 0 |
""")
    
    # 整合性確認
    log.info("\n整合性確認:")
    log.info(f"  races件数: {len(races):,}")
    log.info(f"  shutuba件数: {len(shutuba):,}")
    log.info(f"  差分: {len(shutuba) - len(races):,}")
    
    if len(races) == len(shutuba):
        log.info("  ✅ races と shutuba が一致")
    else:
        log.warning("  ⚠️ races と shutuba が不一致")

if __name__ == '__main__':
    main()
