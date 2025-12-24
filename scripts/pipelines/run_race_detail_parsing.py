#!/usr/bin/env python3
"""
レース詳細データパースパイプライン

全HTMLファイルから払い戻し・ラップ・コーナーデータを抽出し、Parquetに保存
"""
from pathlib import Path
import pandas as pd
import logging
import sys
from tqdm import tqdm
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.parsers.race_detail_parser import parse_race_html

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def run_pipeline(test_mode: bool = False, test_limit: int = 100):
    """
    パイプライン実行
    
    Args:
        test_mode: テストモードの場合True（制限付き実行）
        test_limit: テストモードでの処理ファイル数
    """
    base_dir = project_root / "keibaai/data/raw/html/race"
    output_dir = project_root / "keibaai/data/parsed/parquet"
    
    # 出力ディレクトリ作成
    (output_dir / "returns").mkdir(parents=True, exist_ok=True)
    (output_dir / "race_details").mkdir(parents=True, exist_ok=True)
    (output_dir / "corners").mkdir(parents=True, exist_ok=True)
    
    # HTMLファイル取得
    html_files = sorted(list(base_dir.glob("*.bin")))
    
    if test_mode:
        html_files = html_files[:test_limit]
        log.info(f"テストモード: {test_limit}ファイルのみ処理")
    
    log.info(f"処理対象ファイル数: {len(html_files):,}")
    
    # 集約用
    all_returns = {k: [] for k in ['tansho', 'fukusho', 'wakuren', 'umaren', 'wide', 'umatan', 'sanrenpuku', 'sanrentan']}
    all_laps = []
    all_corners_raw = []
    all_corner_positions = []
    errors = []
    
    # 処理
    for file_path in tqdm(html_files, desc="パース処理"):
        try:
            result = parse_race_html(str(file_path))
            
            # 払い戻し
            for k, df in result['returns'].items():
                if not df.empty:
                    all_returns[k].append(df)
            
            # ラップ
            if result['lap_times']:
                all_laps.append(result['lap_times'])
            
            # コーナー
            all_corners_raw.extend(result['corners_raw'])
            all_corner_positions.extend(result['corner_positions'])
            
        except Exception as e:
            errors.append({'file': file_path.name, 'error': str(e)})
    
    # 結果保存
    log.info("Parquetファイルを保存中...")
    
    # 払い戻し
    returns_dfs = []
    for bet_type, dfs in all_returns.items():
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            combined['bet_type'] = bet_type
            returns_dfs.append(combined)
    
    if returns_dfs:
        returns_df = pd.concat(returns_dfs, ignore_index=True)
        returns_path = output_dir / "returns/returns.parquet"
        returns_df.to_parquet(returns_path, index=False)
        log.info(f"  returns.parquet: {len(returns_df):,}件")
    
    # ラップタイム + コーナー生テキスト
    if all_laps:
        laps_df = pd.DataFrame(all_laps)
        
        # コーナー生テキストをrace_idでピボット
        if all_corners_raw:
            corners_raw_df = pd.DataFrame(all_corners_raw)
            corners_pivot = corners_raw_df.pivot(index='race_id', columns='corner', values='order_raw')
            corners_pivot.columns = [f'corner_{c}_raw' for c in corners_pivot.columns]
            corners_pivot = corners_pivot.reset_index()
            
            # ラップとマージ
            laps_df = laps_df.merge(corners_pivot, on='race_id', how='left')
        
        race_details_path = output_dir / "race_details/race_details.parquet"
        laps_df.to_parquet(race_details_path, index=False)
        log.info(f"  race_details.parquet: {len(laps_df):,}件")
    
    # コーナー馬身差
    if all_corner_positions:
        corners_df = pd.DataFrame(all_corner_positions)
        corners_path = output_dir / "corners/corner_positions.parquet"
        corners_df.to_parquet(corners_path, index=False)
        log.info(f"  corner_positions.parquet: {len(corners_df):,}件")
    
    # エラーレポート
    if errors:
        log.warning(f"エラー: {len(errors)}件")
        errors_df = pd.DataFrame(errors)
        errors_path = output_dir / "parse_errors.csv"
        errors_df.to_csv(errors_path, index=False)
        log.info(f"  エラーログ: {errors_path}")
    
    log.info("パイプライン完了")
    
    return {
        'returns_count': len(returns_df) if returns_dfs else 0,
        'laps_count': len(all_laps),
        'corners_count': len(all_corner_positions),
        'errors_count': len(errors)
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='レース詳細データパースパイプライン')
    parser.add_argument('--test', action='store_true', help='テストモード（100ファイルのみ）')
    parser.add_argument('--test-limit', type=int, default=100, help='テストモードでの処理ファイル数')
    
    args = parser.parse_args()
    
    run_pipeline(test_mode=args.test, test_limit=args.test_limit)
