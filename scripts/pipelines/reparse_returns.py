# -*- coding: utf-8 -*-
"""
払い戻しデータ再パーススクリプト

現状:
- returns.parquetが全レースをカバーしていない（2025年は18.5%欠損）
- race_detail_parser.parse_returns()で払い戻しデータを抽出可能

処理:
- keibaai/data/raw/html/race/*.binから払い戻しデータを抽出
- returns.parquetを再構築
"""
import os
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import traceback

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "keibaai" / "src"))

from parsers.race_detail_parser import parse_race_html


def parse_returns_from_file(file_path: str):
    """ファイルから払い戻しデータをパース"""
    try:
        result = parse_race_html(file_path)
        returns_dict = result.get('returns', {})
        
        all_returns = []
        for bet_type, df in returns_dict.items():
            if df is not None and not df.empty:
                df = df.copy()
                df['bet_type'] = bet_type
                all_returns.append(df)
        
        if all_returns:
            combined = pd.concat(all_returns, ignore_index=True)
            return {"status": "success", "data": combined.to_dict("records"), "file": file_path}
        else:
            return {"status": "empty", "file": file_path}
    except Exception as e:
        return {"status": "error", "error": str(e), "traceback": traceback.format_exc(), "file": file_path}


def main():
    print("=" * 80)
    print("払い戻しデータ再パース処理")
    print("=" * 80)
    
    # HTMLファイル収集
    raw_html_dir = project_root / "keibaai" / "data" / "raw" / "html" / "race"
    html_files = []
    for root, _, files in os.walk(raw_html_dir):
        for file in files:
            if file.endswith((".html", ".bin")):
                html_files.append(os.path.join(root, file))
    
    print(f"対象HTMLファイル: {len(html_files):,}件")
    
    # 既存のreturnsをバックアップ
    output_dir = project_root / "keibaai" / "data" / "parsed" / "parquet" / "returns"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "returns.parquet"
    backup_path = output_dir / "returns_backup.parquet"
    
    if output_path.exists():
        # バックアップ作成
        import shutil
        shutil.copy(output_path, backup_path)
        print(f"既存ファイルをバックアップ: {backup_path}")
    
    # 並列パース
    all_returns = []
    error_count = 0
    empty_count = 0
    success_count = 0
    
    num_workers = max(1, cpu_count() - 1)
    print(f"ワーカー数: {num_workers}")
    
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(parse_returns_from_file, html_files),
            total=len(html_files),
            desc="払い戻しパース"
        ))
    
    for result in results:
        if result["status"] == "success":
            all_returns.extend(result["data"])
            success_count += 1
        elif result["status"] == "empty":
            empty_count += 1
        else:
            error_count += 1
    
    print(f"\n結果: 成功 {success_count:,}件 / 空 {empty_count:,}件 / エラー {error_count:,}件")
    
    # DataFrame作成・保存
    if all_returns:
        returns_df = pd.DataFrame(all_returns)
        print(f"\n生成されたレコード数: {len(returns_df):,}件")
        
        # bet_typeごとの件数
        print("\n馬券種別件数:")
        print(returns_df['bet_type'].value_counts())
        
        # 年別件数
        returns_df['race_id_str'] = returns_df['race_id'].astype(str)
        returns_df['year'] = returns_df['race_id_str'].str[:4].astype(int)
        print("\n年別件数:")
        print(returns_df['year'].value_counts().sort_index())
        
        # race_idのユニーク数
        unique_races = returns_df['race_id'].nunique()
        print(f"\nユニークレースID数: {unique_races:,}")
        
        # 保存
        returns_df.to_parquet(output_path, index=False)
        print(f"\n保存完了: {output_path}")
    else:
        print("払い戻しデータが見つかりませんでした")
    
    print("\n" + "=" * 80)
    print("完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
