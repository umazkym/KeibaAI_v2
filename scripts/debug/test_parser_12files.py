#!/usr/bin/env python3
"""
12ファイル一括テスト
"""
from pathlib import Path
import pandas as pd
import sys

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.modules.parsers.race_detail_parser import parse_race_html

def main():
    base_dir = Path("keibaai/data/raw/html/race")
    
    print("=" * 70)
    print("12ファイル一括テスト")
    print("=" * 70)
    
    all_returns = {k: [] for k in ['tansho', 'fukusho', 'wakuren', 'umaren', 'wide', 'umatan', 'sanrenpuku', 'sanrentan']}
    all_laps = []
    all_corners_raw = []
    all_corner_positions = []
    errors = []
    
    for i in range(1, 13):
        file_name = f"2020010101{i:02d}.bin"
        file_path = base_dir / file_name
        
        if not file_path.exists():
            errors.append(f"{file_name}: ファイルなし")
            continue
        
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
            
            print(f"✅ {file_name}")
        except Exception as e:
            errors.append(f"{file_name}: {e}")
            print(f"❌ {file_name}: {e}")
    
    # 集計
    print("\n" + "=" * 70)
    print("【パース結果サマリ】")
    print("=" * 70)
    
    print("\n--- 払い戻し ---")
    for k, dfs in all_returns.items():
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            print(f"  {k}: {len(combined)}件")
    
    print(f"\n--- ラップタイム ---")
    print(f"  {len(all_laps)}件")
    
    print(f"\n--- コーナー生テキスト ---")
    print(f"  {len(all_corners_raw)}件")
    
    print(f"\n--- コーナー馬身差 ---")
    print(f"  {len(all_corner_positions)}件")
    
    if errors:
        print(f"\n--- エラー ---")
        for e in errors:
            print(f"  {e}")
    else:
        print("\n✅ 全ファイル正常パース完了")


if __name__ == "__main__":
    main()
