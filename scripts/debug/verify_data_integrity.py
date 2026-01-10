# -*- coding: utf-8 -*-
"""
データ完全性検証スクリプト

既存の.binファイルを全て解析し、データが不完全なレースを特定します。
三連複/三連単の組み合わせ数が理論値より少ないレースをリストアップ。

使用方法:
    python scripts/debug/verify_data_integrity.py
"""
import sys
import re
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

RAW_ODDS_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'raw' / 'html' / 'odds'


def count_odds_in_file(file_path: Path) -> int:
    """ファイル内のオッズ数をカウント"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # パーサーと同じパターンで検索
        matches = re.findall(r'id="odds-[78]-\d{6}">[0-9,.]+</span>', content)
        return len(matches)
    except Exception as e:
        return -1


def get_horse_count_from_file(file_path: Path) -> int:
    """ファイルから頭数を推定（AXIS数から）"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        axis_count = content.count('<!-- AXIS')
        return axis_count if axis_count > 0 else 0
    except:
        return 0


def calculate_expected_count(horse_count: int, bet_type: str) -> int:
    """理論上の組み合わせ数を計算"""
    if horse_count < 3:
        return 0
    if bet_type == 'sanrenpuku':
        # nC3 = n! / (3! * (n-3)!)
        return horse_count * (horse_count - 1) * (horse_count - 2) // 6
    elif bet_type == 'sanrentan':
        # nP3 = n! / (n-3)!
        return horse_count * (horse_count - 1) * (horse_count - 2)
    return 0


def main():
    print("=" * 60)
    print("データ完全性検証")
    print("=" * 60)
    
    incomplete_races = []
    total_files = 0
    complete_files = 0
    
    for bet_type in ['sanrenpuku', 'sanrentan']:
        bet_dir = RAW_ODDS_DIR / bet_type
        if not bet_dir.exists():
            continue
        
        files = list(bet_dir.glob('*.bin'))
        print(f"\n{bet_type}: {len(files)}ファイル検証中...")
        
        for file_path in files:
            total_files += 1
            race_id = file_path.stem
            
            horse_count = get_horse_count_from_file(file_path)
            actual_count = count_odds_in_file(file_path)
            expected_count = calculate_expected_count(horse_count, bet_type)
            
            if actual_count < 0:
                incomplete_races.append({
                    'race_id': race_id,
                    'bet_type': bet_type,
                    'reason': 'ファイル読み込みエラー',
                    'actual': 0,
                    'expected': expected_count
                })
            elif expected_count > 0 and actual_count < expected_count * 0.95:
                # 5%以上欠損があれば不完全と判定
                incomplete_races.append({
                    'race_id': race_id,
                    'bet_type': bet_type,
                    'reason': f'{actual_count}/{expected_count} ({100*actual_count//expected_count}%)',
                    'actual': actual_count,
                    'expected': expected_count
                })
            else:
                complete_files += 1
    
    print("\n" + "=" * 60)
    print(f"検証完了: {total_files}ファイル中 {complete_files}ファイル完全")
    print("=" * 60)
    
    if incomplete_races:
        print(f"\n不完全なデータ: {len(incomplete_races)}件")
        print("-" * 60)
        
        # error_races.log形式で保存
        error_log_path = RAW_ODDS_DIR / 'incomplete_races.log'
        with open(error_log_path, 'w', encoding='utf-8') as f:
            for item in incomplete_races:
                print(f"  {item['race_id']} ({item['bet_type']}): {item['reason']}")
                f.write(f"{item['race_id']},{item['bet_type']}\n")
        
        print(f"\n不完全レースリストを保存: {error_log_path}")
    else:
        print("\n✅ 全てのデータが完全です")


if __name__ == "__main__":
    main()
