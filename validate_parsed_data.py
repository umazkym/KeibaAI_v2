#!/usr/bin/env python3
"""
パース済みデータの品質検証スクリプト（修正版）

実行方法:
python validate_parsed_data.py

検証項目:
1. horses.parquetの重複チェック
2. horses.parquetの空データ（Noneのみの行）チェック
3. races.parquetのfailed_to_finishフラグの妥当性チェック
4. jockey_id, trainer_idの欠損状況の確認
"""

import pandas as pd
import sys
from pathlib import Path
from collections import Counter

# ANSIカラーコード
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_section(title: str):
    """セクションヘッダーを表示"""
    print(f"\n{Colors.HEADER}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{title}{Colors.ENDC}")
    print(f"{Colors.HEADER}{'='*60}{Colors.ENDC}")


def validate_horses_parquet(file_path: Path):
    """horses.parquetの検証"""
    print_section("horses.parquet の検証")
    
    if not file_path.exists():
        print(f"{Colors.FAIL}✗ ファイルが見つかりません: {file_path}{Colors.ENDC}")
        return False
    
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    
    print(f"総行数: {total_rows}")
    print(f"カラム数: {len(df.columns)}")
    print(f"カラム: {list(df.columns)}\n")
    
    # 1. 重複チェック
    print(f"{Colors.OKBLUE}[1] horse_idの重複チェック{Colors.ENDC}")
    duplicates = df[df.duplicated(subset=['horse_id'], keep=False)]
    
    if len(duplicates) == 0:
        print(f"{Colors.OKGREEN}✓ 重複なし{Colors.ENDC}")
    else:
        print(f"{Colors.FAIL}✗ {len(duplicates)}行の重複を検出{Colors.ENDC}")
        print("\n重複しているhorse_idのサンプル（最初の10件）:")
        dup_ids = duplicates['horse_id'].value_counts().head(10)
        for horse_id, count in dup_ids.items():
            print(f"  - {horse_id}: {count}回")
        
        print("\n重複データのサンプル（最初の5行）:")
        print(duplicates.head(5).to_string())
    
    # 2. 空データチェック（horse_id以外がすべてNull）
    print(f"\n{Colors.OKBLUE}[2] 空データチェック（horse_id以外がすべてNull）{Colors.ENDC}")
    
    # horse_id以外のカラムを取得
    non_id_cols = [col for col in df.columns if col != 'horse_id']
    
    # すべてのnon_id_colsがNullの行を検出
    empty_mask = df[non_id_cols].isnull().all(axis=1)
    empty_rows = df[empty_mask]
    
    if len(empty_rows) == 0:
        print(f"{Colors.OKGREEN}✓ 空データなし{Colors.ENDC}")
    else:
        print(f"{Colors.FAIL}✗ {len(empty_rows)}行の空データを検出（全体の{len(empty_rows)/total_rows*100:.2f}%）{Colors.ENDC}")
        print("\n空データのサンプル（最初の5行）:")
        print(empty_rows.head(5).to_string())
    
    # 3. 各カラムの欠損率
    print(f"\n{Colors.OKBLUE}[3] 各カラムの欠損率{Colors.ENDC}")
    null_counts = df.isnull().sum()
    null_percentages = (null_counts / total_rows) * 100
    
    missing_summary = pd.DataFrame({
        'null_count': null_counts,
        'null_percentage': null_percentages
    }).sort_values(by='null_percentage', ascending=False)
    
    print(missing_summary.to_string())
    
    # 判定
    issues_found = len(duplicates) > 0 or len(empty_rows) > 0
    
    if not issues_found:
        print(f"\n{Colors.OKGREEN}✓ horses.parquet は正常です{Colors.ENDC}")
    else:
        print(f"\n{Colors.WARNING}⚠ horses.parquet に問題が検出されました{Colors.ENDC}")
    
    return not issues_found


def validate_races_parquet(file_path: Path):
    """races.parquetの検証"""
    print_section("races.parquet の検証")
    
    if not file_path.exists():
        print(f"{Colors.FAIL}✗ ファイルが見つかりません: {file_path}{Colors.ENDC}")
        return False
    
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    
    print(f"総行数: {total_rows}")
    print(f"カラム数: {len(df.columns)}\n")
    
    # 1. failed_to_finishフラグの存在確認
    print(f"{Colors.OKBLUE}[1] failed_to_finishフラグの確認{Colors.ENDC}")
    
    if 'failed_to_finish' not in df.columns:
        print(f"{Colors.WARNING}⚠ failed_to_finishカラムが存在しません（未実装の可能性）{Colors.ENDC}")
        has_failed_flag = False
    else:
        print(f"{Colors.OKGREEN}✓ failed_to_finishカラムが存在します{Colors.ENDC}")
        has_failed_flag = True
        
        # failed_to_finishがTrueの行数
        failed_count = df['failed_to_finish'].sum()
        print(f"  - 競走中止（failed_to_finish=True）: {failed_count}行")
    
    # 2. scratchedフラグの確認
    print(f"\n{Colors.OKBLUE}[2] 出走取消（scratched）の確認{Colors.ENDC}")
    
    if 'scratched' not in df.columns:
        print(f"{Colors.WARNING}⚠ scratchedカラムが存在しません{Colors.ENDC}")
        scratched_count = None
    else:
        scratched_count = df['scratched'].sum()
        print(f"  - 出走取消（scratched=True）: {scratched_count}行")
    
    # 3. finish_positionのNull数
    print(f"\n{Colors.OKBLUE}[3] 着順（finish_position）のNull数{Colors.ENDC}")
    finish_null_count = df['finish_position'].isnull().sum()
    print(f"  - finish_positionがNull: {finish_null_count}行")
    
    # 4. finish_timeのNull数
    print(f"\n{Colors.OKBLUE}[4] 走破タイム（finish_time_seconds）のNull数{Colors.ENDC}")
    time_null_count = df['finish_time_seconds'].isnull().sum()
    print(f"  - finish_time_secondsがNull: {time_null_count}行")
    
    # 5. 整合性チェック
    print(f"\n{Colors.OKBLUE}[5] データ整合性チェック{Colors.ENDC}")
    
    if scratched_count is not None:
        # 理論値: scratched + failed_to_finish = finish_positionのNull数
        if has_failed_flag:
            expected_null = scratched_count + df['failed_to_finish'].sum()
        else:
            expected_null = scratched_count
        
        if finish_null_count == expected_null:
            print(f"{Colors.OKGREEN}✓ finish_positionのNull数が整合しています{Colors.ENDC}")
            print(f"  理論値: {expected_null}, 実測値: {finish_null_count}")
        else:
            print(f"{Colors.WARNING}⚠ finish_positionのNull数に差異があります{Colors.ENDC}")
            print(f"  理論値: {expected_null}, 実測値: {finish_null_count}, 差: {finish_null_count - expected_null}")
    
    # 6. jockey_id, trainer_idの欠損
    print(f"\n{Colors.OKBLUE}[6] 主要IDフィールドの欠損{Colors.ENDC}")
    
    for col in ['jockey_id', 'trainer_id']:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = null_count / total_rows * 100
            
            if null_pct < 1.0:
                print(f"{Colors.OKGREEN}✓ {col}: {null_count}行 ({null_pct:.2f}%) - 許容範囲内{Colors.ENDC}")
            elif null_pct < 5.0:
                print(f"{Colors.WARNING}⚠ {col}: {null_count}行 ({null_pct:.2f}%) - やや多い{Colors.ENDC}")
            else:
                print(f"{Colors.FAIL}✗ {col}: {null_count}行 ({null_pct:.2f}%) - 多すぎる{Colors.ENDC}")
        else:
            print(f"{Colors.FAIL}✗ {col}カラムが存在しません{Colors.ENDC}")
    
    return True


def main():
    """メイン処理"""
    print(f"{Colors.BOLD}Keiba AI パース済みデータ品質検証{Colors.ENDC}")
    
    # プロジェクトルートの取得
    project_root = Path(__file__).resolve().parent
    parsed_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet'
    
    horses_path = parsed_dir / 'horses' / 'horses.parquet'
    races_path = parsed_dir / 'races' / 'races.parquet'
    
    # 検証実行
    horses_ok = validate_horses_parquet(horses_path)
    races_ok = validate_races_parquet(races_path)
    
    # 総合判定
    print_section("総合判定")
    
    if horses_ok and races_ok:
        print(f"{Colors.OKGREEN}✓ すべての検証に合格しました{Colors.ENDC}")
        return 0
    else:
        print(f"{Colors.FAIL}✗ 一部の検証で問題が検出されました{Colors.ENDC}")
        print("\n推奨アクション:")
        
        if not horses_ok:
            print("  1. 既存のhorses.parquetを削除")
            print("  2. run_parsing_pipeline_local.py（修正版）を実行")
        
        if not races_ok:
            print("  3. 既存のraces.parquetを削除")
            print("  4. run_parsing_pipeline_local.py（修正版）を実行")
        
        return 1


if __name__ == "__main__":
    sys.exit(main())