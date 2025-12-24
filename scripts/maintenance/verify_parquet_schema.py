
import pandas as pd
import glob
import re
import os
from pathlib import Path
from typing import Dict, List, Tuple

# プロジェクトルート
PROJECT_ROOT = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2")
DOCS_PATH = PROJECT_ROOT / "docs" / "system" / "03_データモデル.md"
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def parse_markdown_table(doc_path: Path) -> Dict[str, Dict[str, str]]:
    """マークダウンのテーブルからテーブル定義（カラム名: 型）を抽出"""
    content = doc_file = doc_path.read_text(encoding='utf-8', errors='ignore')
    
    tables = {}
    current_table = None
    
    # テーブルごとに簡易パース
    # ## 2.1 レース情報 (races.parquet) のようなヘッダーを探す
    lines = content.split('\n')
    for i, line in enumerate(lines):
        # テーブル特定
        header_match = re.search(r'(\w+\.parquet)', line)
        if header_match:
            current_table = header_match.group(1)
            tables[current_table] = {}
            continue
            
        if current_table and "|" in line and "---" not in line:
            # | カラム名 | 型 | 説明 |
            cols = [c.strip() for c in line.split('|')]
            if len(cols) >= 3:
                col_name = cols[1].replace('`', '')
                col_type = cols[2]
                
                # ヘッダー行スキップ
                if col_name == "カラム名" or col_name == "Column":
                    continue
                    
                if col_name:
                    tables[current_table][col_name] = col_type

    return tables

def get_parquet_schema(file_path: Path) -> Dict[str, str]:
    """Parquetファイルのスキーマを取得"""
    if not file_path.exists():
        return {}
    
    try:
        # スキーマだけ読むためにfastparquetやpyarrowを使うが、ここではpandasで少しだけ読む
        df = pd.read_parquet(file_path) # 全読みは重いかもしれないが、一旦これで行く
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    except Exception as e:
        return {"error": str(e)}

def verify_consistency():
    print(f"Reading tables from {DOCS_PATH}...")
    doc_schemas = parse_markdown_table(DOCS_PATH)
    
    report = []
    
    for table_name, doc_cols in doc_schemas.items():
        # ファイルを探す (再帰的ではなく、特定のパス構造を仮定)
        # 構造: keibaai/data/parsed/parquet/races/races.parquet または races.parquet
        
        candidates = list(DATA_DIR.rglob(table_name))
        if not candidates:
            report.append(f"[MISSING_FILE] Documented '{table_name}' not found locally.")
            continue
            
        real_file = candidates[0]
        print(f"Checking {table_name} vs {real_file}...")
        
        real_schema = get_parquet_schema(real_file)
        if "error" in real_schema:
            report.append(f"[READ_ERROR] Could not read {real_file}: {real_schema['error']}")
            continue
            
        # 1. ドキュメントにあるが実ファイルにない
        for col in doc_cols:
            if col not in real_schema:
                report.append(f"[MISSING_COLUMN] In {table_name} doc: '{col}' is missing in actual parquet.")
        
        # 2. 実ファイルにあるがドキュメントにない
        # (全てを報告すると多すぎるかもしれないが、主要なものは出したい)
        for col in real_schema:
            if col not in doc_cols:
                report.append(f"[UNDOCUMENTED_COLUMN] In {table_name}: '{col}' exists but not in doc.")
                
    return report

if __name__ == "__main__":
    report = verify_consistency()
    
    print("\n--- Schema Consistency Report ---")
    if not report:
        print("Perfect match!")
    else:
        for line in report:
            print(line)
