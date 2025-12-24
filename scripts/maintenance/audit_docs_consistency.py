
import os
import re
import ast
import glob
from pathlib import Path
from typing import Set, Dict, List

# プロジェクトルート
PROJECT_ROOT = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2")
DOCS_DIR = PROJECT_ROOT / "docs" / "system"
SRC_DIR = PROJECT_ROOT / "keibaai" / "src"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

def get_all_python_definitions(root_dir: Path) -> Dict[str, Set[str]]:
    """Pythonコードからクラス・関数定義を抽出"""
    definitions = {'classes': set(), 'functions': set(), 'files': set()}
    
    for py_file in root_dir.rglob("*.py"):
        rel_path = py_file.relative_to(PROJECT_ROOT).as_posix()
        definitions['files'].add(rel_path)
        
        try:
            tree = ast.parse(py_file.read_text(encoding='utf-8', errors='ignore'))
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    definitions['classes'].add(node.name)
                elif isinstance(node, ast.FunctionDef):
                    definitions['functions'].add(node.name)
        except Exception as e:
            pass
            
    return definitions

def check_docs_consistency(definitions: Dict[str, Set[str]]):
    """ドキュメント内の記述を検証"""
    
    # 検出パターン
    patterns = {
        'class': re.compile(r'class\s+(\w+)'),
        'func': re.compile(r'def\s+(\w+)\('),
        'file_py': re.compile(r'[\w/]+\.py'),
        'feature': re.compile(r'`(\w+)`')  # バッククォートで囲まれた単語（特徴量候補）
    }
    
    report = []
    
    for doc_file in DOCS_DIR.rglob("*.md"):
        content = doc_file.read_text(encoding='utf-8', errors='ignore')
        
        # 1. ファイルパスの検証
        file_matches = patterns['file_py'].findall(content)
        for fpath in file_matches:
            # 一般的な除外
            if fpath.startswith(('test', 'example', 'my_script', 'check_parsed_data')):
                continue
            
            # パス補完して存在確認
            # ドキュメント内では "scripts/..." や "keibaai/src/..." と書かれることが多い
            # 単にファイル名だけの場合もあるので、definitions['files']の末尾一致で簡易チェック
            
            found = False
            if (PROJECT_ROOT / fpath).exists():
                found = True
            else:
                # ファイル名部分だけで検索
                fname = fpath.split('/')[-1]
                for existing in definitions['files']:
                    if existing.endswith(fname):
                        found = True
                        break
            
            if not found and 'modules' not in fpath: # src/modulesは既に修正済みだが念のため
                report.append(f"[FILE_MISSING] {doc_file.name}: {fpath}")

        # 2. クラス名の検証 (「class Xxx」という記述がある場合)
        class_matches = patterns['class'].findall(content)
        for cls in class_matches:
            if cls not in definitions['classes']:
                 report.append(f"[CLASS_MISSING] {doc_file.name}: {cls}")

    return report

if __name__ == "__main__":
    print("Collecting code definitions...")
    defs = get_all_python_definitions(PROJECT_ROOT)
    print(f"Found {len(defs['classes'])} classes, {len(defs['functions'])} functions, {len(defs['files'])} files.")
    
    print("Checking docs consistency...")
    report = check_docs_consistency(defs)
    
    print("\n--- Consistency Report ---")
    for line in report:
        print(line)
    
    if not report:
        print("No obvious inconsistencies found!")
