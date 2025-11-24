#!/usr/bin/env python3
"""
移動したスクリプトのimport修正スクリプト
"""
import re
from pathlib import Path

def fix_imports(file_path: Path, depth: int):
    """
    移動したスクリプトのimportを修正
    
    Args:
        file_path: スクリプトのパス
        depth: project rootまでの深さ (scripts/training/ なら 2)
    """
    print(f"修正中: {file_path}")
    
    content = file_path.read_text(encoding='utf-8')
    original = content
    
    # すでにsys.pathが設定されている場合はスキップ
    if 'sys.path.append' in content and 'project_root' in content:
        print(f"  → スキップ (すでに修正済み)")
        return
    
    # import文の前にsys.path設定を追加
    parent_chain = '.parent' * depth
    sys_path_block = f'''import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve(){parent_chain}
sys.path.append(str(project_root))

'''
    
    # 最初のimportの前に挿入
    lines = content.split('\n')
    insert_index = 0
    
    # shebangやdocstringをスキップ
    in_docstring = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # docstringの処理
        if stripped.startswith('"""') or stripped.startswith("'''"):
            if in_docstring:
                in_docstring = False
            else:
                in_docstring = True
            continue
        
        if in_docstring or stripped.startswith('#') or not stripped:
            continue
        
        # 最初の実質的なコード行
        if stripped.startswith('import ') or stripped.startswith('from '):
            insert_index = i
            break
    
    # sys.path設定を挿入
    lines.insert(insert_index, sys_path_block.rstrip())
    content = '\n'.join(lines)
    
    # 古いimportパターンを修正（念のため）
    # 例: from src import ... → from keibaai.src import ...
    content = re.sub(r'^from src import', 'from keibaai.src import', content, flags=re.MULTILINE)
    content = re.sub(r'^from src\.', 'from keibaai.src.', content, flags=re.MULTILINE)
    
    if content != original:
        file_path.write_text(content, encoding='utf-8')
        print(f"  → 修正完了")
    else:
        print(f"  → 変更なし")

def main():
    """メイン処理"""
    print("="*60)
    print("import修正スクリプト")
    print("="*60)
    
    # 修正対象のディレクトリとその深さ
    targets = [
        ("scripts/features", 2),
        ("scripts/training", 2),
        ("scripts/training/experimental", 3),
        ("scripts/optimization", 2),
        ("scripts/simulation", 2),
    ]
    
    for dir_path, depth in targets:
        dir_obj = Path(dir_path)
        if not dir_obj.exists():
            print(f"\nスキップ: {dir_path} (存在しない)")
            continue
        
        print(f"\n{dir_path}/ 内のファイルを修正:")
        
        for py_file in dir_obj.glob("*.py"):
            if py_file.name == "__init__.py":
                continue
            fix_imports(py_file, depth)
    
    print("\n" + "="*60)
    print("修正完了")
    print("="*60)

if __name__ == "__main__":
    main()
