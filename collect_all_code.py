import os

# 対象とするファイルの拡張子
CODE_EXTENSIONS = {
    '.py', '.sh', '.yaml', '.yml',
    'Dockerfile', # 拡張子がないファイル名も直接指定
}

# 除外するディレクトリ名 (これらの名前を持つディレクトリは完全にスキップされます)
EXCLUDE_DIRS = {
    '.git', '__pycache__', '.claude', '.venv', 'node_modules', 
    # dataとlogsはルートレベルでのみ除外したい場合があるため、
    # 下のロジックで特別扱いします。
}

# 除外する特定のパスのプレフィックス
# これらのプレフィックスで始まるパスは無視されます。
EXCLUDE_PATHS_PREFIX = {
    os.path.join('.', 'data'),
    os.path.join('.', 'keibaai', 'data'),
    os.path.join('.', 'keibaai', 'docs'),
    os.path.join('.', 'keibaai', 'notebooks'),
}


# 除外するファイル名
EXCLUDE_FILES = {
    'collect_all_code.py', # このスクリプト自体
    'all_code.txt',      # 出力ファイル
    'エラー内容.txt',
    'gemini.md',
    'claude.md',
    'settings.local.json',
    'db.sqlite3',
}

# 出力ファイル名
OUTPUT_FILE = 'all_code.txt'

def should_exclude_path(path):
    """指定されたパスが除外対象のプレフィックスで始まるかチェックする"""
    for prefix in EXCLUDE_PATHS_PREFIX:
        if path.startswith(prefix):
            return True
    return False

def collect_code():
    """
    プロジェクト内のコードを一つのファイルにまとめる
    """
    print(f"Collecting code into {OUTPUT_FILE}...")
    count = 0
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        # os.walkでカレントディレクトリから再帰的に探索
        for root, dirs, files in os.walk('.'):
            # 正規化されたルートパス
            normalized_root = os.path.normpath(root)

            # パスプレフィックスに基づいてディレクトリを枝刈り
            if should_exclude_path(normalized_root):
                dirs[:] = [] # このディレクトリ以下の探索を中止
                continue

            # 除外ディレクトリ名を探索対象から外す
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for file in files:
                # 除外ファイルをスキップ
                if file in EXCLUDE_FILES:
                    continue

                file_path = os.path.join(normalized_root, file)

                # パスプレフィックスに基づいてファイルをスキップ
                if should_exclude_path(file_path):
                    continue
                
                # 対象の拡張子か、ファイル名が直接指定されているかチェック
                _, ext = os.path.splitext(file)
                if ext in CODE_EXTENSIONS or file in CODE_EXTENSIONS:
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
                            content = infile.read()
                            outfile.write(f'--- {file_path.replace(os.sep, "/")} ---\n\n')
                            outfile.write(content)
                            outfile.write('\n\n')
                        print(f"  Collected: {file_path}")
                        count += 1
                    except Exception as e:
                        print(f"  Error reading {file_path}: {e}")

    print(f"\nSuccessfully collected {count} files into {OUTPUT_FILE}")

if __name__ == '__main__':
    collect_code()
