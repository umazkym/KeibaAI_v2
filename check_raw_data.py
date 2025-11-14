import pathlib
from collections import Counter

def check_raw_data_files():
    """
    keibaai/data/raw/html/ 以下の各サブディレクトリのファイル数と拡張子を調査する
    """
    base_dir = pathlib.Path("keibaai") / "data" / "raw" / "html"
    sub_dirs = ["race", "shutuba", "horse", "ped"]

    print("=" * 50)
    print(f"調査対象ディレクトリ: {base_dir}")
    print("=" * 50)

    if not base_dir.exists():
        print(f"エラー: ベースディレクトリが見つかりません: {base_dir}")
        return

    for sub_dir_name in sub_dirs:
        target_dir = base_dir / sub_dir_name
        print(f"\n--- ディレクトリ: {sub_dir_name} ---")

        if not target_dir.exists():
            print("ディレクトリが存在しません。")
            continue

        try:
            files = [item for item in target_dir.iterdir() if item.is_file()]
            
            if not files:
                print("ファイルが存在しません。")
                continue

            total_files = len(files)
            extension_counts = Counter(file.suffix for file in files)

            print(f"総ファイル数: {total_files}")
            print("拡張子の内訳:")
            for ext, count in extension_counts.items():
                print(f"  - '{ext}': {count}個")
        except Exception as e:
            print(f"ディレクトリのスキャン中にエラーが発生しました: {e}")


if __name__ == "__main__":
    check_raw_data_files()
