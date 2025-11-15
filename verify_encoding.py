
import os
import glob

def find_first_bin_file(directory):
    """指定されたディレクトリで最初の.binファイルを再帰的に見つける"""
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".bin"):
                return os.path.join(root, file)
    return None

def verify_encoding(file_path):
    """
    指定されたファイルのエンコーディングを検証する
    """
    if not file_path or not os.path.exists(file_path):
        print(f"ファイルが見つかりません: {file_path}")
        return

    print(f"--- エンコーディング検証開始: {file_path} ---\\n")

    with open(file_path, 'rb') as f:
        raw_data = f.read()

    encodings_to_try = ['shift_jis', 'euc_jp', 'utf-8']

    for enc in encodings_to_try:
        print(f">>> 試行中: {enc}")
        try:
            # デコードを試みる
            decoded_text = raw_data.decode(enc)
            print(f"  [成功] デコードに成功しました。")
            # デコード後のテキストの先頭150文字を表示
            print(f"  [内容] {decoded_text[:150].replace('\n', ' ')}")
        except UnicodeDecodeError as e:
            print(f"  [失敗] デコードに失敗しました: {e}")
        except Exception as e:
            print(f"  [エラー] 予期せぬエラー: {e}")
        print("-" * 20)

def main():
    """
    メイン処理
    """
    base_dir = "keibaai/data/raw/html/race"
    
    # 最初の.binファイルを探す
    bin_file = find_first_bin_file(base_dir)
    
    if bin_file:
        verify_encoding(bin_file)
    else:
        print(f"{base_dir} 内に .bin ファイルが見つかりませんでした。")

if __name__ == "__main__":
    main()
