import pandas as pd
import os
import sys
import io

# Windowsのコンソール出力エンコーディング設定
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# pandasの表示オプションを設定して、すべてのカラムを表示
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

def inspect_parquet_file(file_path, output_writer):
    """
    単一のParquetファイルを読み込み、先頭100行の内容を出力する
    """
    output_writer.write("=" * 80 + "\n")
    output_writer.write(f"ファイル: {file_path}\n")
    output_writer.write("=" * 80 + "\n\n")

    if not os.path.exists(file_path):
        output_writer.write("--- ファイルが見つかりません ---\n\n")
        return

    try:
        df = pd.read_parquet(file_path)
        
        if df.empty:
            output_writer.write("--- ファイルは空です ---\n\n")
            return

        output_writer.write(f"### 全体像 (総行数: {len(df)}, 総カラム数: {len(df.columns)}) ###\n")
        
        # df.info() の出力を文字列としてキャプチャ
        buffer = io.StringIO()
        df.info(buf=buffer)
        info_str = buffer.getvalue()
        output_writer.write(info_str)

        output_writer.write("\n\n")

        output_writer.write("### 先頭100行のデータ ###\n")
        output_writer.write(df.head(100).to_string())
        output_writer.write("\n\n")

    except Exception as e:
        output_writer.write(f"--- ファイルの読み込みまたは処理中にエラーが発生しました: {e} ---\n\n")


def main():
    """
    主要なParquetファイルの内容を検査し、1つのファイルに出力する
    """
    base_path = "keibaai/data/parsed/parquet"
    output_filename = "parquet_inspection_report.txt"
    
    files_to_inspect = {
        "races": os.path.join(base_path, "races", "races.parquet"),
        "shutuba": os.path.join(base_path, "shutuba", "shutuba.parquet"),
        "pedigrees": os.path.join(base_path, "pedigrees", "pedigrees.parquet"),
        "horses": os.path.join(base_path, "horses", "horses.parquet"),
    }
    
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write("Parquet データ内容検査レポート\n")
        f.write("=" * 80 + "\n\n")
        
        for name, path in files_to_inspect.items():
            inspect_parquet_file(path, f)

    print(f"検査結果を {output_filename} に書き込みました。")


if __name__ == "__main__":
    main()
