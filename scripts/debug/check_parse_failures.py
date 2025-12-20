import sqlite3
import pandas as pd

# データベース接続
conn = sqlite3.connect('keibaai/data/metadata/db.sqlite3')

# テーブル一覧
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]
print("Tables:", tables)

# parse_failuresテーブルを調べる
if 'parse_failures' in tables:
    df = pd.read_sql_query("SELECT parser_name, COUNT(*) as count FROM parse_failures GROUP BY parser_name", conn)
    print("\nパース失敗件数（パーサー別）:")
    print(df)
    
    # shutuba_parserの失敗ファイルを確認
    df_shutuba = pd.read_sql_query("SELECT file_path FROM parse_failures WHERE parser_name='shutuba_parser' LIMIT 10", conn)
    print("\nshutuba_parser失敗ファイル例:")
    for i, row in df_shutuba.iterrows():
        print(f"  {row['file_path']}")

conn.close()
