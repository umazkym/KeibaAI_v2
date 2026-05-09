
import pandas as pd
import argparse

def get_unique_examples(x):
    """agg用のヘルパー関数。ユニークな値の例を5件まで取得する。"""
    unique_values = x.dropna().unique()
    return unique_values[:5].tolist() if len(unique_values) > 0 else []

def analyze_parser_issues(file_path: str):
    """
    パース結果のCSVを読み込み、メタデータの欠損状況を分析する。
    """
    try:
        df = pd.read_csv(file_path)
        print("CSVファイルの読み込みに成功しました。")
        
        meta_cols = [
            'distance_m', 'track_surface', 'weather', 'track_condition', 
            'race_name', 'venue', 'race_class'
        ]
        meta_cols = [col for col in meta_cols if col in df.columns]

        print("\n--- 1. メタデータ欠損レースの調査 ---")
        
        # 最初の馬のデータ（head(1)）だけ見れば、レース単位での欠損がわかる
        race_df = df.groupby('race_id').head(1)
        
        missing_data_races = race_df[race_df[meta_cols].isnull().any(axis=1)]

        if missing_data_races.empty:
            print("メタデータが完全に欠損しているレースは見つかりませんでした。")
        else:
            print(f"{len(missing_data_races)}件のレースでメタデータの一部または全部が欠損しています。")
            print("欠損レースの例 (先頭5件):")
            # 欠損しているカラムを特定して表示
            for index, row in missing_data_races.head(5).iterrows():
                missing_cols = [col for col in meta_cols if pd.isnull(row[col])]
                print(f"  - Race ID: {row['race_id']}, Source: {row['metadata_source']}, Missing: {missing_cols}")

        print("\n--- 2. metadata_sourceごとの取得状況 ---")
        
        for col in meta_cols:
            print(f"\n▼ カラム: '{col}'")
            # metadata_source ごとに、非nullの値の数とユニークな値の例を表示
            summary = df.groupby('metadata_source')[col].agg(['count', get_unique_examples])
            summary.columns = ['取得件数', '値の例']
            print(summary)
            
    except Exception as e:
        print(f"分析中にエラーが発生しました: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='パーサーの欠損データ分析スクリプト')
    parser.add_argument('file_path', type=str, help='分析対象のCSVファイルパス')
    args = parser.parse_args()
    analyze_parser_issues(args.file_path)
