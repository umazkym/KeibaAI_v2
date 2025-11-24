
import pandas as pd
import argparse

def analyze_parser_anomalies(file_path: str):
    """
    パース結果のCSVを読み込み、メタデータの異常値を分析する。
    """
    try:
        df = pd.read_csv(file_path)
        print("CSVファイルの読み込みに成功しました。")
        
        # --- 1. 距離(distance_m)の異常値チェック ---
        print("\n--- 距離(distance_m)の異常値チェック ---")
        
        # distance_m が数値型であることを確認
        if 'distance_m' in df.columns and pd.api.types.is_numeric_dtype(df['distance_m']):
            # 障害レースを除外せずに、ありえない短い距離を検出
            # 一般的な競馬の最短距離は800m程度なので、それより大幅に短いものは異常とみなす
            anomalous_distance = df[df['distance_m'] < 800]

            if anomalous_distance.empty:
                print("距離が800m未満の異常なデータは見つかりませんでした。")
            else:
                print(f"{len(anomalous_distance.groupby('race_id').head(1))}件のレースで、距離が異常なデータが見つかりました。")
                print("異常値データの例:")
                
                cols_to_show = [
                    'race_id', 'distance_m', 'track_surface', 'weather', 
                    'track_condition', 'race_name', 'venue', 'metadata_source'
                ]
                # 存在しないカラムは除外
                cols_to_show = [col for col in cols_to_show if col in df.columns]

                # race_idごとに最初の1件を表示
                print(anomalous_distance.groupby('race_id').head(1)[cols_to_show].to_string())
        else:
            print("distance_m カラムが存在しないか、数値型ではありません。")

    except Exception as e:
        print(f"分析中にエラーが発生しました: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='パーサーの異常値データ分析スクリリプト')
    parser.add_argument('file_path', type=str, help='分析対象のCSVファイルパス')
    args = parser.parse_args()
    analyze_parser_anomalies(args.file_path)
