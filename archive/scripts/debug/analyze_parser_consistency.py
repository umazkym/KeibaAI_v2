import pandas as pd
import argparse

def analyze_consistency(file_path: str):
    """
    パース結果のCSVを読み込み、レースごとのメタデータの一貫性を分析する。
    """
    try:
        # 1. CSVを読み込む
        df = pd.read_csv(file_path)
        print("CSVファイルの読み込みに成功しました。")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")

        # 2. 分析対象のメタデータカラム
        meta_cols = [
            'distance_m', 'track_surface', 'weather', 'track_condition', 
            'race_name', 'venue', 'race_class'
        ]
        
        # 存在しないカラムは除外
        meta_cols = [col for col in meta_cols if col in df.columns]
        if not meta_cols:
            print("エラー: 分析対象のメタデータカラムが見つかりません。")
            return

        print(f"\n分析対象カラム: {meta_cols}")

        # 3. race_idでグループ化し、メタデータの揺れをチェック
        inconsistent_races = []
        
        # race_id ごとにループ
        for race_id, group in df.groupby('race_id'):
            # 各メタデータカラムについて、グループ内のユニークな値の数を数える
            nunique_counts = group[meta_cols].nunique()
            
            # ユニークな値が2つ以上あるカラム（＝値が揺れているカラム）があるかチェック
            if (nunique_counts > 1).any():
                inconsistent_races.append({
                    'race_id': race_id,
                    'details': group[['race_id', 'metadata_source'] + meta_cols]
                })

        # 4. 結果の表示
        if not inconsistent_races:
            print("\n分析結果: 全てのレースでメタデータは一貫していました。")
            return
            
        print(f"\n分析結果: {len(inconsistent_races)}件のレースでメタデータの不整合が検出されました。")
        print("-" * 50)

        for race_info in inconsistent_races:
            print(f"\n▼ Race ID: {race_info['race_id']}")
            print("詳細:")
            print(race_info['details'].to_string())
            print("-" * 50)

    except Exception as e:
        print(f"分析中にエラーが発生しました: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='パーサーの一貫性分析スクリプト')
    parser.add_argument('file_path', type=str, help='分析対象のCSVファイルパス')
    args = parser.parse_args()
    analyze_consistency(args.file_path)
