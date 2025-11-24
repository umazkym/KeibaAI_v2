# -*- coding: utf-8 -*-
"""
実際のParquetデータから feature_names.yaml を生成するスクリプト
"""
import pandas as pd
import yaml
from pathlib import Path

def main():
    project_root = Path(__file__).resolve().parent

    # 2020年1月のデータから特徴量リストを取得
    parquet_path = project_root / 'keibaai/data/features/parquet/year=2020/month=1'
    df = pd.read_parquet(parquet_path)

    # 除外するカラム（特徴量ではないもの）
    exclude_cols = {
        'race_id', 'horse_id', 'race_date',
        'finish_position', 'finish_time_seconds',  # ターゲット変数
        'year', 'month', 'day'  # パーティションキー
    }

    # 特徴量リストを作成
    feature_names = [col for col in df.columns if col not in exclude_cols]
    feature_names.sort()  # アルファベット順にソート

    print(f"抽出した特徴量数: {len(feature_names)}")
    print(f"\nJockey関連特徴量:")
    jockey_features = [f for f in feature_names if 'jockey' in f and 'win_rate' in f]
    for f in jockey_features:
        print(f"  - {f}")

    print(f"\nTrainer関連特徴量:")
    trainer_features = [f for f in feature_names if 'trainer' in f and 'win_rate' in f]
    for f in trainer_features:
        print(f"  - {f}")

    # parquet/feature_names.yaml に保存
    output_path = project_root / 'keibaai/data/features/parquet/feature_names.yaml'
    with open(output_path, 'w', encoding='utf-8') as f:
        yaml.dump(feature_names, f, allow_unicode=True, default_flow_style=False)

    print(f"\n✅ feature_names.yaml を保存しました: {output_path}")
    print(f"   特徴量数: {len(feature_names)}")

if __name__ == '__main__':
    main()
