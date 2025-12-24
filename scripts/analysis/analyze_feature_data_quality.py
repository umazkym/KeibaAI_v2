#!/usr/bin/env python3
# analyze_feature_data_quality.py
# μモデルの特徴量データの品質を詳細に分析するスクリプト

import sys
import logging
import pandas as pd
import numpy as np
from pathlib import Path
import json
import matplotlib.pyplot as plt
import seaborn as sns

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('feature_analysis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("="*80)
    logger.info("μモデル 特徴量データ詳細分析 (Deep Analysis)")
    logger.info("="*80)

    # 1. 特徴量リストのロード
    feature_names_path = Path('models/mu_v2.1_new_features/feature_names.json')
    if not feature_names_path.exists():
        logger.error(f"特徴量リストが見つかりません: {feature_names_path}")
        return

    with open(feature_names_path, 'r', encoding='utf-8') as f:
        feature_names = json.load(f)
    
    logger.info(f"定義済み特徴量数: {len(feature_names)}")

    # 2. データのロード (2023年と2024年のデータを対象に分析)
    # 最新のデータ状況を確認するため
    features_dir = Path('keibaai/data/features/parquet')
    years = [2023, 2024]
    dfs = []
    
    for year in years:
        year_path = features_dir / f'year={year}'
        if year_path.exists():
            logger.info(f"Loading {year} data from {year_path}...")
            try:
                df_year = pd.read_parquet(year_path)
                dfs.append(df_year)
            except Exception as e:
                logger.error(f"Failed to load {year} data: {e}")
    
    if not dfs:
        logger.error("データがロードできませんでした。")
        return

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"分析対象データ総数: {len(df):,} 行")

    # 3. 特徴量の存在確認
    missing_columns = [col for col in feature_names if col not in df.columns]
    if missing_columns:
        logger.error(f"⚠️ データフレームに存在しない特徴量があります ({len(missing_columns)}個):")
        for col in missing_columns:
            logger.error(f"  - {col}")
    else:
        logger.info("✅ 全ての特徴量がデータフレーム内に存在します。")

    # 4. 詳細分析
    analysis_results = []
    
    # 分析対象のカラム（存在するもののみ）
    target_cols = [col for col in feature_names if col in df.columns]
    
    logger.info("各特徴量の詳細分析を実行中...")
    
    for col in target_cols:
        series = df[col]
        
        # 数値変換（object型の場合）
        if series.dtype == 'object':
            series_numeric = pd.to_numeric(series, errors='coerce')
        else:
            series_numeric = series

        # 統計量計算
        total_count = len(series)
        null_count = series.isna().sum()
        null_rate = null_count / total_count
        
        stats = {
            'feature_name': col,
            'dtype': str(series.dtype),
            'null_rate': null_rate,
            'unique_count': series.nunique(),
            'zero_rate': (series_numeric == 0).mean() if pd.api.types.is_numeric_dtype(series_numeric) else 0,
        }
        
        if pd.api.types.is_numeric_dtype(series_numeric):
            stats.update({
                'mean': series_numeric.mean(),
                'std': series_numeric.std(),
                'min': series_numeric.min(),
                'max': series_numeric.max(),
                'median': series_numeric.median()
            })
        else:
            stats.update({
                'mean': None, 'std': None, 'min': None, 'max': None, 'median': None
            })
            
        analysis_results.append(stats)

    # 結果をDataFrame化
    analysis_df = pd.DataFrame(analysis_results)
    
    # 5. 問題のある特徴量の抽出
    
    # A. 欠損率が高い特徴量 (20%以上)
    high_null_features = analysis_df[analysis_df['null_rate'] > 0.2].sort_values('null_rate', ascending=False)
    
    # B. 値が全て同じ特徴量 (分散0)
    zero_variance_features = analysis_df[analysis_df['std'] == 0]
    
    # C. ほとんどが0の特徴量 (Zero Rate > 90%)
    high_zero_features = analysis_df[analysis_df['zero_rate'] > 0.9]

    # 6. レポート出力 (Markdown)
    report_path = Path('docs/feature_quality_report.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# μモデル 特徴量データ品質分析レポート\n\n")
        f.write(f"**分析実施日**: {pd.Timestamp.now().strftime('%Y-%m-%d')}\n")
        f.write(f"**対象データ**: {years}年 (計 {len(df):,} 行)\n\n")
        
        f.write("## 1. 概要\n")
        f.write(f"- 定義済み特徴量数: {len(feature_names)}\n")
        f.write(f"- データ内に存在する特徴量数: {len(target_cols)}\n")
        f.write(f"- 欠損あり特徴量数: {len(analysis_df[analysis_df['null_rate'] > 0])}\n\n")
        
        f.write("## 2. ⚠️ 注意が必要な特徴量\n\n")
        
        f.write("### 2.1 欠損率が高い特徴量 (Null Rate > 20%)\n")
        if not high_null_features.empty:
            f.write("| 特徴量名 | 欠損率 | データ型 | 説明(推定) |\n")
            f.write("|---|---|---|---|\n")
            for _, row in high_null_features.iterrows():
                f.write(f"| `{row['feature_name']}` | {row['null_rate']:.2%} | {row['dtype']} | |\n")
        else:
            f.write("該当なし\n")
        f.write("\n")
        
        f.write("### 2.2 分散が0の特徴量 (値が変化しない)\n")
        if not zero_variance_features.empty:
            f.write("| 特徴量名 | 値 |\n")
            f.write("|---|---|\n")
            for _, row in zero_variance_features.iterrows():
                f.write(f"| `{row['feature_name']}` | {row['mean']} |\n")
        else:
            f.write("該当なし\n")
        f.write("\n")

        f.write("### 2.3 ほとんどが0の特徴量 (Zero Rate > 90%)\n")
        if not high_zero_features.empty:
            f.write("| 特徴量名 | 0の割合 | 平均値 | 最大値 |\n")
            f.write("|---|---|---|---|\n")
            for _, row in high_zero_features.iterrows():
                f.write(f"| `{row['feature_name']}` | {row['zero_rate']:.2%} | {row['mean']:.4f} | {row['max']} |\n")
        else:
            f.write("該当なし\n")
        f.write("\n")
        
        f.write("## 3. カテゴリ別詳細統計\n\n")
        
        # カテゴリ推定（名前から）
        categories = {
            'Basic': ['horse_number', 'bracket', 'age', 'weight', 'sex'],
            'Jockey/Trainer': ['jockey', 'trainer', 'combo'],
            'Past Perf': ['past_', 'avg_finish', 'win_rate'],
            'Course': ['venue', 'dist', 'surface'],
            'Pace': ['pace', 'speed', 'nige', 'senko'],
            'Bloodline': ['sire', 'nicks'],
            'Change': ['change', 'is_'],
        }
        
        for cat, keywords in categories.items():
            f.write(f"### {cat} Features\n")
            # キーワードにマッチする特徴量を抽出
            cat_features = analysis_df[analysis_df['feature_name'].apply(lambda x: any(k in x for k in keywords))]
            
            if not cat_features.empty:
                f.write("| 特徴量名 | 欠損率 | 平均 | Std | Min | Max |\n")
                f.write("|---|---|---|---|---|---|\n")
                for _, row in cat_features.iterrows():
                    f.write(f"| `{row['feature_name']}` | {row['null_rate']:.1%} | {row['mean']:.4f} | {row['std']:.4f} | {row['min']:.2f} | {row['max']:.2f} |\n")
            else:
                f.write("該当なし\n")
            f.write("\n")

    logger.info(f"レポートを作成しました: {report_path}")
    
    # CSV出力も行う
    csv_path = Path('docs/feature_quality_stats.csv')
    analysis_df.to_csv(csv_path, index=False)
    logger.info(f"詳細データを保存しました: {csv_path}")

if __name__ == '__main__':
    main()
