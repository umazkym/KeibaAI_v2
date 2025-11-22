"""
μモデルで期間を指定して一括予測するスクリプト

σ/νモデルの学習に必要な、訓練データ全体に対するμの予測結果を生成します。

Usage:
    python predict_bulk.py \
        --start_date 2020-01-01 \
        --end_date 2023-12-31 \
        --model_dir keibaai/data/models/mu_model_20241122 \
        --output_path keibaai/data/predictions/parquet/mu_predictions.parquet
"""

import argparse
import logging
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import joblib

# プロジェクトルートをPYTHONPATHに追加
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root / 'keibaai' / 'src'))
sys.path.insert(0, str(project_root / 'keibaai'))  # keibaaiディレクトリも追加

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def load_mu_model(model_dir: Path):
    """μモデルを読み込む"""
    model_path = model_dir / "mu_model.pkl"

    if not model_path.exists():
        raise FileNotFoundError(f"μモデルが見つかりません: {model_path}")

    logging.info(f"μモデルをロード中: {model_path}")
    model = joblib.load(model_path)

    return model

def load_features_for_period(start_date: str, end_date: str):
    """期間を指定して特徴量データを読み込む"""
    features_base_path = Path("keibaai/data/features/parquet")

    if not features_base_path.exists():
        raise FileNotFoundError(f"特徴量データが見つかりません: {features_base_path}")

    logging.info(f"特徴量データをロード中: {start_date} 〜 {end_date}")

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    # 年・月ごとにparquetファイルを個別に読み込み（YAMLファイルを除外）
    all_dfs = []

    for year in range(start_dt.year, end_dt.year + 1):
        year_path = features_base_path / f"year={year}"
        if not year_path.exists():
            continue

        for month in range(1, 13):
            month_path = year_path / f"month={month}"
            if not month_path.exists():
                continue

            # .parquetファイルのみを読み込み
            parquet_files = list(month_path.glob("*.parquet"))
            for pq_file in parquet_files:
                df_part = pd.read_parquet(pq_file)
                all_dfs.append(df_part)

    if not all_dfs:
        raise RuntimeError(f"期間 {start_date} - {end_date} の特徴量データが見つかりません")

    # 全データを結合
    df = pd.concat(all_dfs, ignore_index=True)

    # race_dateでフィルタリング
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df[(df['race_date'] >= start_dt) & (df['race_date'] <= end_dt)]

    logging.info(f"ロード完了: {len(df):,}行")

    return df

def predict_mu(model, features_df: pd.DataFrame):
    """μモデルで予測"""

    # 識別子カラムを保存（horse_numberは特徴量としても使用）
    id_cols = ['race_id', 'horse_id']
    ids_df = features_df[id_cols].copy()

    # horse_numberを結果用に保存（特徴量からは除外しない）
    if 'horse_number' in features_df.columns:
        ids_df['horse_number'] = features_df['horse_number']

    # 特徴量カラムを選択（数値型のみ）
    # race_id, horse_id は除外するが、horse_numberは含める
    feature_cols = [c for c in features_df.columns
                   if c not in ['race_id', 'horse_id', 'race_date', 'race_date_str', 'year', 'month', 'day']]

    # 数値型でないカラムを除外
    numeric_feature_cols = []
    for col in feature_cols:
        if pd.api.types.is_numeric_dtype(features_df[col]):
            numeric_feature_cols.append(col)

    logging.info(f"予測に使用する特徴量: {len(numeric_feature_cols)}個")

    X = features_df[numeric_feature_cols]

    # 欠損値を0で埋める（必要に応じて調整）
    X = X.fillna(0)

    logging.info("μ予測を実行中...")
    mu_predictions = model.predict(X)

    # 結果をDataFrameにまとめる
    result_df = ids_df.copy()
    result_df['mu'] = mu_predictions

    return result_df

def main():
    parser = argparse.ArgumentParser(description='μモデル一括予測スクリプト')
    parser.add_argument('--start_date', required=True, help='開始日 (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='終了日 (YYYY-MM-DD)')
    parser.add_argument('--model_dir', required=True, help='μモデルディレクトリ')
    parser.add_argument('--output_path', required=True, help='出力Parquetパス')

    args = parser.parse_args()

    try:
        logging.info("=" * 70)
        logging.info("μモデル一括予測スクリプト開始")
        logging.info("=" * 70)
        logging.info(f"期間: {args.start_date} 〜 {args.end_date}")
        logging.info(f"モデル: {args.model_dir}")
        logging.info(f"出力: {args.output_path}")
        logging.info("")

        # 1. μモデルのロード
        model_dir = Path(args.model_dir)
        model = load_mu_model(model_dir)

        # 2. 特徴量データのロード
        features_df = load_features_for_period(args.start_date, args.end_date)

        if features_df.empty:
            logging.error("特徴量データが空です")
            sys.exit(1)

        # 3. 予測実行
        predictions_df = predict_mu(model, features_df)

        logging.info(f"予測完了: {len(predictions_df):,}行")

        # 4. 保存
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        predictions_df.to_parquet(output_path, index=False)
        logging.info(f"予測結果を保存しました: {output_path}")

        # 5. サマリー表示
        logging.info("")
        logging.info("=" * 70)
        logging.info("予測サマリー")
        logging.info("=" * 70)
        logging.info(f"総予測数: {len(predictions_df):,}行")
        logging.info(f"μ統計:")
        logging.info(f"  平均: {predictions_df['mu'].mean():.4f}")
        logging.info(f"  中央値: {predictions_df['mu'].median():.4f}")
        logging.info(f"  標準偏差: {predictions_df['mu'].std():.4f}")
        logging.info(f"  範囲: {predictions_df['mu'].min():.4f} 〜 {predictions_df['mu'].max():.4f}")
        logging.info("")
        logging.info("✅ μモデル一括予測完了")
        logging.info("=" * 70)

    except Exception as e:
        logging.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
