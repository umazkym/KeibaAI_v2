import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import mean_squared_error

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from src.pipeline_core import setup_logging
    from src.utils.data_utils import load_parquet_data_by_date
    from src.models.model_train import MuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    sys.exit(1)

def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI μモデル評価パイプライン')
    parser.add_argument('--model_dir', type=str, required=True, help='評価対象の学習済みモデルが格納されているディレクトリ')
    parser.add_argument('--start_date', type=str, required=True, help='評価開始日 (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='評価終了日 (YYYY-MM-DD)')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='基本設定ファイルパス')
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='ログレベル')

    args = parser.parse_args()

    # --- 0. 設定とロギングの初期化 ---
    try:
        project_root_path = Path(__file__).resolve().parent.parent.parent
        config_path = project_root_path / args.config
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        data_path = project_root_path / config.get('data_path', 'data')
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.replace('${data_path}', str(data_path))
            if key.endswith('_path') and not Path(config[key]).is_absolute():
                config[key] = str(project_root_path / config[key])

        log_conf = config.get('logging', {})
        now = datetime.now()
        log_file_path = now.strftime(log_conf.get('log_file', 'logs/pipeline.log').replace('{YYYY}', '%Y').replace('{MM}', '%m').replace('{DD}', '%d'))
        Path(log_file_path).parent.mkdir(parents=True, exist_ok=True)
        setup_logging(log_level=args.log_level, log_file=log_file_path, log_format=log_conf.get('format'))
    except Exception as e:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [ERROR] %(message)s')
        logging.error(f"ロギングの初期化に失敗しました: {e}", exc_info=True)
        sys.exit(1)

    logging.info("=" * 60)
    logging.info("Keiba AI μモデル評価パイプライン開始")
    logging.info("=" * 60)
    logging.info(f"評価期間: {args.start_date} - {args.end_date}")
    logging.info(f"評価対象モデル: {args.model_dir}")

    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')

    # --- 1. モデルと特徴量リストのロード ---
    model_path = Path(args.model_dir)
    try:
        estimator = MuEstimator({}) # configはloadで上書きされるので空でOK
        estimator.load_model(model_path)
        feature_names = estimator.feature_names
        logging.info(f"{len(feature_names)}個の特徴量を持つモデルをロードしました")
    except FileNotFoundError:
        logging.error(f"モデルファイルが見つかりません: {model_path}")
        sys.exit(1)

    # --- 2. 評価用データのロードと前処理 ---
    features_path_str = config['features_path']
    parquet_dir = Path(features_path_str) / 'parquet'
    features_df = load_parquet_data_by_date(parquet_dir, start_dt, end_dt, date_col='race_date')
    if features_df.empty:
        logging.error(f"期間 {args.start_date} - {args.end_date} の特徴量データが見つかりません。")
        sys.exit(1)
    
    parsed_data_path = config['parsed_data_path']
    races_parquet_path = Path(parsed_data_path) / 'parquet' / 'races' / 'races.parquet'
    try:
        races_df = pd.read_parquet(races_parquet_path)
    except FileNotFoundError:
        logging.error(f"レース結果ファイルが見つかりません: {races_parquet_path}")
        sys.exit(1)

    # 学習時と全く同じ前処理
    merge_keys = ['race_id', 'horse_id']
    for df in [features_df, races_df]:
        for key in merge_keys:
            if key in df.columns:
                df[key] = df[key].astype(str).str.strip()
    
    if features_df.duplicated(subset=merge_keys).any():
        features_df = features_df.drop_duplicates(subset=merge_keys, keep='first')
    
    target_cols = ['finish_position', 'finish_time_seconds']
    races_subset_df = races_df[merge_keys + target_cols].copy()
    merged_df = pd.merge(features_df, races_subset_df, on=merge_keys, how='inner')
    
    if merged_df.empty:
        logging.error("マージの結果、データが0行になりました。")
        sys.exit(1)

    final_df = merged_df.dropna(subset=target_cols + ['race_id']).copy()
    if final_df.empty:
        logging.error("必須カラムの欠損値を除去した結果、評価データが0行になりました。")
        sys.exit(1)
    
    for col in feature_names:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
    logging.info(f"評価用データの準備完了: {len(final_df)}行")

    # --- 3. 予測の実行 ---
    try:
        predictions = estimator.predict(final_df)
        final_df['predicted_score'] = predictions
        logging.info("予測スコアの計算が完了しました。")
    except Exception as e:
        logging.error(f"予測の実行中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. 評価指標の計算 ---
    logging.info("評価指標の計算を開始します...")
    
    # 4.1 Regressor評価 (RMSE)
    try:
        pred_reg = estimator.model_regressor.predict(final_df[feature_names])
        true_time = final_df['finish_time_seconds']
        rmse = np.sqrt(mean_squared_error(true_time, pred_reg))
        logging.info(f"[Regressor評価] タイム予測RMSE: {rmse:.4f} 秒")
    except Exception as e:
        logging.error(f"Regressor評価中にエラー: {e}")

    # 4.2 Ranker評価 (Spearman's Rank Correlation)
    try:
        correlations = []
        for race_id, group in final_df.groupby('race_id'):
            if len(group) > 1:
                corr, _ = spearmanr(group['predicted_score'], group['finish_position'])
                if not np.isnan(corr):
                    correlations.append(corr)
        
        if correlations:
            avg_corr = np.mean(correlations)
            median_corr = np.median(correlations)
            std_corr = np.std(correlations)
            logging.info(f"[Ranker評価] スピアマン順位相関係数 (予測スコア vs 着順):")
            logging.info(f"  - 平均: {avg_corr:.4f}")
            logging.info(f"  - 中央値: {median_corr:.4f}")
            logging.info(f"  - 標準偏差: {std_corr:.4f}")
            logging.info("  (注: 予測スコアが高いほど良い順位と予測するため、この値が負に大きく振れるほど良いモデルです)")
        else:
            logging.warning("相関係数を計算できるレースがありませんでした。")
    except Exception as e:
        logging.error(f"Ranker評価中にエラー: {e}")


    logging.info("=" * 60)
    logging.info("Keiba AI μモデル評価パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
