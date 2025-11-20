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
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

try:
    from keibaai.src.pipeline_core import setup_logging
    from keibaai.src.utils.data_utils import load_parquet_data_by_date
    from keibaai.src.modules.models.model_train import MuEstimator
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
    # --- 1. モデルと特徴量リストのロード ---
    model_path = Path(args.model_dir) / 'mu_model.pkl'
    try:
        import joblib
        estimator = joblib.load(model_path)
        feature_names = estimator.feature_names_
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
    
    # features_dfにターゲットが含まれていることを前提とする
    # train_full_pipeline.pyではfeatures_dfから直接学習しているため、ここでも同様にする
    
    target_cols = ['finish_position', 'finish_time_seconds']
    missing_targets = [col for col in target_cols if col not in features_df.columns]
    
    if missing_targets:
        logging.warning(f"特徴量データにターゲット列が含まれていません: {missing_targets}")
        # もし含まれていない場合は、race_entries.parquetから結合する必要があるが、
        # 現状のパイプラインではfeatures.parquetに含まれているはず。
        # 万が一のため、race_entriesからの結合ロジックを入れることも検討できるが、
        # まずはエラーにする。
        sys.exit(1)

    final_df = features_df.dropna(subset=target_cols + ['race_id']).copy()
    if final_df.empty:
        logging.error("必須カラムの欠損値を除去した結果、評価データが0行になりました。")
        sys.exit(1)
    
    for col in feature_names:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
    logging.info(f"評価用データの準備完了: {len(final_df)}行")

    # --- 3. 予測の実行 ---
    try:
        # モデルが使用する特徴量のみを渡す
        X_eval = final_df[feature_names]
        
        logging.info(f"予測入力データ形状: {X_eval.shape}")
        logging.info(f"モデル期待特徴量数 (Ranker): {estimator.ranker.n_features_}")
        if hasattr(estimator.regressor, 'n_features_'):
             logging.info(f"モデル期待特徴量数 (Regressor): {estimator.regressor.n_features_}")
        
        predictions = estimator.predict(X_eval)
        final_df['predicted_score'] = predictions
        logging.info("予測スコアの計算が完了しました。")
    except Exception as e:
        logging.error(f"予測の実行中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. 評価指標の計算と保存 ---
    logging.info("評価指標の計算を開始します...")
    
    daily_metrics = []
    
    # 日付ごとに集計
    for date, date_df in final_df.groupby('race_date'):
        metrics = {'date': date}
        
        # 4.1 RMSE (Regressor)
        if estimator.regressor is not None:
            # pred_reg = estimator.regressor.predict(date_df[feature_names]) # これはエラーになる（rank_scoreがないため）
            # 既に計算済みの predicted_score を使用する
            pred_reg = date_df['predicted_score']
            true_time = date_df['finish_time_seconds']
            metrics['rmse'] = np.sqrt(mean_squared_error(true_time, pred_reg))
        
        # 4.2 Spearman Correlation, Hit Rate, ROI
        correlations = []
        hits = 0
        races_count = 0
        total_bet = 0
        total_return = 0
        
        for race_id, race_df in date_df.groupby('race_id'):
            if len(race_df) > 1:
                # 相関係数
                pred_score = race_df['predicted_score']
                true_rank = race_df['finish_position']
                corr, _ = spearmanr(pred_score, true_rank)
                if not np.isnan(corr):
                    correlations.append(corr)
                
                # AI本命馬を特定 (予測タイムが最小 = 最速予想)
                best_horse_idx = race_df['predicted_score'].idxmin()
                best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                
                # Hit Rate (3着以内に入ったか)
                if best_horse_rank <= 3:
                    hits += 1
                
                # ROI計算 (単勝100円ベット想定)
                bet_amount = 100
                total_bet += bet_amount
                
                # 勝利した場合のみ払い戻し
                if best_horse_rank == 1:
                    # relative_oddsがあれば使用、なければ推定オッズ
                    if 'relative_odds' in race_df.columns:
                        odds = race_df.loc[best_horse_idx, 'relative_odds']
                        # relative_oddsは倍率なので、100円 × oddsが払い戻し
                        # ただし、実際のオッズデータがない場合はデフォルト値を使う
                        if pd.notna(odds) and odds > 0:
                            payout = bet_amount * odds
                        else:
                            # オッズデータがない場合は平均的なオッズ（5倍）を仮定
                            payout = bet_amount * 5.0
                    else:
                        # オッズ列がない場合は平均的なオッズを仮定
                        payout = bet_amount * 5.0
                    
                    total_return += payout
                
                races_count += 1
        
        metrics['spearman_corr'] = np.mean(correlations) if correlations else 0
        metrics['hit_rate'] = hits / races_count if races_count > 0 else 0
        metrics['race_count'] = races_count
        
        # 回収率 (Recovery Rate) = 払戻金 / 購入金額
        metrics['recovery_rate'] = total_return / total_bet if total_bet > 0 else 0
        metrics['total_bet'] = total_bet
        metrics['total_return'] = total_return
        
        daily_metrics.append(metrics)
        
    # CSVに保存
    eval_df = pd.DataFrame(daily_metrics)
    output_dir = Path(config.get('data_path', 'data')) / 'evaluation'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'evaluation_results.csv'
    
    eval_df.to_csv(output_path, index=False)
    logging.info(f"評価結果を保存しました: {output_path}")
    
    # 全体平均のログ出力
    logging.info(f"全体平均 RMSE: {eval_df['rmse'].mean():.4f}")
    logging.info(f"全体平均 Spearman Correlation: {eval_df['spearman_corr'].mean():.4f}")
    logging.info(f"全体平均 Hit Rate: {eval_df['hit_rate'].mean():.2%}")
    logging.info(f"全体平均 回収率 (Recovery Rate): {eval_df['recovery_rate'].mean():.2%}")


    logging.info("=" * 60)
    logging.info("Keiba AI μモデル評価パイプライン完了")
    logging.info("=" * 60)

if __name__ == '__main__':
    main()
