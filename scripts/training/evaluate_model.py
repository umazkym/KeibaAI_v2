# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / 'keibaai'))

import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from scipy.stats import spearmanr
from sklearn.metrics import mean_squared_error
import yaml

try:
    from keibaai.src.pipeline_core import setup_logging
    from keibaai.src.utils.data_utils import load_parquet_data_by_date
    from keibaai.src.models.model_train import MuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print(f"sys.path: {sys.path}")
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
    model_dir_path = Path(args.model_dir)
    
    try:
        import joblib
        import json
        
        # MuEstimatorのインスタンスを作成（config不要）してload_modelを呼び出す
        # 最小限のconfigで初期化
        minimal_config = {}
        estimator = MuEstimator(config=minimal_config)
        estimator.load_model(str(model_dir_path))
        
        feature_names = estimator.feature_names
        logging.info(f"{len(feature_names)}個の特徴量を持つμEstimatorをロードしました")
        
        # ★ 確率較正器のロード (Phase E-1) ★
        calibrator = None
        calibrator_path = model_dir_path / 'calibrator.pkl'
        if calibrator_path.exists():
            try:
                from keibaai.src.models.calibration import IsotonicCalibrator
                calibrator = IsotonicCalibrator.load(str(calibrator_path))
                logging.info(f"✓ 確率較正器をロードしました: {calibrator}")
            except Exception as e:
                logging.warning(f"確率較正器のロードに失敗: {e}")
                calibrator = None
        else:
            logging.info("確率較正器が見つかりません。未較正の予測を使用します。")
        
    except FileNotFoundError as e:
        logging.error(f"モデルファイルが見つかりません: {e}")
        sys.exit(1)
    # --- 2. 評価用データのロードと前処理 ---
    features_path_str = config['features_path']
    parquet_dir = Path(features_path_str) / 'parquet'
    
    # 全年月のパーティションをロード
    logging.info(f"特徴量を{start_dt.strftime('%Y-%m-%d')}から{end_dt.strftime('%Y-%m-%d')}までロードしています...")
    
    # 年月範囲を抽出
    date_range = pd.date_range(start=start_dt, end=end_dt, freq='MS')  # Month Start
    partitions_to_load = []
    
    for date in date_range:
        partition_path = parquet_dir / f'year={date.year}' / f'month={date.month}'
        if partition_path.exists():
            partitions_to_load.append(partition_path)
            logging.debug(f"  パーティション追加: {partition_path}")
    
    if not partitions_to_load:
        logging.error(f"指定期間の特徴量パーティションが見つかりません: {start_dt} - {end_dt}")
        sys.exit(1)
   
    logging.info(f"{len(partitions_to_load)}個のパーティションを読み込みます")
    features_df = pd.concat([pd.read_parquet(p) for p in partitions_to_load], ignore_index=True)
    
    # 日付フィルタリング
    if 'race_date' in features_df.columns:
        features_df['race_date'] = pd.to_datetime(features_df['race_date'])
        features_df = features_df[
            (features_df['race_date'] >= start_dt) &
            (features_df['race_date'] <= end_dt)
        ]
    
    if features_df.empty:
        logging.error(f"期間 {args.start_date} - {args.end_date} の特徴量データが見つかりません。")
        sys.exit(1)
    
    logging.info(f"特徴量をロードしました: {len(features_df)}行, {len(features_df.columns)}列")

    # features_dfにターゲットが含まれていることを前提とする
    target_cols = ['finish_position', 'finish_time_seconds']
    missing_targets = [col for col in target_cols if col not in features_df.columns]

    if missing_targets:
        logging.warning(f"特徴量データにターゲット列が含まれていません: {missing_targets}")
        sys.exit(1)

    final_df = features_df.dropna(subset=target_cols + ['race_id']).copy()
    if final_df.empty:
        logging.error("必須カラムの欠損値を除去した結果、評価データが0行になりました。")
        sys.exit(1)

    for col in feature_names:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
    logging.info(f"評価用データの準備完了: {len(final_df)}行")

    # --- 2.1. レース結果データ(races.parquet)のロード (track_surface取得用) ---
    parsed_data_path = config['parsed_data_path']
    races_parquet_path = Path(parsed_data_path) / 'parquet' / 'races' / 'races.parquet'
    try:
        races_df_master = pd.read_parquet(races_parquet_path)
        # 必要なカラムのみ抽出
        races_subset = races_df_master[['race_id', 'track_surface']].copy()
        races_subset['race_id'] = races_subset['race_id'].astype(str).str.strip()
        
        # final_dfにマージ
        final_df = final_df.merge(races_subset, on='race_id', how='left')
        logging.info("races.parquetからtrack_surfaceをマージしました")
    except Exception as e:
        logging.warning(f"レース結果データのロードに失敗: {e}")

    # --- 2.5. horses_performanceからオッズデータをマージ ---
    logging.info("horses_performance.parquetからオッズデータを読み込んでいます...")

    # パスの優先順位: parsed_parquet_path > parsed_data_path/parquet > data_path/parsed/parquet
    perf_path = None
    if 'parsed_parquet_path' in config:
        perf_path = Path(config['parsed_parquet_path']) / 'horses_performance' / 'horses_performance.parquet'
    elif 'parsed_data_path' in config:
        perf_path = Path(config['parsed_data_path']) / 'parquet' / 'horses_performance' / 'horses_performance.parquet'
    else:
        perf_path = Path(config.get('data_path', 'data')) / 'parsed' / 'parquet' / 'horses_performance' / 'horses_performance.parquet'

    print(f"DEBUG: perf_path resolved to: {perf_path}")
    print(f"DEBUG: perf_path exists: {perf_path.exists()}")

    if perf_path.exists():
        try:
            perf_df = pd.read_parquet(perf_path)
            logging.info(f"horses_performance.parquet読み込み完了: {len(perf_df):,}行")
            print(f"DEBUG: perf_df loaded, rows: {len(perf_df)}")

            # race_dateをdatetime型に変換（フィルタリング用）
            if 'race_date' in perf_df.columns:
                perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
                # 評価期間のデータのみを抽出（メモリ効率化）
                before_filter = len(perf_df)
                perf_df = perf_df[(perf_df['race_date'] >= start_dt) & (perf_df['race_date'] <= end_dt)]
                logging.info(f"評価期間でフィルタリング: {before_filter:,}行 → {len(perf_df):,}行")

            # キーとなるカラムを文字列型に統一
            final_df['race_id'] = final_df['race_id'].astype(str).str.strip()
            if 'horse_id' in final_df.columns:
                final_df['horse_id'] = final_df['horse_id'].astype(str).str.strip()

            perf_df['race_id'] = perf_df['race_id'].astype(str).str.strip()
            if 'horse_id' in perf_df.columns:
                perf_df['horse_id'] = perf_df['horse_id'].astype(str).str.strip()

            # win_oddsカラムのみを取得してマージ
            logging.info(f"perf_dfのカラム: {list(perf_df.columns)[:10]}... (全{len(perf_df.columns)}個)")

            # マージ前にfinal_dfから既存のwin_oddsカラムを削除（もし存在すれば）
            if 'win_odds' in final_df.columns:
                logging.info("final_dfに既存のwin_oddsカラムが存在します。削除してから再マージします。")
                final_df = final_df.drop(columns=['win_odds'])

            if 'win_odds' in perf_df.columns:
                logging.info(f"win_oddsカラム発見: 非null数={perf_df['win_odds'].notna().sum():,}/{len(perf_df):,}")
                odds_df = perf_df[['race_id', 'horse_id', 'win_odds']].copy()

                # マージ前の行数を記録
                rows_before = len(final_df)

                # 左外部結合でオッズデータをマージ
                final_df = final_df.merge(
                    odds_df,
                    on=['race_id', 'horse_id'],
                    how='left'
                )

                # マージ成功確認
                if 'win_odds' in final_df.columns:
                    odds_available = final_df['win_odds'].notna().sum()
                    logging.info(f"オッズデータのマージ完了: {odds_available}/{len(final_df)}行 ({odds_available/len(final_df)*100:.1f}%) でオッズ取得")
                    print(f"DEBUG: Odds merge success. Available: {odds_available}/{len(final_df)}")
                    
                    if rows_before != len(final_df):
                        logging.warning(f"マージ後に行数が変化しました: {rows_before} → {len(final_df)}")
                else:
                    logging.warning("マージ後にwin_oddsカラムが見つかりません")
                    print("DEBUG: win_odds column missing after merge")
                    final_df['win_odds'] = pd.NA
            else:
                logging.warning("horses_performance.parquetにwin_oddsカラムが存在しません")
                final_df['win_odds'] = pd.NA
        except Exception as e:
            logging.error(f"オッズデータの読み込みに失敗しました: {e}", exc_info=True)
            logging.warning("オッズデータなしで評価を続行します")
            final_df['win_odds'] = pd.NA
    else:
        logging.warning(f"horses_performance.parquetが見つかりません: {perf_path}")
        logging.warning("オッズデータなしで評価を続行します")
        final_df['win_odds'] = pd.NA

    # --- 3. 予測の実行 ---
    # 特徴量の整合性チェックと欠損補完
    missing_cols = [col for col in feature_names if col not in final_df.columns]
    if missing_cols:
        logging.warning(f"以下の特徴量がデータに存在しないため、0.0で補完します: {missing_cols[:10]} ... (全{len(missing_cols)}個)")
        for col in missing_cols:
            final_df[col] = 0.0

    logging.info("予測を実行中...")
    try:
        # MuEstimatorのインターフェースに合わせて予測
        if hasattr(estimator, 'predict_score'):
            predictions = estimator.predict_score(final_df)
        elif hasattr(estimator, 'predict'):
             predictions = estimator.predict(final_df)
        else:
             # Fallback
             predictions = estimator.predict(final_df[feature_names])
            
        final_df['predicted_score'] = predictions
        
        # ★ Phase E: 確率較正の適用 ★
        if calibrator:
            try:
                # Rankerのスコアを取得（MuEstimatorの内部Rankerを使用）
                # Note: estimator.model_rankerにアクセスできる前提
                if hasattr(estimator, 'model_ranker'):
                    ranker_score = estimator.model_ranker.predict(final_df[feature_names])
                    
                    # ★修正: Sigmoid変換を削除 - Rankerスコアを直接使用
                    proba_input = ranker_score
                    
                    # 較正
                    calibrated_proba = calibrator.predict_proba(proba_input)
                    final_df['calibrated_proba'] = calibrated_proba
                    logging.info("確率較正を適用しました。")
                else:
                    logging.warning("estimator.model_rankerが見つからないため、較正をスキップします。")
                    final_df['calibrated_proba'] = 0.0
            except Exception as e:
                logging.error(f"確率較正の適用中にエラー: {e}")
                final_df['calibrated_proba'] = 0.0
        
        logging.info("予測スコアの計算が完了しました。")
        
        print(f"DEBUG: Predictions sample: {predictions[:10]}")
        print(f"DEBUG: Predictions mean: {np.mean(predictions)}, std: {np.std(predictions)}")
        
    except Exception as e:
        logging.error(f"予測の実行中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. 評価指標の計算と保存 ---
    logging.info("評価指標の計算を開始します...")
    
    # ★ Phase E-2: race_num抽出（トラックバイアス用） ★
    if 'race_num' not in final_df.columns:
        # race_idから推定: 通常 "YYYYMMDDVVRR" 形式（RRがレース番号）
        try:
            final_df['race_num'] = final_df['race_id'].str[-2:].astype(int)
            logging.info("race_numをrace_idから抽出しました")
        except:
            logging.warning("race_numの抽出に失敗。トラックバイアス機能を無効化します。")
            final_df['race_num'] = 1

    daily_metrics = []

    # 日付ごとに集計
    for date, date_df in final_df.groupby('race_date'):
        metrics = {'date': date}
        
        # ★ Phase E-2: トラックバイアス検知（データリーク対策） ★
        # 日付内でrace_num順にソート
        date_df = date_df.sort_values('race_num').copy()
        
        # バイアス累積用（各レースの結果を順次追加）
        past_results_today = pd.DataFrame()
        bias_scores_dict = {}  # {race_id: bias_stats}
        
        try:
            from keibaai.src.features.track_bias import calculate_daily_bias, get_bias_fit_score
            track_bias_enabled = True
        except ImportError:
            logging.warning("track_bias.pyのインポートに失敗。バイアス検知を無効化します。")
            track_bias_enabled = False
        
        if track_bias_enabled:
            # レース番号順にバイアスを計算
            for current_race_num in sorted(date_df['race_num'].unique()):
                race_data = date_df[date_df['race_num'] == current_race_num].copy()
                
                # 同じrace_idの馬をグループ化
                for race_id, race_horses in race_data.groupby('race_id'):
                    # バイアス計算（このレース番号未満の過去結果のみ使用）
                    if len(past_results_today) >= 3:  # 最低3レース必要
                        # 現在のレースの馬場状態を取得
                        current_surface = race_horses['track_surface'].iloc[0] if 'track_surface' in race_horses.columns else None
                        
                        bias_stats = calculate_daily_bias(
                            past_results_today,
                            current_race_num,
                            target_surface=current_surface
                        )
                        bias_scores_dict[race_id] = bias_stats
                        logging.debug(f"レース{race_id} バイアス: {bias_stats}")
                    
                    # このレースの結果を累積（次のレースで使用）
                    past_results_today = pd.concat([past_results_today, race_horses], ignore_index=True)

        # 4.1 RMSE (Regressor)
        has_regressor = (hasattr(estimator, 'model_regressor') and estimator.model_regressor is not None) or \
                        (hasattr(estimator, 'regressor') and estimator.regressor is not None)
        
        if has_regressor:
            pred_reg = date_df['predicted_score']
            true_time = date_df['finish_time_seconds']
            metrics['rmse'] = np.sqrt(mean_squared_error(true_time, pred_reg))

        # 4.2 Spearman Correlation, Hit Rate, ROI
        correlations = []
        hits = 0
        races_count = 0
        total_bet = 0
        total_return = 0

        debug_race_count = 0
        for race_id, race_df in date_df.groupby('race_id'):
            if len(race_df) > 1:
                # 相関係数
                pred_score = race_df['predicted_score']
                true_rank = race_df['finish_position']
                corr, _ = spearmanr(pred_score, true_rank)
                if not np.isnan(corr):
                    correlations.append(corr)

                # AI本命馬を特定 (予測タイムが最小 = 最速予想)
                # Note: MuEstimator returns higher score for better horse?
                # If so, we should use idxmax(). But let's check what's happening.
                # ★ Phase E-2: バイアス調整 & 確率ベース選択 ★
                if 'calibrated_proba' in race_df.columns and race_df['calibrated_proba'].sum() > 0:
                    # 確率ベース選択（大きいほど良い）
                    if race_id in bias_scores_dict and track_bias_enabled:
                        # バイアス調整（確率に対して行う）
                        try:
                            race_df_copy = race_df.copy()
                            race_df_copy['bias_fit_score'] = race_df_copy.apply(
                                lambda row: get_bias_fit_score(row, bias_scores_dict[race_id]),
                                axis=1
                            )
                            # 確率へのバイアス調整: 単純な掛け算
                            # bias_fit_score: 0.0(不利) ~ 1.0(有利) -> 0.9 ~ 1.1
                            adjustment_factor = 0.9 + 0.2 * race_df_copy['bias_fit_score']
                            race_df_copy['adjusted_proba'] = race_df_copy['calibrated_proba'] * adjustment_factor
                            
                            best_horse_idx = race_df_copy['adjusted_proba'].idxmax()
                            best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                            logging.debug(f"バイアス調整(確率)適用: レース{race_id}")
                        except Exception as e:
                            logging.warning(f"バイアス調整(確率)失敗: {e}、通常確率選択を使用")
                            best_horse_idx = race_df['calibrated_proba'].idxmax()
                            best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                    else:
                        # バイアスなし確率選択
                        best_horse_idx = race_df['calibrated_proba'].idxmax()
                        best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                        
                elif race_id in bias_scores_dict and track_bias_enabled:
                    # スコアベース（小さいほど良い）でのバイアス調整
                    try:
                        race_df_copy = race_df.copy()
                        race_df_copy['bias_fit_score'] = race_df_copy.apply(
                            lambda row: get_bias_fit_score(row, bias_scores_dict[race_id]),
                            axis=1
                        )
                        # スコアへのバイアス調整: 小さいほど良いので、有利な馬はスコアを小さくする
                        # bias_fit_score: 0.0(不利) ~ 1.0(有利) -> 1.1 ~ 0.9
                        adjustment_factor = 1.1 - 0.2 * race_df_copy['bias_fit_score']
                        race_df_copy['adjusted_score'] = race_df_copy['predicted_score'] * adjustment_factor
                        
                        best_horse_idx = race_df_copy['adjusted_score'].idxmin()
                        best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                        logging.debug(f"バイアス調整(スコア)適用: レース{race_id}")
                    except Exception as e:
                        logging.warning(f"バイアス調整(スコア)失敗: {e}、通常スコア選択を使用")
                        best_horse_idx = race_df['predicted_score'].idxmin()
                        best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                else:
                    # 通常スコア選択（小さいほど良い）
                    best_horse_idx = race_df['predicted_score'].idxmin()
                    best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']

                if debug_race_count < 3:
                    print(f"\nDEBUG: Race {race_id}")
                    print(f"  Predictions (Score): {pred_score.tolist()}")
                    if 'calibrated_proba' in race_df.columns:
                        print(f"  Predictions (Proba): {race_df['calibrated_proba'].tolist()}")
                    print(f"  True Ranks: {true_rank.tolist()}")
                    print(f"  Best Horse Idx: {best_horse_idx}")
                    print(f"  Best Horse Rank: {best_horse_rank}")
                    print(f"  Win Odds: {race_df.loc[best_horse_idx, 'win_odds'] if 'win_odds' in race_df.columns else 'N/A'}")
                    if race_id in bias_scores_dict:
                        print(f"  Track Bias: {bias_scores_dict[race_id]}")
                        print(f"  Bias Adjusted: True")
                    debug_race_count += 1

                # Hit Rate (3着以内に入ったか)
                if best_horse_rank <= 3:
                    hits += 1

                # ROI計算 (単勝100円ベット想定)
                # ★ Phase F: EVベッティング ★
                bet_amount = 0
                is_bet = False
                
                if 'calibrated_proba' in race_df.columns and 'win_odds' in race_df.columns:
                    # ★重要な修正: Pandasの参照渡しリークを防ぐため.copy()を使用★
                    race_df_ev = race_df.copy()
                    
                    # EV計算
                    # win_oddsがNaNの場合は0扱い
                    odds_series = race_df_ev['win_odds'].fillna(0)
                    race_df_ev['expected_value'] = race_df_ev['calibrated_proba'] * odds_series
                    
                    # EV最大の馬を探す
                    ev_best_idx = race_df_ev['expected_value'].idxmax()
                    max_ev = race_df_ev.loc[ev_best_idx, 'expected_value']
                    
                    # 戦略: EV最大馬に賭ける。ただしEVが閾値を超えている場合のみ。
                    best_horse_idx = ev_best_idx
                    best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                    
                    # 閾値判定 (Threshold = 1.1)
                    if max_ev > 1.1:
                        bet_amount = 100
                        is_bet = True
                    else:
                        bet_amount = 0 # 見送り
                        
                else:
                    # 従来通り（全レース100円）
                    bet_amount = 100
                    is_bet = True

                if is_bet:
                    total_bet += bet_amount

                    # 勝利した場合のみ払い戻し（実オッズを使用）
                    if best_horse_rank == 1:
                        # win_oddsを使用（horses_performanceから取得した実オッズ）
                        if 'win_odds' in race_df.columns:
                            odds = race_df.loc[best_horse_idx, 'win_odds']
                            if pd.notna(odds) and odds > 0:
                                payout = bet_amount * odds
                                total_return += payout
                            # オッズがない場合は払い戻しなし（ベットしたが回収できず）

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
    model_name = Path(args.model_dir).name
    output_path = output_dir / f'evaluation_results_{model_name}.csv'

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
