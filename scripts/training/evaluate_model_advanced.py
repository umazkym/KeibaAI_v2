#!/usr/bin/env python3
# src/models/evaluate_model_advanced.py
"""
高度な評価スクリプト（σ・ν活用版）
モンテカルロシミュレーション、EV最大化、レース選別を実装

データリーク対策:
- オッズは評価時のみ使用（学習には使わない）
- 時系列を厳守（評価期間 > 学習期間）
- .copy()で参照渡しリーク防止
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import mean_squared_error
import joblib

# プロジェクトルート
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from keibaai.src.pipeline_core import setup_logging
    from keibaai.src.models.model_train import MuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    sys.exit(1)


def monte_carlo_win_probability(mu, sigma, nu, n_samples=1000):
    """
    モンテカルロシミュレーションで1位確率を計算
    
    データリーク対策:
    - mu, sigma, nuは全て過去データから学習したモデルの予測
    - 未来情報は一切使用されていない
    """
    n_horses = len(mu)
    win_counts = np.zeros(n_horses)
    
    for _ in range(n_samples):
        # 確率的サンプリング
        sampled_times = np.random.normal(
            loc=mu,
            scale=sigma * nu
        )
        winner_idx = np.argmin(sampled_times)
        win_counts[winner_idx] += 1
    
    return win_counts / n_samples


def ev_maximization_strategy(race_df, mu, sigma, nu, ev_threshold=1.2):
    """
    EV最大化戦略
    
    データリーク対策:
    - race_dfは.copy()したものを使用
    - オッズは評価時のデータ（未来情報ではない）
    - モデル予測は過去データから学習
    
    Returns:
        (best_horse_idx, should_bet, max_ev, win_prob)
    """
    # オッズチェック（評価時のデータ）
    if 'win_odds' not in race_df.columns:
        # オッズがない場合は確率最大の馬を選択
        win_probs = monte_carlo_win_probability(mu, sigma, nu)
        best_idx = race_df.iloc[np.argmax(win_probs)].name
        return best_idx, True, 0.0, win_probs[np.argmax(win_probs)]
    
    # 1位確率計算
    win_probs = monte_carlo_win_probability(mu, sigma, nu)
    
    # EV計算
    odds = race_df['win_odds'].fillna(0).values
    expected_values = win_probs * odds
    
    best_idx_local = np.argmax(expected_values)
    best_idx = race_df.iloc[best_idx_local].name  # DataFrame indexを取得
    max_ev = expected_values[best_idx_local]
    
    # 閾値判定
    should_bet = (max_ev > ev_threshold)
    
    return best_idx, should_bet, max_ev, win_probs[best_idx_local]


def race_selection_strategy(nu, nu_threshold=1.0):
    """
    レース選別戦略
    
    データリーク対策:
    - nuは過去データから学習
    """
    # 荒れやすいレースを避ける
    return nu <= nu_threshold


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 高度評価（σ・ν活用）')
    parser.add_argument('--model_dir', type=str, required=True, help='モデルディレクトリ')
    parser.add_argument('--start_date', type=str, required=True, help='評価開始日')
    parser.add_argument('--end_date', type=str, required=True, help='評価終了日')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='設定ファイル')
    parser.add_argument('--strategy', type=str, default='combined',
                       choices=['baseline', 'monte_carlo', 'ev_max', 'race_select', 'combined'],
                       help='評価戦略')
    parser.add_argument('--ev_threshold', type=float, default=1.2, help='EV閾値')
    parser.add_argument('--nu_threshold', type=float, default=1.0, help='ν閾値')
    parser.add_argument('--mc_samples', type=int, default=1000, help='モンテカルロサンプル数')
    parser.add_argument('--log_level', type=str, default='INFO', help='ログレベル')

    args = parser.parse_args()

    # 設定読み込み
    project_root_path = Path(__file__).resolve().parent.parent.parent
    config_path = project_root_path / args.config

    with open(config_path, 'r',encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # ロギング
    data_path = project_root_path / config.get('data_path', 'data')
    logs_path = data_path / 'logs'
    log_file = logs_path / f"evaluate_advanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    setup_logging(log_level=args.log_level, log_file=str(log_file), log_format=log_format)

    logging.info("=" * 60)
    logging.info(f"高度評価開始: 戦略={args.strategy}")
    logging.info("=" * 60)
    logging.info(f"モデル: {args.model_dir}")
    logging.info(f"評価期間: {args.start_date} - {args.end_date}")
    logging.info(f"EV閾値: {args.ev_threshold}")
    logging.info(f"ν閾値: {args.nu_threshold}")

def load_data_and_models(args, config):
    """データとモデルをロードする関数"""
    project_root_path = Path(__file__).resolve().parent.parent.parent
    data_path = project_root_path / config.get('data_path', 'data')
    
    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')

    # ★データリーク防止: 評価期間が学習期間より後であることを確認★
    model_info_path = Path(args.model_dir) / 'model_info.json'
    if model_info_path.exists():
        import json
        with open(model_info_path) as f:
            model_info = json.load(f)
        train_end = model_info.get('train_period', '').split(' to ')[-1]
        if train_end:
            train_end_dt = datetime.strptime(train_end, '%Y-%m-%d')
            assert start_dt > train_end_dt, f"評価開始日({start_dt})が学習終了日({train_end_dt})より前！データリーク！"
            logging.info(f"✓ データリーク防止: 学習期間({train_end}) < 評価期間({args.start_date})")

    # データロード
    features_path_str = config['features_path'].replace('${data_path}', str(data_path.relative_to(project_root_path)))
    features_path_str = features_path_str.replace('/', '\\') if os.name == 'nt' else features_path_str
    
    if Path(features_path_str).is_absolute():
        features_dir = Path(features_path_str)
    else:
        features_dir = project_root_path / features_path_str
        
    parquet_dir = features_dir / 'parquet'

    year_partitions = sorted(parquet_dir.glob('year=*'))
    dfs = []
    for year_partition in year_partitions:
        df_year = pd.read_parquet(year_partition)
        dfs.append(df_year)

    features_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"特徴量データロード: {len(features_df)}行")

    # 日付フィルタ
    if 'race_date' in features_df.columns:
        features_df['race_date'] = pd.to_datetime(features_df['race_date'])
        features_df = features_df[
            (features_df['race_date'] >= start_dt) &
            (features_df['race_date'] <= end_dt)
        ]
        logging.info(f"日付フィルタ後: {len(features_df)}行")

    # レース結果・オッズロード
    parsed_data_path_str = config['parsed_data_path'].replace('${data_path}', str(data_path.relative_to(project_root_path)))
    parsed_data_path_str = parsed_data_path_str.replace('/', '\\') if os.name == 'nt' else parsed_data_path_str
    
    if Path(parsed_data_path_str).is_absolute():
        parsed_data_path = Path(parsed_data_path_str)
    else:
        parsed_data_path = project_root_path / parsed_data_path_str
        
    races_path = Path(parsed_data_path) / 'parquet' / 'races' / 'races.parquet'
    races_df = pd.read_parquet(races_path)
    logging.info(f"レース結果ロード: {len(races_df)}行")
    
    # マージ準備
    merge_keys = ['race_id', 'horse_id']
    for key in merge_keys:
        features_df[key] = features_df[key].astype(str).str.strip()
        races_df[key] = races_df[key].astype(str).str.strip()

    features_df = features_df.drop_duplicates(subset=merge_keys, keep='first')

    # ★重要: features_dfにターゲットが含まれている場合（リーク）、マージで衝突するので削除する★
    drop_cols = ['finish_position', 'finish_time_seconds', 'win_odds', 'track_surface']
    existing_drop_cols = [c for c in drop_cols if c in features_df.columns]
    if existing_drop_cols:
        logging.warning(f"features_dfからターゲット列を削除します（衝突回避）: {existing_drop_cols}")
        features_df = features_df.drop(columns=existing_drop_cols)

    # ターゲット+オッズを結合
    eval_targets = ['finish_position', 'finish_time_seconds', 'win_odds', 'track_surface']
    available_targets = [col for col in eval_targets if col in races_df.columns]
    
    races_subset = races_df[merge_keys + available_targets].copy()
    final_df = pd.merge(features_df, races_subset, on=merge_keys, how='inner')
    logging.info(f"マージ後: {len(final_df)}行")

    final_df = final_df.dropna(subset=['finish_position', 'race_id']).copy()
    logging.info(f"欠損値除去後: {len(final_df)}行")

    # モデル設定ロード
    models_config_path = project_root_path / 'configs' / 'models.yaml'
    with open(models_config_path, 'r', encoding='utf-8') as f:
        models_config = yaml.safe_load(f)

    model_dir_path = Path(args.model_dir)

    # μモデル
    from keibaai.src.models.model_train import MuEstimator
    mu_model = MuEstimator(models_config.get('mu_estimator', {}))
    mu_model.load_model(str(model_dir_path))

    # σモデル（オプショナル）
    sigma_model = None
    sigma_features = []
    try:
        sigma_path = model_dir_path / 'sigma_model.pkl'
        sigma_features_path = model_dir_path / 'sigma_features.json'
        sigma_model = joblib.load(sigma_path)
        with open(sigma_features_path) as f:
            import json
            sigma_features = json.load(f)
        logging.info("σモデルロード完了")
    except:
        logging.warning("σモデルなし: σ=1.0固定")

    # νモデル（オプショナル）
    nu_model = None
    nu_features = []
    try:
        nu_path = model_dir_path / 'nu_model.pkl'
        nu_features_path = model_dir_path / 'nu_features.json'
        nu_model = joblib.load(nu_path)
        with open(nu_features_path) as f:
            nu_features = json.load(f)
        logging.info("νモデルロード完了")
    except:
        logging.warning("νモデルなし: ν=1.0固定")

    return final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features


def evaluate_strategy(final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features, strategy, ev_threshold, nu_threshold, mc_samples):
    """戦略を評価する関数"""
    # ★重要: final_dfをコピーして汚染を防ぐ★
    # これがないと、1回目の評価でmu_pred/sigma_predが追加され、
    # 2回目以降の評価で予測が変わらなくなる
    final_df = final_df.copy()
    
    # 特徴量名
    feature_names = mu_model.feature_names

    # 予測
    # logging.info("予測実行中...")
    X = final_df[feature_names]
    mu_pred = mu_model.model_regressor.predict(X)
    final_df['mu_pred'] = mu_pred

    # σ予測
    if sigma_model:
        sigma_pred_squared = sigma_model.predict(X[sigma_features])
        final_df['sigma_pred'] = np.sqrt(np.maximum(sigma_pred_squared, 0))
    else:
        final_df['sigma_pred'] = 1.0

    # 評価
    total_bet = 0
    total_return = 0
    hits = 0
    races_count = 0

    for date, date_df in final_df.groupby('race_date'):
        date_df_copy = date_df.copy()  # ★リーク防止: .copy()必須★
        
        for race_id, race_df in date_df_copy.groupby('race_id'):
            if len(race_df) <= 1:
                continue

            race_df_copy = race_df.copy()  # ★リーク防止: 2重.copy()★
            races_count += 1

            # ν予測（レース単位）
            if nu_model:
                try:
                    race_feat = {
                        'distance_m': race_df_copy['distance_m'].iloc[0],
                        'head_count': len(race_df_copy),
                        'weather': race_df_copy['weather'].iloc[0] if 'weather' in race_df_copy.columns else 'sunny',
                        'track_surface': race_df_copy['track_surface'].iloc[0] if 'track_surface' in race_df_copy.columns else 'turf',
                        'track_condition': race_df_copy['track_condition'].iloc[0] if 'track_condition' in race_df_copy.columns else 'good'
                    }
                    race_feat_df = pd.DataFrame([race_feat])
                    race_feat_df = pd.get_dummies(race_feat_df, columns=['weather', 'track_surface', 'track_condition'])
                    # 欠損カラム補完
                    for col in nu_features:
                        if col not in race_feat_df.columns:
                            race_feat_df[col] = 0
                    race_feat_df = race_feat_df[nu_features]
                    nu_pred = nu_model.predict(race_feat_df)[0]
                except Exception as e:
                    nu_pred = 1.0
            else:
                nu_pred = 1.0

            race_df_copy['nu_pred'] = nu_pred

            # 戦略選択
            mu = race_df_copy['mu_pred'].values
            sigma = race_df_copy['sigma_pred'].values
            nu = nu_pred

            best_horse_idx = None
            should_bet = True

            if strategy == 'baseline':
                best_horse_idx = race_df_copy['mu_pred'].idxmin()
            
            elif strategy == 'monte_carlo':
                win_probs = monte_carlo_win_probability(mu, sigma, nu, mc_samples)
                best_horse_idx = race_df_copy.iloc[np.argmax(win_probs)].name

            elif strategy == 'ev_max':
                best_horse_idx, should_bet, max_ev, win_prob = ev_maximization_strategy(
                    race_df_copy, mu, sigma, nu, ev_threshold
                )

            elif strategy == 'race_select':
                if race_selection_strategy(nu, nu_threshold):
                    best_horse_idx = race_df_copy['mu_pred'].idxmin()
                else:
                    should_bet = False

            elif strategy == 'combined':
                if race_selection_strategy(nu, nu_threshold):
                    best_horse_idx, should_bet, max_ev, win_prob = ev_maximization_strategy(
                        race_df_copy, mu, sigma, nu, ev_threshold
                    )
                else:
                    should_bet = False

            # ベット判定
            if should_bet and best_horse_idx is not None:
                best_horse_rank = race_df.loc[best_horse_idx, 'finish_position']
                win_odds = race_df.loc[best_horse_idx, 'win_odds'] if 'win_odds' in race_df.columns else 0

                total_bet += 100
                if best_horse_rank == 1:
                    hits += 1
                    total_return += win_odds * 100

    hit_rate = hits / races_count if races_count > 0 else 0
    roi = total_return / total_bet if total_bet > 0 else 0
    
    return {
        'strategy': strategy,
        'ev_threshold': ev_threshold,
        'nu_threshold': nu_threshold,
        'total_races': races_count,
        'total_bet': total_bet,
        'total_return': total_return,
        'roi': roi,
        'hit_rate': hit_rate
    }


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI 高度評価（σ・ν活用）')
    parser.add_argument('--model_dir', type=str, required=True, help='モデルディレクトリ')
    parser.add_argument('--start_date', type=str, required=True, help='評価開始日')
    parser.add_argument('--end_date', type=str, required=True, help='評価終了日')
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='設定ファイル')
    parser.add_argument('--strategy', type=str, default='combined',
                       choices=['baseline', 'monte_carlo', 'ev_max', 'race_select', 'combined'],
                       help='評価戦略')
    parser.add_argument('--ev_threshold', type=float, default=1.2, help='EV閾値')
    parser.add_argument('--nu_threshold', type=float, default=1.0, help='ν閾値')
    parser.add_argument('--mc_samples', type=int, default=1000, help='モンテカルロサンプル数')
    parser.add_argument('--log_level', type=str, default='INFO', help='ログレベル')

    args = parser.parse_args()

    # 設定読み込み
    project_root_path = Path(__file__).resolve().parent.parent.parent
    config_path = project_root_path / args.config

    with open(config_path, 'r',encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # ロギング
    data_path = project_root_path / config.get('data_path', 'data')
    logs_path = data_path / 'logs'
    log_file = logs_path / f"evaluate_advanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    setup_logging(log_level=args.log_level, log_file=str(log_file), log_format=log_format)

    logging.info("=" * 60)
    logging.info(f"高度評価開始: 戦略={args.strategy}")
    logging.info("=" * 60)
    
    # データとモデルのロード
    final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features = load_data_and_models(args, config)
    
    # 評価実行
    result = evaluate_strategy(
        final_df, mu_model, sigma_model, sigma_features, nu_model, nu_features,
        args.strategy, args.ev_threshold, args.nu_threshold, args.mc_samples
    )

    logging.info("=" * 60)
    logging.info(f"評価完了: 戦略={result['strategy']}")
    logging.info("=" * 60)
    logging.info(f"Total Races: {result['total_races']}")
    logging.info(f"Total Bet: {result['total_bet']} JPY")
    logging.info(f"Total Return: {result['total_return']:.0f} JPY")
    logging.info(f"ROI: {result['roi']:.2%}")
    logging.info(f"Hit Rate: {result['hit_rate']:.2%}")
    logging.info("=" * 60)

    # 結果保存
    output_dir = data_path / 'evaluation'
    output_dir.mkdir(exist_ok=True)
    
    import json
    result_file = output_dir / f"advanced_results_{args.strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(result_file, 'w') as f:
        json.dump(result, f, indent=2)
    
    logging.info(f"結果保存: {result_file}")


if __name__ == '__main__':
    main()
