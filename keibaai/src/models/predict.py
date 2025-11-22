#!/usr/bin/env python3
# src/models/predict.py
"""
モデル推論 実行スクリプト
指定された日付の特徴量を読み込み、
学習済みモデル（μ, σ, ν）で推論を実行し、
結果を data/predictions/ に保存する。

仕様書 17.3章 に基づく実装

実行例:
python src/models/predict.py --date 2023-10-01 --model_dir data/models/latest
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import numpy as np
import yaml
import joblib

# プロジェクトルート（keibaai/）をパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))  # keibaai/src をパスに追加

try:
    from pipeline_core import setup_logging, load_config
    from utils.data_utils import load_parquet_data_by_date
    from models.model_train import MuEstimator
    from models.sigma_estimator import SigmaEstimator
    from models.nu_estimator import NuEstimator
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    print("プロジェクトルートが正しく設定されているか確認してください。")
    print(f"project_root: {project_root}")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def load_model_safely(model_class, config, model_path_str):
    """モデルをロードする（クラスラッパーを使用）"""
    model_path = Path(model_path_str)
    if not model_path.exists():
        logging.error(f"モデルディレクトリが見つかりません: {model_path}")
        raise FileNotFoundError(f"モデルディレクトリが見つかりません: {model_path}")
        
    model = model_class(config)
    model.load_model(str(model_path))
    return model

def load_plain_model(model_path_str, meta_path_str):
    """
    プレーンなLGBMモデルファイルとメタデータ（特徴量リスト）をロードする
    (train_sigma_nu_models.pyの実装に合わせたロード方法)
    """
    model_file = Path(model_path_str)
    meta_file = Path(meta_path_str)
    
    if not model_file.exists():
        raise FileNotFoundError(f"モデルファイルが見つかりません: {model_file}")
    if not meta_file.exists():
        raise FileNotFoundError(f"メタファイル（特徴量リスト）が見つかりません: {meta_file}")

    model = joblib.load(model_file)
    
    with open(meta_file, 'r', encoding='utf-8') as f:
        meta_data = yaml.safe_load(f)
        if isinstance(meta_data, dict): # 仕様書 7.7.2 の場合
            feature_names = meta_data.get('feature_names', [])
        else: # 仕様書 13.4 (train_sigma_nu_models.py) の場合
            feature_names = meta_data 
            
    return model, feature_names


def prepare_nu_inference(race_features_df, feature_names):
    """
    νモデル推論用の特徴量を作成する
    """
    # 1. レース単位の集計
    head_count = len(race_features_df)
    avg_win_odds = race_features_df['win_odds'].mean() if 'win_odds' in race_features_df.columns else 0
    std_win_odds = race_features_df['win_odds'].std() if 'win_odds' in race_features_df.columns else 0
    
    # 2. カテゴリ変数の取得 (最初の1行目を使用)
    weather = race_features_df['weather'].iloc[0] if 'weather' in race_features_df.columns else None
    distance_m = race_features_df['distance_m'].iloc[0] if 'distance_m' in race_features_df.columns else 0
    
    # 3. データフレーム作成
    data = {
        'distance_m': [distance_m],
        'head_count': [head_count],
        'avg_win_odds': [avg_win_odds],
        'std_win_odds': [std_win_odds],
        'weather': [weather]
    }
    df = pd.DataFrame(data)
    
    # 4. ダミー変数化
    df = pd.get_dummies(df, columns=['weather'])
    
    # 5. カラムを学習時と揃える (reindex)
    # 欠損カラムは0で埋め、不要カラムは削除
    df_reindexed = df.reindex(columns=feature_names, fill_value=0)
    
    return df_reindexed


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Keiba AI モデル推論パイプライン')
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='推論対象日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--model_dir',
        type=str,
        required=True,
        help='学習済みモデルが格納されているディレクトリ (例: data/models/latest)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='configs/default.yaml',
        help='設定ファイルパス'
    )
    parser.add_argument(
        '--models_config',
        type=str,
        default='configs/models.yaml',
        help='モデル設定ファイルパス'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='ログレベル'
    )
    parser.add_argument(
        '--output_filename',
        type=str,
        default=None,
        help='出力ファイル名 (例: predictions_2024_01_01.parquet)。指定がない場合は自動生成。'
    )
    
    args = parser.parse_args()
    
    # ロギング設定は後で行う（config読み込み後）
    # setup_logging(log_level=args.log_level)
    print("モデル推論パイプライン開始") # logging設定前なのでprint
    logging.info(f"推論対象日: {args.date}")
    logging.info(f"モデルディレクトリ: {args.model_dir}")

    # --- 0. 設定とロギング ---
    try:
        # 設定ファイルパスを絶対パスで解決
        # args.configは 'configs/default.yaml' のような形式を想定
        config_path = project_root / args.config
        models_config_path = project_root / args.models_config
        
        if not config_path.exists():
            # フォールバック: configs/default.yaml
            config_path = project_root / 'configs' / 'default.yaml'
            
        if not models_config_path.exists():
            # フォールバック: configs/models.yaml
            models_config_path = project_root / 'configs' / 'models.yaml'
        
        logging.info(f"設定ファイル読み込み: {config_path}")
        
        config = load_config(str(config_path))
        
        # 変数置換処理（${data_path}などを実際の値に置き換える）
        def replace_variables(config_dict, variables=None):
            if variables is None:
                variables = {}
            # まずdata_pathを取得
            if 'data_path' in config_dict:
                variables['data_path'] = config_dict['data_path']
            
            # 再帰的に置換
            for key, value in config_dict.items():
                if isinstance(value, str):
                    for var_name, var_value in variables.items():
                        value = value.replace(f'${{{var_name}}}', var_value)
                    config_dict[key] = value
                    # 新しい変数として登録
                    if key.endswith('_path'):
                        variables[key] = value
                elif isinstance(value, dict):
                    config_dict[key] = replace_variables(value, variables)
            return config_dict
        
        config = replace_variables(config)
        paths = config.get('paths', config)  # pathsキーがない場合はconfigをそのまま使う
        
        with open(models_config_path, 'r') as f:
            models_config = yaml.safe_load(f)

        # --- 1. 日付パース ---
        try:
            target_dt = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError as e:
            # まだロギング設定前なのでprintで出力
            print(f"日付フォーマットエラー: {e}")
            sys.exit(1)

        # ログ設定
        log_path_template = config.get('logging', {}).get('log_file', 'data/logs/{YYYY}/{MM}/{DD}/predict.log')
        # target_dtを使ってログパスを生成
        log_path = log_path_template.format(YYYY=target_dt.year, MM=f"{target_dt.month:02}", DD=f"{target_dt.day:02}")
        
        # project_root基準の絶対パスに変換
        if not Path(log_path).is_absolute():
            log_path = project_root / log_path
            
        log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'

        setup_logging(
            log_level=args.log_level,
            log_file=str(log_path),
            log_format=log_format
        )
            
    except FileNotFoundError as e:
        print(f"設定ファイルが見つかりません: {e}")
        sys.exit(1)

    # --- 2. データロード ---
    features_dir = Path('data/features/parquet')
    # project_root基準の絶対パスに変換
    if not features_dir.is_absolute():
        features_dir = project_root / features_dir
        
    logging.info(f"特徴量データをロードします: {features_dir}")
    
    # 日付範囲でロード
    features_df = load_parquet_data_by_date(features_dir, target_dt, target_dt, date_col='race_date')
    
    if features_df.empty:
        logging.error(f"{args.date} の特徴量データが見つかりません。")
        sys.exit(1)
        
    logging.info(f"データロード完了: {len(features_df)}行")
    logging.info(f"カラム一覧: {list(features_df.columns)}") # デバッグ出力追加
    
    if 'horse_number' not in features_df.columns:
        logging.warning("horse_numberカラムが存在しません。出馬表データから結合を試みます。")
        # shutubaデータをロードして結合
        shutuba_path = project_root / 'data/parsed/parquet/shutuba.parquet'
        if shutuba_path.exists():
             try:
                 logging.info(f"出馬表データをロード中: {shutuba_path}")
                 shutuba_df = pd.read_parquet(shutuba_path)
                 
                 # race_idとhorse_idで結合
                 if 'horse_number' in shutuba_df.columns:
                     # 必要なカラムだけ抽出してマージ
                     shutuba_subset = shutuba_df[['race_id', 'horse_id', 'horse_number']].copy()
                     # 型変換: horse_idは両方とも文字列型であることを確認
                     features_df['horse_id'] = features_df['horse_id'].astype(str)
                     shutuba_subset['horse_id'] = shutuba_subset['horse_id'].astype(str)
                     features_df['race_id'] = features_df['race_id'].astype(str)
                     shutuba_subset['race_id'] = shutuba_subset['race_id'].astype(str)
                     
                     features_df = pd.merge(features_df, shutuba_subset, on=['race_id', 'horse_id'], how='left')
                     logging.info(f"horse_numberを結合しました。欠損数: {features_df['horse_number'].isnull().sum()}")
                     
                     # 結合後も欠損がある場合は0で埋める（エラー回避）
                     features_df['horse_number'] = features_df['horse_number'].fillna(0).astype(int)
                 else:
                     logging.error("出馬表データにhorse_numberカラムがありません。")
             except Exception as e:
                 logging.error(f"出馬表データのロード/結合に失敗: {e}")
        else:
             logging.error(f"出馬表データが見つかりません: {shutuba_path}")
             
    # それでもhorse_numberがない場合はエラー回避のためにダミーを追加
    if 'horse_number' not in features_df.columns:
        logging.warning("horse_numberを取得できませんでした。ダミー値(0)を使用します。")
        features_df['horse_number'] = 0

    # モデルロード
    logging.info("モデルをロードします...")
    model_dir_path = Path(args.model_dir)
    # 相対パスの場合、project_root基準に変換
    if not model_dir_path.is_absolute():
        model_dir_path = project_root / model_dir_path
    # --- 0. 設定とロギング ---
    try:
        # 設定ファイルパスを絶対パスで解決
        # args.configは 'configs/default.yaml' のような形式を想定
        # project_root/configs/default.yaml となるように結合
        config_path = project_root / args.config
        models_config_path = project_root / args.models_config
        
        if not config_path.exists():
            # フォールバック: configs/default.yaml
            config_path = project_root / 'configs' / 'default.yaml'
            
        if not models_config_path.exists():
            # フォールバック: configs/models.yaml
            models_config_path = project_root / 'configs' / 'models.yaml'
        
        logging.info(f"設定ファイル読み込み: {config_path}")
        
        # 3.1 μ (mu) モデル
        logging.info("μモデルをロード中...")
        mu_model_config = models_config.get('mu_estimator', {})
        
        # μモデルのロード（3つの形式に対応）
        mu_model_pkl = model_dir_path / 'mu_model.pkl'
        mu_model_dir = model_dir_path / 'mu_model'
        regressor_pkl = model_dir_path / 'regressor.pkl'
        ranker_pkl = model_dir_path / 'ranker.pkl'

        if regressor_pkl.exists() and ranker_pkl.exists():
            # 新形式: regressor.pkl と ranker.pkl が直接存在
            logging.info(f"μモデル（新形式）をロード: {model_dir_path}")
            mu_model = MuEstimator(mu_model_config)
            mu_model.load_model(str(model_dir_path))
        elif mu_model_pkl.exists():
            # 旧形式1: mu_model.pkl が直接存在
            logging.info(f"μモデルファイルを直接ロード: {mu_model_pkl}")
            mu_model = MuEstimator(mu_model_config)
            mu_model.load_model(str(mu_model_pkl.parent))
        elif mu_model_dir.exists():
            # 旧形式2: mu_model ディレクトリ
            logging.info(f"μモデルディレクトリからロード: {mu_model_dir}")
            mu_model = load_model_safely(
                MuEstimator,
                mu_model_config,
                str(mu_model_dir)
            )
        else:
            raise FileNotFoundError(f"μモデルが見つかりません: {regressor_pkl}, {ranker_pkl}, {mu_model_pkl}, または {mu_model_dir}")

        # 3.2 σ (sigma) モデル (プレーンなLGBMモデルをロード)
        sigma_model = None
        sigma_features = None
        try:
            logging.info("σモデルをロード中...")
            sigma_model, sigma_features = load_plain_model(
                str(model_dir_path / 'sigma_model.pkl'),
                str(model_dir_path / 'sigma_features.json')
            )
            sigma_model.feature_names_ = sigma_features # 特徴量リストをアタッチ
            logging.info("σモデルのロード完了")
        except FileNotFoundError as e:
            logging.warning(f"σモデルが見つかりません: {e}")
            logging.warning("グローバル値（σ=1.0）を使用します")

        # 3.3 ν (nu) モデル (プレーンなLGBMモデルをロード)
        nu_model = None
        nu_features = None
        try:
            logging.info("νモデルをロード中...")
            nu_model, nu_features = load_plain_model(
                str(model_dir_path / 'nu_model.pkl'),
                str(model_dir_path / 'nu_features.json')
            )
            nu_model.feature_names_ = nu_features # 特徴量リストをアタッチ
            logging.info("νモデルのロード完了")
        except FileNotFoundError as e:
            logging.warning(f"νモデルが見つかりません: {e}")
            logging.warning("グローバル値（ν=1.0）を使用します")
        
        if sigma_model and nu_model:
            logging.info("全モデル（μ, σ, ν）のロード完了")
        else:
            logging.info("μモデルのみロード完了（σ/νは代替値使用）")

    except Exception as e:
        logging.error(f"モデルのロード中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)
        
    # --- 4. 推論実行 ---
    logging.info("推論実行中...")
    
    predictions_list = []
    race_ids = features_df['race_id'].unique()
    
    for race_id in race_ids:
        race_features_df = features_df[features_df['race_id'] == race_id].copy().reset_index(drop=True)
        
        if race_features_df.empty:
            continue
            
        # 4.1 μ の予測
        try:
            mu_pred = mu_model.predict(race_features_df)
        except Exception as e:
            logging.error(f"レース {race_id} のμ予測に失敗: {e}")
            continue
        
        # 4.2 σ の予測
        if sigma_model is not None:
            try:
                # sigmaモデルの特徴量がデータフレームに含まれているか確認
                missing_cols = [col for col in sigma_features if col not in race_features_df.columns]
                if missing_cols:
                    # 簡易的な欠損埋め (0)
                    for col in missing_cols:
                        race_features_df[col] = 0
                
                X_sigma = race_features_df[sigma_features]
                sigma_pred = sigma_model.predict(X_sigma)
                sigma_pred = np.sqrt(np.maximum(sigma_pred, 0.0)) 
            except Exception as e:
                logging.warning(f"レース {race_id} のσ予測に失敗: {e}。グローバル値 (1.0) で代替します。")
                sigma_pred = np.full(len(race_features_df), 1.0)
        else:
            sigma_pred = np.full(len(race_features_df), 1.0)
        
        # 4.3 ν の予測
        if nu_model is not None:
            try:
                # 特徴量作成
                X_nu = prepare_nu_inference(race_features_df, nu_features)
                nu_pred_val = nu_model.predict(X_nu)[0]
                # nu_pred はレース全体で1つの値
                nu_pred = nu_pred_val
            except Exception as e:
                logging.warning(f"レース {race_id} のν予測に失敗: {e}。グローバル値 (1.0) で代替します。")
                nu_pred = 1.0
        else:
            nu_pred = 1.0
        
        # 結果を格納
        result_df = pd.DataFrame({
            'race_id': race_id,
            'horse_id': race_features_df['horse_id'],
            'horse_number': race_features_df['horse_number'], # 追加
            'mu': mu_pred,
            'sigma': sigma_pred,
            'nu': nu_pred # このレースの全馬に同じν値を適用
        })
        
        # 必要なカラムを追加（日付など）
        if 'race_date' in race_features_df.columns:
            result_df['race_date'] = race_features_df['race_date'].iloc[0]
            
        predictions_list.append(result_df)
        
    if not predictions_list:
        logging.error("推論結果がありません。")
        sys.exit(1)
        
    predictions_df = pd.concat(predictions_list, ignore_index=True)
    
    # カラムの存在を確認
    logging.info(f"{len(predictions_df)}件の推論結果を生成")
    logging.info(f"出力カラム: {list(predictions_df.columns)}")
    logging.info(f"サンプル:\n{predictions_df.head(3)}")

    # --- 5. 結果の保存 ---
    output_dir = Path('data/predictions/parquet')
    # project_root基準の絶対パスに変換
    if not output_dir.is_absolute():
        output_dir = project_root / output_dir
        
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.output_filename:
        output_filename = args.output_filename
    else:
        output_filename = f"predictions_{target_dt.strftime('%Y%m%d')}.parquet"
        
    output_file = output_dir / output_filename
    
    # 日付関連カラムの追加（パーティション用）
    if 'race_date' in predictions_df.columns:
        predictions_df['race_date'] = pd.to_datetime(predictions_df['race_date'])
        predictions_df['year'] = predictions_df['race_date'].dt.year
        predictions_df['month'] = predictions_df['race_date'].dt.month
    
    try:
        # 単一ファイルで全カラムを保存
        predictions_df.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        logging.info(f"推論結果を保存しました: {output_file}")

    except Exception as e:
        logging.error(f"保存中にエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()