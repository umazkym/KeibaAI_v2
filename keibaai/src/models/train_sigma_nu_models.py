#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
σ (sigma) モデルとν (nu) モデルの訓練スクリプト

Usage:
    python train_sigma_nu_models.py \
        --training_window_months 48 \
        --output_dir ../data/models \
        --mu_predictions_path ../data/predictions/parquet/mu_predictions.parquet
"""

import argparse
import logging
from pathlib import Path
import sys
import json
import pickle  # Added: pickleモジュールのインポート

# プロジェクトルートをPYTHONPATHに追加（predict.pyと同様の修正）
project_root = Path(__file__).resolve().parent.parent.parent  # keibaaiディレクトリ
sys.path.insert(0, str(project_root / 'src'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import lightgbm as lgb
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

sys.path.append(str(project_root))

try:
    from src.utils.data_utils import load_parquet_data_by_date
except ImportError as e:
    print(f"エラー: 必要なモジュールのインポートに失敗しました: {e}")
    sys.exit(1)


# ロギング設定
log_dir = Path("keibaai/data/logs")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "sigma_nu_training.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_training_data(months: int):
    """
    データ読み込み
    - parsed/results/ に過去レースの結果が格納されている前提
    """
    logging.info(f"過去 {months} ヶ月分のレース結果データをロード中...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30*months)
    
    results_path = project_root / "data" / "parsed" / "parquet" / "races"
    
    if not results_path.exists():
        logging.error(f"レース結果ディレクトリが見つかりません: {results_path}")
        raise FileNotFoundError(f"ディレクトリが見つかりません: {results_path}")

    results_df = load_parquet_data_by_date(results_path, start_date, end_date, date_col='race_date')

    if results_df.empty:
        raise RuntimeError(f"期間 {start_date} - {end_date} のレース結果データが見つかりません")
        
    logging.info(f"{len(results_df)} 行のレース結果データをロードしました。")
    return results_df

def prepare_sigma_training(results_df: pd.DataFrame, mu_series: pd.Series):
    """
    σモデル用データ整備
    - 目的変数: squared_error = (actual_finish_metric - mu_pred)^2
    """
    df = results_df.copy()
    if 'horse_id' not in df.columns:
        raise ValueError("results_df に 'horse_id' が必要です")
        
    df['horse_id'] = df['horse_id'].astype(str)
    
    # μの予測値をマージ
    df['mu_pred'] = df['horse_id'].map(mu_series)
    
    # 目的変数 (performance_target) を定義
    # 仕様書 通り、ここでは 'finish_position' を使用
    df['performance_target'] = df['finish_position'].astype(float)
    
    # μ予測が欠損している行は学習から除外
    df = df.dropna(subset=['mu_pred', 'performance_target'])
    
    df['squared_error'] = (df['performance_target'] - df['mu_pred']) ** 2
    
    # 馬レベルで集約（馬ごとの分散を予測するため）
    horse_agg = df.groupby('horse_id').agg(
        sigma_target=('squared_error', 'mean')
    ).reset_index()
    
    # 特徴量の準備 (簡易版: 過去走の統計値を使用)
    horse_features = df.groupby('horse_id').agg(
        age=('age', 'first'),
        mean_finish_pos=('finish_position', 'mean'),
        std_finish_pos=('finish_position', 'std'),
        total_races=('race_id', 'count')
    ).reset_index()
    
    train_df = horse_features.merge(horse_agg, on='horse_id', how='inner')
    
    # 欠損埋め (stdが計算できない場合など)
    train_df = train_df.fillna(0)
    return train_df

def prepare_nu_training(results_df: pd.DataFrame):
    """
    νモデル用データ整備（レース荒れ度）
    - レースごとに実際の順位分散 / 着順の標準偏差 を計算し、レース特徴量から予測する
    """
    df = results_df.copy()
    
    # レースごとの実際の順位分散を計算
    race_variance = df.groupby('race_id').agg(
        nu_target=('finish_position', 'std')
    ).reset_index()
    
    # レース特徴量の作成（例）
    race_features = df.groupby('race_id').agg(
        distance_m=('distance_m', 'first'),
        track_surface=('track_surface', 'first'),
        track_condition=('track_condition', 'first'),
        weather=('weather', 'first'),
        head_count=('horse_number', 'max'), # 頭数
        avg_win_odds=('win_odds', 'mean'),
        std_win_odds=('win_odds', 'std'),
    ).reset_index()

    train_df = race_features.merge(race_variance, on='race_id', how='inner')
    
    # カテゴリダミー化
    categorical_cols = ['track_surface', 'track_condition', 'weather']
    train_df = pd.get_dummies(train_df, columns=categorical_cols, dummy_na=True)
    
    train_df = train_df.fillna(0)
    return train_df

def train_model_lgb(train_X, train_y, params=None):
    """
    LightGBM 回帰モデル 学習（シンプル実装）
    """
    if params is None:
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'n_estimators': 100
        }
    model = lgb.LGBMRegressor(**params)
    model.fit(train_X, train_y)
    return model

def main():
    parser = argparse.ArgumentParser(description="Train sigma and nu models (仕様書 13.4)")
    parser.add_argument("--training_window_months", type=int, default=12, help="学習に使用する過去月数")
    parser.add_argument("--output_dir", type=str, default="data/models/latest", help="モデルの保存先ディレクトリ")
    parser.add_argument(
        "--mu_predictions_path", 
        type=str, 
        default="data/predictions/parquet/mu_predictions.parquet", 
        help="μモデルの推論結果 (Parquet) のパス"
    )
    args = parser.parse_args()

    logging.info("σ/ν モデル学習スクリプト開始")
    logging.info(f"学習期間: 過去 {args.training_window_months} ヶ月")
    logging.info(f"モデル保存先: {args.output_dir}")

    try:
        # --- 1. μ推論結果のロード ---
        mu_preds_path = Path(args.mu_predictions_path)
        if not mu_preds_path.exists():
            logging.error(f"μの推論結果ファイルが見つかりません: {mu_preds_path}")
            logging.error("先にμモデルで推論を実行し、 --mu_predictions_path で指定してください。")
            sys.exit(1)
        
        logging.info(f"μ推論結果をロード中: {mu_preds_path}")
        mu_preds_df = pd.read_parquet(mu_preds_path)
        logging.info(f"{len(mu_preds_df)} 行のμ推論結果をロードしました。")
        
        # Series (index=horse_id, value=mu) を作成
        if 'horse_id' not in mu_preds_df.columns or 'mu' not in mu_preds_df.columns:
             logging.error("μ推論ファイルには 'horse_id' と 'mu' カラムが必要です。")
             sys.exit(1)
             
        # horse_idの重複に対応：同じ馬の複数レースのmuを平均
        # (同じ馬は同じmuを持つべきだが、安全のため平均を取る)
        mu_series = mu_preds_df.groupby('horse_id')['mu'].mean()
        logging.info(f"{len(mu_series)} 頭分のμ値を準備しました。")

        # --- 2. 学習データのロード ---
        results_df = load_training_data(args.training_window_months)
        
        # --- 3. σ モデル学習 ---
        logging.info("σモデルの学習データ準備中...")
        sigma_train_df = prepare_sigma_training(results_df, mu_series)
        sigma_feature_cols = [c for c in sigma_train_df.columns if c not in ('horse_id', 'sigma_target')]
        
        if not sigma_feature_cols:
            logging.error("σモデルの学習特徴量がありません。")
            sys.exit(1)
            
        X_sigma = sigma_train_df[sigma_feature_cols]
        y_sigma = sigma_train_df['sigma_target']
        X_train, X_val, y_train, y_val = train_test_split(X_sigma, y_sigma, test_size=0.2, random_state=42)
        
        logging.info("σモデル学習中...")
        sigma_model = train_model_lgb(X_train, y_train)
        y_pred = sigma_model.predict(X_val)
        logging.info(f"σモデル RMSE (Validation): {mean_squared_error(y_val, y_pred, squared=False):.6f}")

        # --- 4. ν モデル学習 ---
        logging.info("νモデルの学習データ準備中...")
        nu_train_df = prepare_nu_training(results_df)
        nu_feature_cols = [c for c in nu_train_df.columns if c not in ('race_id', 'nu_target')]
        
        if not nu_feature_cols:
            logging.error("νモデルの学習特徴量がありません。")
            sys.exit(1)
            
        X_nu = nu_train_df[nu_feature_cols]
        y_nu = nu_train_df['nu_target']
        Xn_train, Xn_val, yn_train, yn_val = train_test_split(X_nu, y_nu, test_size=0.2, random_state=42)
        
        logging.info("νモデル学習中...")
        nu_model = train_model_lgb(Xn_train, yn_train)
        yn_pred = nu_model.predict(Xn_val)
        logging.info(f"νモデル RMSE (Validation): {mean_squared_error(yn_val, yn_pred, squared=False):.6f}")

        # --- 5. 保存 ---
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # σモデル (SigmaEstimator クラスのラッパーではなく、LGBMモデル本体を保存)
        # (注: 仕様書 7.7.2 とは異なり、ここではモデル本体のみを保存)
        sigma_path = out_dir / "sigma_model.pkl"
        logging.info(f"σモデルを保存中: {sigma_path}")
        with open(sigma_path, "wb") as f:
            pickle.dump(sigma_model, f)
        # 特徴量リストも保存
        with open(out_dir / "sigma_features.json", "w") as f:
             json.dump(sigma_feature_cols, f)

        # νモデル (NuEstimator クラスのラッパーではなく、LGBMモデル本体を保存)
        nu_path = out_dir / "nu_model.pkl"
        logging.info(f"νモデルを保存中: {nu_path}")
        with open(nu_path, "wb") as f:
            pickle.dump(nu_model, f)
        # 特徴量リストも保存
        with open(out_dir / "nu_features.json", "w") as f:
             json.dump(nu_feature_cols, f)

        logging.info("σ/ν モデル学習が完了しました。")

    except Exception as e:
        logging.error(f"σ/ν モデル学習中に致命的なエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()