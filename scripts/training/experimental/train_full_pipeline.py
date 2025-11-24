import argparse
import os
import pandas as pd
import numpy as np
import logging
import joblib
from datetime import datetime
from pathlib import Path
import yaml

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.modules.models.model_train import MuEstimator
from keibaai.src.modules.models.sigma_estimator import SigmaEstimator
from keibaai.src.modules.models.nu_estimator import NuEstimator
from keibaai.src.features.feature_engine import FeatureEngine

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description='Train Full KeibaAI Pipeline (Mu, Sigma, Nu)')
    parser.add_argument('--features_dir', type=str, required=True, help='Path to features parquet directory')
    parser.add_argument('--output_dir', type=str, required=True, help='Directory to save trained models')
    parser.add_argument('--start_date', type=str, default='2020-01-01', help='Training start date')
    parser.add_argument('--end_date', type=str, default='2023-12-31', help='Training end date')
    parser.add_argument('--config', type=str, default='keibaai/configs/models.yaml', help='Path to model config')
    
    parser.add_argument('--tune', action='store_true', help='Run hyperparameter tuning with Optuna')
    parser.add_argument('--ensemble', action='store_true', help='Use Stacking Ensemble')
    
    args = parser.parse_args()
    
    # 出力ディレクトリ作成
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 設定読み込み
    config = load_config(args.config) if os.path.exists(args.config) else {}
    
    logger.info("Loading data...")
    # ここでは簡易的にParquetを一括読み込みする実装（実際はパーティション読み込み推奨）
    # 注意: メモリ効率のため、必要カラムのみ読み込むなどの工夫が必要
    try:
        # Parquetファイルを検索して結合
        parquet_files = list(Path(args.features_dir).glob('**/*.parquet'))
        if not parquet_files:
            logger.error(f"No parquet files found in {args.features_dir}")
            return
            
        dfs = []
        for p in parquet_files:
            try:
                dfs.append(pd.read_parquet(p))
            except Exception as e:
                logger.warning(f"Skipping {p}: {e}")
                
        if not dfs:
            logger.error("Failed to load any parquet files.")
            return
            
        
        df = pd.concat(dfs, ignore_index=True)
        
        # 重複削除（念のため）
        # 同じレース・同じ馬のデータが複数ある場合は削除
        if 'horse_id' in df.columns:
            df = df.drop_duplicates(subset=['race_id', 'horse_id'])
        else:
            df = df.drop_duplicates(subset=['race_id', 'horse_number'])
            
        # 日付フィルタ
        df['race_date'] = pd.to_datetime(df['race_date'])
        mask = (df['race_date'] >= args.start_date) & (df['race_date'] <= args.end_date)
        train_df = df[mask].copy()
        
        logger.info(f"Loaded {len(train_df)} samples for training.")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return

    # 欠損値処理（簡易）
    train_df = train_df.dropna(subset=['finish_time_seconds'])
    
    # 特徴量カラムの特定（数値型のみ、IDやターゲットを除く）
    exclude_cols = [
        'race_id', 'horse_id', 'race_date', 'finish_time_seconds', 
        'finish_position', 'horse_name', 'jockey_name', 'trainer_name',
        'owner_name', 'race_name'
    ]
    feature_cols = [c for c in train_df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(train_df[c])]
    
    # データをレースIDでソート（LightGBM Rankerのために必須）
    train_df = train_df.sort_values('race_id').reset_index(drop=True)
    
    X = train_df[feature_cols]
    y = train_df['finish_time_seconds']
    # ソート済みなので、groupby(sort=False)で順序を維持したままサイズを取得
    groups = train_df.groupby('race_id', sort=False).size().values
    race_ids = train_df['race_id']
    
    # ==========================================
    # 1. Mu Model Training
    # ==========================================
    logger.info("Training Mu Model...")
    
    mu_params = {}
    
    if args.tune:
        from keibaai.src.modules.models.optuna_tuner import OptunaTuner
        tuner = OptunaTuner(n_trials=20) # 試行回数は調整
        
        # Ranker Tuning
        logger.info("Tuning Ranker...")
        # Rankerのターゲットは着順の逆数などにする必要があるが、ここでは簡易的にyをそのまま渡す
        # 実際はOptunaTuner内で適切に処理するか、ここで変換して渡す
        rank_target = -y
        best_ranker_params = tuner.tune_lgbm_ranker(X, rank_target, groups)
        
        # Regressor Tuning
        logger.info("Tuning Regressor...")
        best_regressor_params = tuner.tune_lgbm_regressor(X, y)
        
        mu_params = {
            'ranker_params': best_ranker_params,
            'regressor_params': best_regressor_params
        }
        
    mu_model = MuEstimator(**mu_params)
    mu_model.fit(X, y, group=groups)
         
    # 保存
    joblib.dump(mu_model, os.path.join(args.output_dir, 'mu_model.pkl'))
    
    # Sigma/Nu学習用の予測値生成
    mu_pred = mu_model.predict(X)
    
    # ==========================================
    # 2. Sigma Model Training
    # ==========================================
    logger.info("Training Sigma Model...")
    sigma_model = SigmaEstimator()
    sigma_model.fit(X, y, mu_pred)
    
    # 保存
    sigma_model.save_model(os.path.join(args.output_dir, 'sigma_model.txt'))
    
    # Nu学習用の予測値生成
    sigma_pred = sigma_model.predict(X)
    
    # ==========================================
    # 3. Nu Model Training
    # ==========================================
    logger.info("Training Nu Model...")
    
    # レース特徴量の作成（レースごとに1行）
    # 数値カラムの平均をとる簡易実装
    race_features_df = train_df.groupby('race_id')[feature_cols].mean().reset_index()
    
    nu_model = NuEstimator()
    nu_model.fit(race_features_df, y, mu_pred, sigma_pred, race_ids)
    
    # 保存
    nu_model.save_model(os.path.join(args.output_dir, 'nu_model.txt'))
    
    logger.info("All models trained and saved successfully.")

if __name__ == '__main__':
    main()
