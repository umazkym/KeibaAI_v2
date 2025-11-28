#!/usr/bin/env python3
"""
μv2.5 モデル学習スクリプト (train_mu_v2_5_roi_opt.py)

【変更点 from v2.4】
- ROI最大化戦略 Phase 1: ROI主導型ハイパーパラメータチューニング
- Optunaの目的関数をAUCから「シミュレーションROI」に変更。
- 保存先ディレクトリを `keibaai/models/mu_v2_5` に変更。
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))
import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
import logging
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Tuple, List, Dict, Optional
from sklearn.metrics import roc_auc_score, log_loss, accuracy_score

# プロジェクト内のモジュールをインポート
from keibaai.src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_mu_v2_5.log')
    ]
)

class MuV25RoiTrainer:
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        # v2.5用のディレクトリ
        self.models_dir = Path('keibaai/models/mu_v2_5')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """必要なデータを読み込む"""
        logging.info("データを読み込んでいます...")
        
        # 1. 成績データ (Target & History)
        perf_path = self.data_dir / 'horses_performance/horses_performance_fixed.parquet'
        if not perf_path.exists():
            raise FileNotFoundError(f"{perf_path} が見つかりません。rebuild_dataset.py を実行してください。")
        
        df = pd.read_parquet(perf_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 2. レース情報 (補完用)
        races_path = self.data_dir / 'races/races.parquet'
        if races_path.exists():
            races_df = pd.read_parquet(races_path)
            cols_to_add = ['round_of_year', 'day_of_meeting', 'track_condition', 'venue', 'track_surface', 'win_odds']
            cols_to_merge = [c for c in cols_to_add if c not in df.columns]
            
            if cols_to_merge:
                logging.info(f"races.parquet から以下のカラムを補完します: {cols_to_merge}")
                df = df.merge(races_df[['race_id'] + cols_to_merge], on='race_id', how='left')
        
        # 3. 血統データ
        ped_path = self.data_dir / 'pedigrees/pedigrees.parquet'
        ped_df = pd.read_parquet(ped_path) if ped_path.exists() else pd.DataFrame()
        
        # 4. 馬プロファイル
        prof_path = self.data_dir / 'horses/horses.parquet'
        prof_df = pd.read_parquet(prof_path) if prof_path.exists() else pd.DataFrame()
        
        logging.info(f"データ読み込み完了: {len(df):,} レース結果")
        return df, ped_df, prof_df

    def generate_rolling_features(self, df: pd.DataFrame, ped_df: pd.DataFrame, prof_df: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
        """
        月次ローリングウィンドウで特徴量を生成する
        Target: 月単位
        History: その月の前日までの全データ
        """
        logging.info("=" * 50)
        logging.info("ローリング特徴量生成を開始します (v2.5 ROI Optimization Mode)")
        logging.info("=" * 50)
        
        # 特徴量エンジンの初期化
        config_path = Path('keibaai/configs/features.yaml')
        engine = FeatureEngine(config_path)
        
        # 期間設定
        start_date = df['race_date'].min()
        end_date = df['race_date'].max()
        
        # 2020年以降を学習対象とする
        training_start_date = pd.Timestamp('2020-01-01')
        if start_date > training_start_date:
            training_start_date = start_date
            
        logging.info(f"生成期間: {training_start_date.date()} 〜 {end_date.date()}")
        
        current_date = training_start_date.replace(day=1)
        feature_dfs = []
        
        while current_date <= end_date:
            next_month = current_date + relativedelta(months=1)
            
            # Target: 今月のデータ
            target_mask = (df['race_date'] >= current_date) & (df['race_date'] < next_month)
            target_df = df[target_mask].copy()
            
            if target_df.empty:
                current_date = next_month
                continue
                
            # History: 今月より前の全データ
            history_mask = (df['race_date'] < current_date)
            history_df = df[history_mask].copy()
            
            logging.info(f"[{current_date.strftime('%Y-%m')}] Target: {len(target_df):5,}件 | History: {len(history_df):7,}件")
            
            # 特徴量生成
            try:
                features = engine.generate_features(
                    shutuba_df=target_df,
                    results_history_df=history_df,
                    horse_profiles_df=prof_df,
                    pedigree_df=ped_df
                )
                
                # 目的変数とIDを保持
                cols_to_keep = ['race_id', 'horse_id', 'finish_position', 'race_date', 'win_odds']
                cols_to_merge = [c for c in cols_to_keep if c not in features.columns and c in target_df.columns]
                
                if cols_to_merge:
                    features = features.merge(target_df[cols_to_merge], on=['race_id', 'horse_id'], how='left')
                
                feature_dfs.append(features)
                
            except Exception as e:
                logging.error(f"Error in {current_date.strftime('%Y-%m')}: {e}")
            
            if dry_run:
                logging.info("Dry Run Mode: 1ヶ月分のみ生成して終了します")
                break

            current_date = next_month
            
        # 全期間のデータを結合
        if not feature_dfs:
            raise ValueError("特徴量が生成されませんでした。")
            
        full_df = pd.concat(feature_dfs, ignore_index=True)
        logging.info(f"全特徴量生成完了: {len(full_df):,} 行")
        
        # 保存 (v2.5用のファイル名)
        if not dry_run:
            save_path = self.models_dir / 'train_data_mu_v2_5.parquet'
            full_df.to_parquet(save_path)
            logging.info(f"学習データを保存しました: {save_path}")
        
        return full_df

    def train_model(self, df: pd.DataFrame, n_trials: int = 50):
        """LightGBMモデルの学習とチューニング (ROI最適化)"""
        logging.info("=" * 50)
        logging.info("モデル学習を開始します (v2.5 ROI Optimization)")
        logging.info("=" * 50)
        
        # 目的変数の作成 (1着予測)
        # finish_positionがInt64(nullable)の場合、比較結果がNAになることがあるためfillnaする
        df['target'] = (df['finish_position'] == 1).fillna(False).astype(int)
        
        # 学習・検証・テストデータの分割
        if n_trials <= 2: # Dry Run (n_trials is small)
            logging.info("Dry Run Mode: 時系列分割ではなくランダム分割を使用します")
            # 単純にランダム分割 (Train: 60%, Valid: 20%, Test: 20%)
            # 時系列性を無視するが、パイプラインの動作確認には十分
            np.random.seed(42)
            n = len(df)
            perm = np.random.permutation(n)
            train_idx = perm[:int(n*0.6)]
            valid_idx = perm[int(n*0.6):int(n*0.8)]
            test_idx = perm[int(n*0.8):]
            
            train_df = df.iloc[train_idx]
            valid_df = df.iloc[valid_idx]
            test_df = df.iloc[test_idx]
        else:
            # 通常の時系列分割
            train_mask = df['race_date'] < '2023-01-01'
            valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
            test_mask = df['race_date'] >= '2024-01-01'
            
            train_df = df[train_mask]
            valid_df = df[valid_mask]
            test_df = df[test_mask]
        
        logging.info(f"Train: {len(train_df):,} ({train_df['target'].mean():.1%})")
        logging.info(f"Valid: {len(valid_df):,} ({valid_df['target'].mean():.1%})")
        logging.info(f"Test : {len(test_df):,} ({test_df['target'].mean():.1%})")
        
        # 特徴量カラムの特定
        exclude_cols = [
            'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
            'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
            'race_name', 'horse_name', 'jockey_name', 'trainer_name',
            # Leakage columns
            'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
            'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
            'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
            'passing_order_3', 'passing_order_4', 'position_change_1_2', 
            'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
        ]
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        # 数値型のみ残す
        for col in feature_cols:
            if df[col].dtype == 'object':
                df[col] = df[col].astype('category')
                
        logging.info(f"使用特徴量: {len(feature_cols)}個")
        
        # LightGBMデータセット
        lgb_train = lgb.Dataset(train_df[feature_cols], label=train_df['target'])
        lgb_valid = lgb.Dataset(valid_df[feature_cols], label=valid_df['target'], reference=lgb_train)
        
        # Optunaによるチューニング (ROI最適化)
        def objective(trial):
            params = {
                'objective': 'binary',
                'metric': 'auc', # 学習中のモニタリングはAUCのままでOK
                'verbosity': -1,
                'boosting_type': 'gbdt',
                'feature_pre_filter': False,
                'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
                'num_leaves': trial.suggest_int('num_leaves', 2, 256),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            }
            
            model = lgb.train(
                params,
                lgb_train,
                valid_sets=[lgb_train, lgb_valid],
                num_boost_round=1000,
                callbacks=[
                    lgb.early_stopping(stopping_rounds=50),
                    lgb.log_evaluation(period=0) # ログ出力を抑制
                ]
            )
            
            # --- ROI計算ロジック ---
            # Validでの予測
            preds = model.predict(valid_df[feature_cols])
            valid_df_copy = valid_df.copy() # 警告回避のためコピー
            valid_df_copy['pred_prob'] = preds

            # レースごとの予測1位を特定
            valid_df_copy['rank_pred'] = valid_df_copy.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')

            # 予測1位の馬だけを購入した場合のシミュレーション
            bet_df = valid_df_copy[valid_df_copy['rank_pred'] == 1].copy()

            # オッズがないデータは除外
            bet_df = bet_df.dropna(subset=['win_odds'])

            # 的中判定と払い戻し計算
            hits = bet_df[bet_df['target'] == 1]
            return_amount = hits['win_odds'].sum()
            bet_amount = len(bet_df)

            # ROI計算
            roi = return_amount / bet_amount if bet_amount > 0 else 0
            
            return roi  # AUCではなくROIを返す

        logging.info("Optunaチューニングを開始します (Target: ROI)...")
        # ROIは最大化したいので direction='maximize'
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)
        
        logging.info(f"Best trial: ROI={study.best_value:.2%}")
        logging.info(f"Best params: {study.best_params}")
        
        # 最適パラメータで再学習
        best_params = study.best_params
        best_params.update({
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1
        })
        
        logging.info("最適パラメータで最終モデルを学習します...")
        model = lgb.train(
            best_params,
            lgb_train,
            valid_sets=[lgb_train, lgb_valid],
            num_boost_round=2000,
            callbacks=[
                lgb.early_stopping(stopping_rounds=100),
                lgb.log_evaluation(period=100)
            ]
        )
        
        # モデル保存
        model_path = self.models_dir / 'mu_v2_5_model.txt'
        model.save_model(str(model_path))
        
        with open(self.models_dir / 'mu_v2_5_model.pkl', 'wb') as f:
            pickle.dump(model, f)
            
        logging.info(f"モデルを保存しました: {model_path}")
        
        # 評価
        self.evaluate_model(model, test_df, feature_cols)
        
        return model

    def evaluate_model(self, model, test_df: pd.DataFrame, feature_cols: List[str]):
        """テストデータでの評価"""
        logging.info("=" * 50)
        logging.info("モデル評価 (Test Data: 2024)")
        logging.info("=" * 50)
        
        preds = model.predict(test_df[feature_cols])
        test_df = test_df.copy()
        test_df['pred_prob'] = preds
        
        auc = roc_auc_score(test_df['target'], preds)
        logloss = log_loss(test_df['target'], preds)
        
        logging.info(f"Test AUC: {auc:.4f}")
        logging.info(f"Test LogLoss: {logloss:.4f}")
        
        if 'win_odds' in test_df.columns:
            test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            bet_df = test_df[test_df['rank_pred'] == 1].copy()
            
            hits = bet_df[bet_df['target'] == 1]
            accuracy = len(hits) / len(bet_df)
            
            bet_df = bet_df.dropna(subset=['win_odds'])
            hits = bet_df[bet_df['target'] == 1]
            
            return_amount = hits['win_odds'].sum() * 100
            bet_amount = len(bet_df) * 100
            roi = return_amount / bet_amount if bet_amount > 0 else 0
            
            logging.info(f"単勝的中率 (Top1): {accuracy:.2%}")
            logging.info(f"単勝回収率 (Top1): {roi:.2%} ({return_amount:,.0f}/{bet_amount:,.0f})")
            
        # 特徴量重要度
        importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importance(importance_type='gain')
        }).sort_values('importance', ascending=False)
        
        logging.info("\nFeature Importance (Top 20):")
        logging.info(importance.head(20).to_string())
        
        importance.to_csv(self.models_dir / 'feature_importance.csv', index=False)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Train μ v2.5 model with ROI optimization')
    parser.add_argument('--n_trials', type=int, default=50, help='Number of Optuna trials')
    parser.add_argument('--dry-run', action='store_true', help='Run with small data/trials for testing')
    args = parser.parse_args()

    trainer = MuV25RoiTrainer()
    
    # 1. データ読み込み
    df, ped_df, prof_df = trainer.load_data()
    
    # 2. 特徴量生成
    # v2.5用のデータファイルが存在するか確認
    # (v2.4と同じ特徴量セットなら流用可能だが、念のため分離しておく)
    train_data_path = trainer.models_dir / 'train_data_mu_v2_5.parquet'
    
    if train_data_path.exists():
        logging.info(f"既存の学習データを使用します: {train_data_path}")
        full_df = pd.read_parquet(train_data_path)
    else:
        # v2.4のデータがあればそれをコピーして使うのも手だが、ここでは再生成ロジックを通す
        # (ただし時間がかかるので、v2.4のデータがあればそれを読み込むように変更してもよい)
        # 今回は安全のため再生成フローとする
        logging.info("新規に特徴量を生成します...")
        full_df = trainer.generate_rolling_features(df, ped_df, prof_df, dry_run=args.dry_run)
    
    # Dry Run用: データを小さくする
    if args.dry_run:
        logging.info("Dry Run Mode: データを縮小して実行します")
        sample_size = 10000
        if len(full_df) > sample_size:
            full_df = full_df.sample(n=sample_size, random_state=42)
        else:
            logging.info(f"データ件数({len(full_df)})がsample_size({sample_size})未満のため、全データを使用します")
        args.n_trials = 2

    # 3. 学習
    trainer.train_model(full_df, n_trials=args.n_trials)
