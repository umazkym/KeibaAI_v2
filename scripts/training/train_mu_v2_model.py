#!/usr/bin/env python3
"""
μv2.0 モデル学習スクリプト (train_mu_v2_model.py)

【重要】リーク防止のための厳格なデータ分割
- 特徴量生成において、未来のデータ（予測対象レース以降の結果）を参照することは「データリーク」であり、
  モデルの性能を不当に高く見せてしまう原因となる。
- 本スクリプトでは、これを防ぐために「月次ローリングウィンドウ方式」を採用する。
  - Target: ある月（例: 2022年5月）の全レース
  - History: その月の**前月末まで**（例: 2022年4月30日まで）の過去全レース
- これにより、予測時点（レース前）に入手可能な情報のみを使って特徴量が生成されることを保証する。
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent
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
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from keibaai.src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_mu_v2.log')
    ]
)

class MuV2Trainer:
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        self.models_dir = Path('keibaai/models/mu_v2')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """必要なデータを読み込む"""
        logging.info("データを読み込んでいます...")
        
        # 1. 成績データ (Target & History)
        # rebuild_dataset.py で作成した完全版データを使用
        perf_path = self.data_dir / 'horses_performance/horses_performance_fixed.parquet'
        if not perf_path.exists():
            raise FileNotFoundError(f"{perf_path} が見つかりません。rebuild_dataset.py を実行してください。")
        
        df = pd.read_parquet(perf_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 2. 血統データ
        ped_path = self.data_dir / 'pedigrees/pedigrees.parquet'
        ped_df = pd.read_parquet(ped_path) if ped_path.exists() else pd.DataFrame()
        
        # 3. 馬プロファイル
        prof_path = self.data_dir / 'horses/horses.parquet'
        prof_df = pd.read_parquet(prof_path) if prof_path.exists() else pd.DataFrame()
        
        logging.info(f"データ読み込み完了: {len(df):,} レース結果")
        return df, ped_df, prof_df

    def generate_rolling_features(self, df: pd.DataFrame, ped_df: pd.DataFrame, prof_df: pd.DataFrame) -> pd.DataFrame:
        """
        月次ローリングウィンドウで特徴量を生成する
        Target: 月単位
        History: その月の前日までの全データ
        """
        logging.info("=" * 50)
        logging.info("ローリング特徴量生成を開始します (リーク防止モード)")
        logging.info("=" * 50)
        
        # 特徴量エンジンの初期化
        config_path = Path('keibaai/configs/features.yaml')
        engine = FeatureEngine(config_path)
        
        # 期間設定
        start_date = df['race_date'].min()
        end_date = df['race_date'].max()
        
        # 2020年以降を学習対象とする（それ以前はHistoryとしてのみ使用）
        # データ量に応じて調整可能
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
            # 【重要】ここがリーク防止の肝。Target期間のデータは一切Historyに含まれない。
            history_mask = (df['race_date'] < current_date)
            history_df = df[history_mask].copy()
            
            logging.info(f"[{current_date.strftime('%Y-%m')}] Target: {len(target_df):5,}件 | History: {len(history_df):7,}件")
            
            # 特徴量生成
            # generate_featuresは内部でmu_v2_featuresなどを呼び出す
            # target_dfに対して、history_dfを使って特徴量を付与する
            try:
                features = engine.generate_features(
                    shutuba_df=target_df,
                    results_history_df=history_df,
                    horse_profiles_df=prof_df,
                    pedigree_df=ped_df
                )
                
                # 目的変数とIDを保持
                # FeatureEngineはIDと特徴量を返すが、目的変数(finish_position等)は除外される場合があるため
                # 必要なカラムをtarget_dfから結合
                cols_to_keep = ['race_id', 'horse_id', 'finish_position', 'race_date', 'win_odds']
                # featuresに既に含まれている場合は除外
                cols_to_merge = [c for c in cols_to_keep if c not in features.columns and c in target_df.columns]
                
                if cols_to_merge:
                    features = features.merge(target_df[cols_to_merge], on=['race_id', 'horse_id'], how='left')
                
                feature_dfs.append(features)
                
            except Exception as e:
                logging.error(f"Error in {current_date.strftime('%Y-%m')}: {e}")
            
            current_date = next_month
            
        # 全期間のデータを結合
        if not feature_dfs:
            raise ValueError("特徴量が生成されませんでした。")
            
        full_df = pd.concat(feature_dfs, ignore_index=True)
        logging.info(f"全特徴量生成完了: {len(full_df):,} 行")
        
        # 保存
        save_path = self.models_dir / 'train_data_mu_v2.parquet'
        full_df.to_parquet(save_path)
        logging.info(f"学習データを保存しました: {save_path}")
        
        return full_df

    def train_model(self, df: pd.DataFrame, n_trials: int = 50):
        """LightGBMモデルの学習とチューニング"""
        logging.info("=" * 50)
        logging.info("モデル学習を開始します")
        logging.info("=" * 50)
        
        # 目的変数の作成 (1着予測)
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        # 学習・検証・テストデータの分割
        # Train: ~2022
        # Valid: 2023
        # Test: 2024
        
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
        # IDや目的変数、日付などを除外
        exclude_cols = [
            'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
            'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
            'race_name', 'horse_name', 'jockey_name', 'trainer_name'
        ]
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        # 数値型のみ残す（カテゴリカルはLightGBMが扱うが、object型は変換が必要）
        # ここでは簡易的に数値型のみとするか、category型に変換する
        for col in feature_cols:
            if df[col].dtype == 'object':
                df[col] = df[col].astype('category')
                
        logging.info(f"使用特徴量: {len(feature_cols)}個")
        logging.info(f"特徴量リスト: {feature_cols[:10]} ...")
        
        # LightGBMデータセット
        lgb_train = lgb.Dataset(train_df[feature_cols], label=train_df['target'])
        lgb_valid = lgb.Dataset(valid_df[feature_cols], label=valid_df['target'], reference=lgb_train)
        
        # Optunaによるチューニング
        def objective(trial):
            params = {
                'objective': 'binary',
                'metric': 'auc',
                'verbosity': -1,
                'boosting_type': 'gbdt',
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
            
            # ValidでのAUCを返す
            preds = model.predict(valid_df[feature_cols])
            auc = roc_auc_score(valid_df['target'], preds)
            return auc

        logging.info("Optunaチューニングを開始します...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)
        
        logging.info(f"Best trial: AUC={study.best_value:.4f}")
        logging.info(f"Best params: {study.best_params}")
        
        # 最適パラメータで再学習
        best_params = study.best_params
        best_params.update({
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1
        })
        
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
        model_path = self.models_dir / 'mu_v2_model.txt'
        model.save_model(str(model_path))
        
        # Pickle保存（便利のため）
        with open(self.models_dir / 'mu_v2_model.pkl', 'wb') as f:
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
        test_df['pred_prob'] = preds
        
        auc = roc_auc_score(test_df['target'], preds)
        logloss = log_loss(test_df['target'], preds)
        
        logging.info(f"Test AUC: {auc:.4f}")
        logging.info(f"Test LogLoss: {logloss:.4f}")
        
        # 回収率シミュレーション
        # 単勝オッズがある場合のみ
        if 'win_odds' in test_df.columns:
            # 予測確率が高い順にソート
            # レースごとに1位を予測
            
            # レースごとの予測1位を取得
            test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            
            # 予測1位の馬を買った場合
            bet_df = test_df[test_df['rank_pred'] == 1].copy()
            
            hits = bet_df[bet_df['target'] == 1]
            accuracy = len(hits) / len(bet_df)
            
            # 回収率
            # 欠損オッズは除外
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
        
        # 保存
        importance.to_csv(self.models_dir / 'feature_importance.csv', index=False)

if __name__ == "__main__":
    trainer = MuV2Trainer()
    
    # 1. データ読み込み
    df, ped_df, prof_df = trainer.load_data()
    
    # 2. 特徴量生成 (時間がかかるため、既に保存されていればスキップ可能にするロジックも検討)
    # 今回は毎回生成する（安全のため）
    train_data_path = trainer.models_dir / 'train_data_mu_v2.parquet'
    if train_data_path.exists():
        logging.info(f"既存の学習データを使用します: {train_data_path}")
        full_df = pd.read_parquet(train_data_path)
    else:
        full_df = trainer.generate_rolling_features(df, ped_df, prof_df)
    
    # 3. 学習
    trainer.train_model(full_df, n_trials=30) # トライアル数は調整
