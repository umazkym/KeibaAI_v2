#!/usr/bin/env python3
"""
v2.6 モデル比較検証スクリプト (compare_models_v26.py)

目的:
v2.6 (Odds-Weighted) の優位性を、他のアプローチと比較して定量的に評価する。

比較対象:
1. v2.6 Weighted (System A): log1p(odds) 荷重 LightGBM Binary
2. v2.5 Baseline: 荷重なし LightGBM Binary
3. v2.x Ranker: LightGBM Ranker (Lambdarank) - 旧アプローチ

評価指標:
- Test AUC (Binary系) / NDCG (Ranker系)
- ROI (Top1, 10-50倍など)
- Hit Rate (的中率)
"""

import sys
from pathlib import Path

# プロジェクトルートの設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
from typing import Dict, Any, List
from sklearn.metrics import roc_auc_score, log_loss

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ModelComparator:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.output_dir = project_root / 'keibaai' / 'models' / 'comparison_v26'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def load_data(self):
        """学習済み特徴量データをロード"""
        if not self.data_path.exists():
            raise FileNotFoundError(f"{self.data_path} が見つかりません。train_mu_v2_6_weighted.py を一度実行して特徴量を生成してください。")
        
        logging.info("データをロード中...")
        df = pd.read_parquet(self.data_path)
        
        # 2024年以降をテストデータとする (v2.6基準)
        train_mask = df['race_date'] < '2024-01-01'
        test_mask = df['race_date'] >= '2024-01-01'
        
        self.train_df = df[train_mask].copy()
        self.test_df = df[test_mask].copy()
        
        logging.info(f"Train: {len(self.train_df):,} records")
        logging.info(f"Test : {len(self.test_df):,} records")
        
        # 特徴量カラムの特定
        exclude_cols = [
            'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
            'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
            'race_name', 'horse_name', 'jockey_name', 'trainer_name',
            'weight',
            'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
            'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
            'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
            'passing_order_3', 'passing_order_4', 'position_change_1_2', 
            'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
        ]
        self.feature_cols = [c for c in df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]
        logging.info(f"特徴量数: {len(self.feature_cols)}")
        
        # ターゲットと重みの準備
        self._prepare_targets(self.train_df)
        self._prepare_targets(self.test_df)

    def _prepare_targets(self, df: pd.DataFrame):
        """ターゲット変数と重みを作成"""
        df['target_binary'] = (df['finish_position'] == 1).astype(int)
        
        # Ranker用: 1着=最大, 他=0 (簡易的) or 逆順位
        # ここでは簡易的に 1/着順 をスコアとする
        df['target_rank'] = 1.0 / df['finish_position']
        
        # 重み (v2.6 System A)
        df['weight_v26'] = np.log1p(df['win_odds'].fillna(0) + 1.0)
        
        # 重みなし (Baseline)
        df['weight_none'] = 1.0

    def train_v26_weighted(self) -> lgb.Booster:
        """1. v2.6 Weighted (System A)"""
        logging.info("\n--- Training Model 1: v2.6 Weighted (System A) ---")
        
        # パラメータ (v2.6 デフォルトに近い設定)
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 31,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'seed': 42
        }
        
        lgb_train = lgb.Dataset(
            self.train_df[self.feature_cols], 
            label=self.train_df['target_binary'],
            weight=self.train_df['weight_v26']
        )
        
        model = lgb.train(
            params,
            lgb_train,
            num_boost_round=500  # 比較用なので固定
        )
        return model

    def train_v25_baseline(self) -> lgb.Booster:
        """2. v2.5 Baseline (Unweighted)"""
        logging.info("\n--- Training Model 2: v2.5 Baseline (Unweighted) ---")
        
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 31,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'seed': 42
        }
        
        lgb_train = lgb.Dataset(
            self.train_df[self.feature_cols], 
            label=self.train_df['target_binary'],
            weight=self.train_df['weight_none'] # 重みなし
        )
        
        model = lgb.train(
            params,
            lgb_train,
            num_boost_round=500
        )
        return model

    def train_ranker(self) -> lgb.Booster:
        """3. Ranker (Lambdarank)"""
        logging.info("\n--- Training Model 3: Ranker (Lambdarank) ---")
        
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 31,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'seed': 42,
            'label_gain': [0, 1] + [0]*20 # 1位に利得
        }
        
        # グループ情報が必要
        # データをレースID順にソートしておく必要がある
        train_sorted = self.train_df.sort_values('race_id')
        groups = train_sorted.groupby('race_id').size().values
        
        # Rankerのターゲットは relevance score
        # ここでは 1着=1, 他=0 のシンプルなrelevanceを使う（NDCG@1重視）
        targets = (train_sorted['finish_position'] == 1).astype(int)
        
        lgb_train = lgb.Dataset(
            train_sorted[self.feature_cols], 
            label=targets,
            group=groups
        )
        
        model = lgb.train(
            params,
            lgb_train,
            num_boost_round=500
        )
        return model

    def evaluate(self, models: Dict[str, lgb.Booster]):
        """全モデルの評価と比較"""
        logging.info("\n=== Evaluation Results (Test Data: 2024-) ===")
        results = []
        
        test_base = self.test_df.copy()
        
        for name, model in models.items():
            # 予測
            preds = model.predict(test_base[self.feature_cols])
            test_df = test_base.copy()
            test_df['pred_prob'] = preds
            
            # Binary系の指標
            try:
                auc = roc_auc_score(test_df['target_binary'], preds)
            except:
                auc = 0.0 # Rankerだとそのまま計算できない場合があるが、予測値がスコアならOK
            
            # ランキング作成
            test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            
            # ROI計算
            # 1. Pure Top1 Strategy
            bet_df_top1 = test_df[test_df['rank_pred'] == 1].dropna(subset=['win_odds'])
            hits_top1 = bet_df_top1[bet_df_top1['finish_position'] == 1]
            roi_top1 = hits_top1['win_odds'].sum() / len(bet_df_top1) if len(bet_df_top1) > 0 else 0
            acc_top1 = len(hits_top1) / len(bet_df_top1) if len(bet_df_top1) > 0 else 0
            
            # 2. Odds 10-50x Strategy (v2.6 推奨)
            bet_df_strat = bet_df_top1[
                (bet_df_top1['win_odds'] >= 10.0) & 
                (bet_df_top1['win_odds'] < 50.0)
            ]
            hits_strat = bet_df_strat[bet_df_strat['finish_position'] == 1]
            roi_strat = hits_strat['win_odds'].sum() / len(bet_df_strat) if len(bet_df_strat) > 0 else 0
            acc_strat = len(hits_strat) / len(bet_df_strat) if len(bet_df_strat) > 0 else 0
            count_strat = len(bet_df_strat)
            
            results.append({
                'Model': name,
                'AUC': auc,
                'Top1 ROI': roi_top1,
                'Top1 Hit%': acc_top1,
                'Strat ROI (10-50x)': roi_strat,
                'Strat Hit%': acc_strat,
                'Strat Bets': count_strat
            })
            
        # 結果表示
        res_df = pd.DataFrame(results)
        print("\n")
        print(res_df.to_string(index=False, float_format=lambda x: f"{x:.4f}" if isinstance(x, float) else f"{x}"))
        print("\n")
        
        # MarkDown形式でも保存
        res_df.to_markdown(self.output_dir / 'comparison_results.md', index=False, floatfmt=".4f")
        logging.info(f"結果を保存しました: {self.output_dir / 'comparison_results.md'}")

def main():
    # データパス (train_mu_v2_6_weighted.py で生成されたファイルを想定)
    data_path = project_root / 'keibaai' / 'models' / 'mu_v2_6' / 'train_data_mu_v2_6.parquet'
    
    comparator = ModelComparator(data_path)
    comparator.load_data()
    
    models = {}
    models['v2.5 Baseline'] = comparator.train_v25_baseline()
    models['v2.6 Weighted'] = comparator.train_v26_weighted()
    models['Lambdarank'] = comparator.train_ranker()
    
    comparator.evaluate(models)

if __name__ == "__main__":
    main()
