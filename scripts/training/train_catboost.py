#!/usr/bin/env python3
"""
Phase 3: CatBoostモデル訓練とEdge Score分析

【計画書】
> Phase 3: メインモデル改良
> - CatBoost（カテゴリ特徴量）
> - 騎手・調教師のカテゴリ

【CatBoostの利点】
1. カテゴリ変数をネイティブ処理（ラベルエンコード不要）
2. 順序ブースティングでリーク防止
3. LightGBMとは異なる予測 → アンサンブル可能性
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
from catboost import CatBoostRanker, Pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class CatBoostEdgeAnalyzer:
    """CatBoostモデル訓練とEdge Score分析"""
    
    def __init__(self):
        self.output_dir = Path('keibaai/models/catboost_v1')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # カテゴリ変数のリスト
        self.cat_features = [
            'jockey_id',
            'trainer_id',
            'venue',
            'track_surface',
            'track_condition',
        ]
    
    def load_data(self):
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        base_data['race_id'] = base_data['race_id'].astype(str)
        base_data['horse_id'] = base_data['horse_id'].astype(str)
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        
        return base_data, races_df, returns_df
    
    def prepare_features(self, df, races_df):
        """特徴量準備"""
        # LightGBMの特徴量名を読み込み
        with open('keibaai/models/mu_v11_0/feature_names.json', 'r', encoding='utf-8') as f:
            lgbm_features = json.load(f)
        
        # 数値特徴量
        numeric_features = [f for f in lgbm_features if f not in self.cat_features]
        
        # カテゴリ変数をracesからマージ
        races_subset = races_df[['race_id', 'horse_id', 'venue', 'track_surface', 'track_condition', 
                                  'finish_position', 'horse_number', 'win_odds']].drop_duplicates(
            subset=['race_id', 'horse_id']
        )
        
        for col in ['venue', 'track_surface', 'track_condition', 'finish_position', 'horse_number', 'win_odds']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # カテゴリ変数を文字列に変換
        for cat in self.cat_features:
            if cat in df.columns:
                df[cat] = df[cat].astype(str).fillna('unknown')
            else:
                df[cat] = 'unknown'
        
        # 数値特徴量の欠損埋め
        for f in numeric_features:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        # ターゲット（順位ラベル）
        df['label'] = -df['finish_position'].fillna(99)  # 小さい方が良い → 負にする
        
        # 使用特徴量のリスト
        feature_cols = [f for f in numeric_features if f in df.columns]
        feature_cols.extend([f for f in self.cat_features if f in df.columns])
        
        return df, feature_cols
    
    def train_catboost(self, df, feature_cols):
        """CatBoostモデル訓練"""
        logging.info("\n" + "=" * 60)
        logging.info("CatBoost訓練")
        logging.info("=" * 60)
        
        # Train/Valid分割（時系列）
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        
        logging.info(f"  Train: {len(train_df):,}件 ({train_df['race_id'].nunique()}レース)")
        logging.info(f"  Valid: {len(valid_df):,}件 ({valid_df['race_id'].nunique()}レース)")
        
        # カテゴリ変数のインデックス
        cat_indices = [feature_cols.index(c) for c in self.cat_features if c in feature_cols]
        
        # グループ情報（レース単位）
        train_groups = train_df.groupby('race_id').size().values
        valid_groups = valid_df.groupby('race_id').size().values
        
        # Pool作成
        train_pool = Pool(
            data=train_df[feature_cols],
            label=train_df['label'],
            group_id=train_df['race_id'],
            cat_features=cat_indices
        )
        
        valid_pool = Pool(
            data=valid_df[feature_cols],
            label=valid_df['label'],
            group_id=valid_df['race_id'],
            cat_features=cat_indices
        )
        
        # モデル訓練
        model = CatBoostRanker(
            iterations=200,
            learning_rate=0.1,
            depth=6,
            loss_function='PairLogit',  # YetiRankではなくPairLogit
            task_type='CPU',
            random_seed=42,
            verbose=50
        )
        
        logging.info("  訓練開始...")
        model.fit(
            train_pool,
            eval_set=valid_pool
        )
        
        # モデル保存
        model.save_model(str(self.output_dir / 'catboost_ranker.cbm'))
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(feature_cols, f, ensure_ascii=False)
        
        logging.info(f"  モデル保存: {self.output_dir}")
        
        return model
    
    def evaluate_edge_score(self, model, df, feature_cols, returns_df):
        """Edge Score評価"""
        logging.info("\n" + "=" * 60)
        logging.info("CatBoost Edge Score評価")
        logging.info("=" * 60)
        
        # Test期間
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        # 予測
        preds = model.predict(test_df[feature_cols])
        test_df['raw_score'] = preds
        test_df['rank_in_race'] = test_df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        test_df['model_prob'] = test_df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
        # オッズ暗示確率
        test_df['odds_prob'] = 1 / test_df['win_odds'].clip(lower=1.1)
        test_df['odds_prob_normalized'] = test_df.groupby('race_id')['odds_prob'].transform(lambda x: x / x.sum() * 0.8)
        
        # Edge Score
        test_df['edge_score'] = test_df['model_prob'] / test_df['odds_prob_normalized'].clip(lower=0.01)
        
        # 配当データ
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
        # 予測1位を取得
        top1 = test_df[test_df['rank_in_race'] == 1].copy()
        top1 = top1.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        results = []
        
        logging.info("\n【CatBoost Test期間ROI】")
        for edge_thresh in [1.0, 1.2, 1.5, 2.0]:
            subset = top1[top1['edge_score'] >= edge_thresh]
            
            if len(subset) < 30:
                continue
            
            payout = subset['tansho_payout'].fillna(0).sum()
            roi = payout / (len(subset) * 100)
            hits = (subset['tansho_payout'] > 0).sum()
            hit_rate = hits / len(subset)
            
            status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
            logging.info(f"  Edge≥{edge_thresh:.1f} | ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
            
            results.append({
                'edge_thresh': edge_thresh,
                'n_bets': len(subset),
                'hit_rate': hit_rate,
                'roi': roi,
            })
        
        # LightGBMとの比較
        logging.info("\n【LightGBM予測1位との比較】")
        
        # 1番人気選択率
        pop1_rate = (top1['win_odds'] == top1.groupby('race_id')['win_odds'].transform('min')).mean()
        logging.info(f"  1番人気選択率: {pop1_rate:.1%}")
        
        # 全件ROI
        all_payout = top1['tansho_payout'].fillna(0).sum()
        all_roi = all_payout / (len(top1) * 100)
        logging.info(f"  全件ROI: {all_roi:.1%}")
        
        return pd.DataFrame(results)
    
    def run(self):
        base_data, races_df, returns_df = self.load_data()
        
        df, feature_cols = self.prepare_features(base_data, races_df)
        
        model = self.train_catboost(df, feature_cols)
        
        results = self.evaluate_edge_score(model, df, feature_cols, returns_df)
        results.to_csv(self.output_dir / 'edge_results.csv', index=False)
        
        # 結論
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        if len(results) > 0:
            best = results.loc[results['roi'].idxmax()]
            logging.info(f"  最高Test ROI: {best['roi']:.1%} (Edge≥{best['edge_thresh']})")
            
            if best['roi'] >= 1.03:
                logging.info("  ✅ CatBoostでROI 103%達成！")
            else:
                logging.info("  ❌ CatBoostでもROI 103%未達")
        
        return results


if __name__ == "__main__":
    analyzer = CatBoostEdgeAnalyzer()
    analyzer.run()
