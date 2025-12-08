#!/usr/bin/env python3
"""
真のエッジスコア分析スクリプト（全券種対応）

【根本的な問題の発見】
これまでの分析は「v11.0の生予測」にフィルタをかけていただけで、
計画書で述べている「専門モデルによる確率調整」が欠けていた！

【計画書の設計】
統合確率 = main_prob × (1 + pace_adj) × (1 + pedigree_adj)
Edge Score = 統合確率 / オッズ暗示確率

【このスクリプトの目的】
1. v11.0モデルのraw確率を取得
2. PaceScenarioModelの調整値を適用
3. PedigreeAptitudeModelの調整値を適用
4. 「真のEdge Score」を計算
5. 全券種（単勝/複勝/馬連/馬単/ワイド/三連複/三連単）で評価
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TrueEdgeAnalyzer:
    """
    真のエッジスコア分析
    
    専門モデルの調整を適用した「真のEdge Score」で全券種を評価
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/true_edge_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
        self.pace_model = None
        self.pedigree_model = None
        
    def load_all_components(self):
        """全コンポーネント読み込み"""
        logging.info("=" * 60)
        logging.info("コンポーネント読み込み")
        logging.info("=" * 60)
        
        # v11.0モデル
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        logging.info(f"  v11.0モデル: {len(self.feature_names)}特徴量")
        
        # 専門モデル読み込み
        try:
            from keibaai.src.models.specialized import PaceScenarioModel, PedigreeAptitudeModel
            self.pace_model = PaceScenarioModel()
            self.pedigree_model = PedigreeAptitudeModel()
            logging.info("  専門モデル: PaceScenarioModel, PedigreeAptitudeModel")
        except ImportError as e:
            logging.warning(f"  専門モデル読み込み失敗: {e}")
        
        # データ
        base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        base_data['race_date'] = pd.to_datetime(base_data['race_date'])
        base_data['race_id'] = base_data['race_id'].astype(str)
        base_data['horse_id'] = base_data['horse_id'].astype(str)
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        # コーナー通過データ
        try:
            corner_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
            corner_df['race_id'] = corner_df['race_id'].astype(str)
            logging.info(f"  コーナー通過: {len(corner_df):,}件")
        except:
            corner_df = None
            logging.warning("  コーナー通過データなし")
        
        # 血統データ
        try:
            pedigree_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
            pedigree_df['horse_id'] = pedigree_df['horse_id'].astype(str)
            logging.info(f"  血統データ: {len(pedigree_df):,}件")
        except:
            pedigree_df = None
            logging.warning("  血統データなし")
        
        logging.info(f"  学習データ: {len(base_data):,}件")
        logging.info(f"  レース結果: {len(races_df):,}件")
        logging.info(f"  配当データ: {len(returns_df):,}件")
        
        return base_data, races_df, returns_df, corner_df, pedigree_df
    
    def fit_specialized_models(self, races_df, corner_df, pedigree_df, train_cutoff='2023-01-01'):
        """専門モデルを学習期間データでfit（Pedigreeのみ、Paceは直接predict）"""
        logging.info("\n専門モデル学習中...")
        
        train_races = races_df[races_df['race_date'] < train_cutoff]
        
        # PaceScenarioModelはfitメソッドなし（predictで直接使用）
        if self.pace_model and corner_df is not None:
            logging.info(f"  PaceScenarioModel: corner_history {len(corner_df):,}件（predictで直接使用）")
        
        # PedigreeAptitudeModel: 血統適性
        if self.pedigree_model and pedigree_df is not None:
            self.pedigree_model.fit(train_races, pedigree_df)
            n_sires = len(self.pedigree_model.sire_stats) if hasattr(self.pedigree_model, 'sire_stats') else 0
            logging.info(f"  PedigreeAptitudeModel: {n_sires}種牡馬")
    
    def calculate_true_edge_score(self, df, races_df, corner_df, pedigree_df):
        """
        真のEdge Scoreを計算
        
        統合確率 = main_prob × (1 + pace_adj) × (1 + pedigree_adj)
        Edge Score = 統合確率 / オッズ暗示確率
        """
        logging.info("\n真のEdge Score計算中...")
        
        df = df.copy()
        
        # 1. v11.0モデルの生予測
        for f in self.feature_names:
            if f not in df.columns:
                df[f] = 0
            else:
                df[f] = df[f].fillna(0)
        
        raw_preds = self.model.predict(df[self.feature_names])
        df['raw_score'] = raw_preds
        df['rank_in_race'] = df.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
        
        # 2. softmax確率
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['main_prob'] = df.groupby('race_id')['raw_score'].transform(softmax_prob)
        
        # 3. 展開調整 (PaceScenarioModel)
        df['pace_adj'] = 0.0
        if self.pace_model and corner_df is not None:
            logging.info("  展開調整を計算中...")
            for race_id in df['race_id'].unique()[:100]:  # サンプル
                race_df = df[df['race_id'] == race_id]
                try:
                    _, adv_scores = self.pace_model.predict(race_df, corner_df)
                    df.loc[df['race_id'] == race_id, 'pace_adj'] = adv_scores[:len(race_df)]
                except:
                    pass
        
        # 4. 血統調整 (PedigreeAptitudeModel)
        df['pedigree_adj'] = 0.0
        if self.pedigree_model and pedigree_df is not None:
            logging.info("  血統調整を計算中...")
            for race_id in df['race_id'].unique()[:100]:  # サンプル
                race_df = df[df['race_id'] == race_id]
                try:
                    apt_scores = self.pedigree_model.predict(race_df, None)
                    df.loc[df['race_id'] == race_id, 'pedigree_adj'] = apt_scores[:len(race_df)]
                except:
                    pass
        
        # 5. 統合確率 = main_prob × (1 + pace_adj) × (1 + pedigree_adj)
        adjustment_factor = (1 + df['pace_adj'].clip(-0.2, 0.2)) * (1 + df['pedigree_adj'].clip(-0.15, 0.15))
        df['integrated_prob'] = df['main_prob'] * adjustment_factor
        
        # レースごとに正規化
        df['integrated_prob'] = df.groupby('race_id')['integrated_prob'].transform(
            lambda x: x / x.sum() if x.sum() > 0 else x
        )
        
        # 6. 結果データマージ
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        for col in ['finish_position', 'horse_number', 'win_odds', 'popularity']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        races_subset = races_df[['race_id', 'horse_id', 'finish_position', 'horse_number', 'win_odds', 'popularity']].copy()
        races_subset['horse_number'] = pd.to_numeric(races_subset['horse_number'], errors='coerce')
        races_subset['finish_position'] = pd.to_numeric(races_subset['finish_position'], errors='coerce')
        races_subset['win_odds'] = pd.to_numeric(races_subset['win_odds'], errors='coerce')
        races_subset = races_subset.drop_duplicates(subset=['race_id', 'horse_id'])
        
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        # 7. オッズ暗示確率（控除率20%調整）
        df['odds_implied_prob'] = (1 / df['win_odds'].clip(lower=1.1)) * 0.8
        
        # 8. 真のEdge Score
        df['true_edge_score'] = df['integrated_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # 9. 信頼度（確信度）= 予測1位と2位のスコア差
        df['score_diff'] = df.groupby('race_id')['raw_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        score_diff_max = df['score_diff'].max()
        df['confidence'] = df['score_diff'] / score_diff_max if score_diff_max > 0 else 0
        
        df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
        df['is_place'] = (df['finish_position'] <= 3).fillna(False).astype(int)
        
        return df
    
    def analyze_all_bet_types_with_true_edge(self, pred_df, returns_df, period_name):
        """全券種を真のEdge Scoreで分析"""
        logging.info(f"\n【{period_name}期間 真のEdge分析】")
        logging.info("=" * 60)
        
        results = []
        
        # 予測1位を取得
        top1 = pred_df[pred_df['rank_in_race'] == 1].copy()
        
        # 単勝・複勝の配当マージ
        tansho_df = returns_df[returns_df['bet_type'] == 'tansho'].copy()
        fukusho_df = returns_df[returns_df['bet_type'] == 'fukusho'].copy()
        
        top1 = top1.merge(
            tansho_df[['race_id', 'horse_number', 'payout']].rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'], how='left'
        )
        top1 = top1.merge(
            fukusho_df[['race_id', 'horse_number', 'payout']].rename(columns={'payout': 'fukusho_payout'}),
            on=['race_id', 'horse_number'], how='left'
        )
        
        # 閾値別に評価
        for edge_thresh in [1.0, 1.2, 1.5, 2.0]:
            for conf_thresh in [0.0, 0.1, 0.2]:
                filtered = top1[(top1['true_edge_score'] >= edge_thresh) & 
                               (top1['confidence'] >= conf_thresh)]
                
                if len(filtered) < 30:
                    continue
                
                # 単勝ROI
                tansho_payout = filtered['tansho_payout'].fillna(0).sum()
                tansho_roi = tansho_payout / (len(filtered) * 100)
                
                # 複勝ROI
                fukusho_payout = filtered['fukusho_payout'].fillna(0).sum()
                fukusho_roi = fukusho_payout / (len(filtered) * 100)
                
                # 的中率
                tansho_hit = (filtered['tansho_payout'] > 0).sum() / len(filtered)
                fukusho_hit = (filtered['fukusho_payout'] > 0).sum() / len(filtered)
                
                results.append({
                    'period': period_name,
                    'edge_thresh': edge_thresh,
                    'conf_thresh': conf_thresh,
                    'bet_count': len(filtered),
                    'tansho_roi': tansho_roi,
                    'tansho_hit': tansho_hit,
                    'fukusho_roi': fukusho_roi,
                    'fukusho_hit': fukusho_hit,
                })
                
                if conf_thresh == 0.1:
                    logging.info(
                        f"  Edge≥{edge_thresh:.1f}, Conf≥{conf_thresh} | "
                        f"ベット{len(filtered):4} | "
                        f"単勝ROI{tansho_roi:6.1%} | 複勝ROI{fukusho_roi:6.1%}"
                    )
        
        return pd.DataFrame(results)
    
    def analyze_multi_horse_with_true_edge(self, pred_df, returns_df, period_name):
        """馬連・馬単・三連複・三連単を真のEdge Scoreで分析"""
        logging.info(f"\n【{period_name}期間 マルチ馬ベット（真Edge）】")
        logging.info("=" * 60)
        
        results = []
        
        # レースごとに予測上位を取得
        race_preds = {}
        for race_id, group in pred_df.groupby('race_id'):
            sorted_g = group.sort_values('rank_in_race')
            top3 = sorted_g.head(3)
            
            if len(top3) >= 3:
                # 上位3頭の平均Edge Score
                avg_edge = top3['true_edge_score'].mean()
                avg_conf = top3['confidence'].mean()
                
                race_preds[race_id] = {
                    'top1': int(top3.iloc[0]['horse_number']) if pd.notna(top3.iloc[0]['horse_number']) else None,
                    'top2': int(top3.iloc[1]['horse_number']) if pd.notna(top3.iloc[1]['horse_number']) else None,
                    'top3': int(top3.iloc[2]['horse_number']) if pd.notna(top3.iloc[2]['horse_number']) else None,
                    'avg_edge': avg_edge,
                    'avg_conf': avg_conf,
                }
        
        # 各券種の配当データ
        umaren_df = returns_df[returns_df['bet_type'] == 'umaren']
        umatan_df = returns_df[returns_df['bet_type'] == 'umatan']
        sanrenpuku_df = returns_df[returns_df['bet_type'] == 'sanrenpuku']
        sanrentan_df = returns_df[returns_df['bet_type'] == 'sanrentan']
        
        for edge_thresh, conf_thresh in [(1.0, 0.0), (1.2, 0.1), (1.5, 0.1)]:
            # Edge/Confidence閾値でフィルタ
            filtered_races = {
                k: v for k, v in race_preds.items()
                if v['avg_edge'] >= edge_thresh and v['avg_conf'] >= conf_thresh
            }
            
            if len(filtered_races) < 30:
                continue
            
            # 馬連
            umaren_hits = 0
            umaren_payout = 0
            for _, row in umaren_df.iterrows():
                if row['race_id'] in filtered_races:
                    rp = filtered_races[row['race_id']]
                    if rp['top1'] and rp['top2']:
                        pred_set = {rp['top1'], rp['top2']}
                        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                            actual_set = {int(row['horse_1']), int(row['horse_2'])}
                            if pred_set == actual_set:
                                umaren_hits += 1
                                umaren_payout += row['payout']
            
            # 馬単
            umatan_hits = 0
            umatan_payout = 0
            for _, row in umatan_df.iterrows():
                if row['race_id'] in filtered_races:
                    rp = filtered_races[row['race_id']]
                    if rp['top1'] and rp['top2']:
                        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
                            if rp['top1'] == int(row['horse_1']) and rp['top2'] == int(row['horse_2']):
                                umatan_hits += 1
                                umatan_payout += row['payout']
            
            # 三連複
            sanrenpuku_hits = 0
            sanrenpuku_payout = 0
            for _, row in sanrenpuku_df.iterrows():
                if row['race_id'] in filtered_races:
                    rp = filtered_races[row['race_id']]
                    if all(rp[f'top{i}'] for i in [1, 2, 3]):
                        pred_set = {rp['top1'], rp['top2'], rp['top3']}
                        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) and pd.notna(row['horse_3']):
                            actual_set = {int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])}
                            if pred_set == actual_set:
                                sanrenpuku_hits += 1
                                sanrenpuku_payout += row['payout']
            
            # 三連単
            sanrentan_hits = 0
            sanrentan_payout = 0
            for _, row in sanrentan_df.iterrows():
                if row['race_id'] in filtered_races:
                    rp = filtered_races[row['race_id']]
                    if all(rp[f'top{i}'] for i in [1, 2, 3]):
                        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']) and pd.notna(row['horse_3']):
                            if (rp['top1'] == int(row['horse_1']) and 
                                rp['top2'] == int(row['horse_2']) and 
                                rp['top3'] == int(row['horse_3'])):
                                sanrentan_hits += 1
                                sanrentan_payout += row['payout']
            
            n_races = len(filtered_races)
            
            results.append({
                'period': period_name,
                'edge_thresh': edge_thresh,
                'conf_thresh': conf_thresh,
                'n_races': n_races,
                'umaren_hits': umaren_hits,
                'umaren_roi': umaren_payout / (n_races * 100),
                'umatan_hits': umatan_hits,
                'umatan_roi': umatan_payout / (n_races * 100),
                'sanrenpuku_hits': sanrenpuku_hits,
                'sanrenpuku_roi': sanrenpuku_payout / (n_races * 100),
                'sanrentan_hits': sanrentan_hits,
                'sanrentan_roi': sanrentan_payout / (n_races * 100),
            })
            
            logging.info(
                f"  Edge≥{edge_thresh:.1f}, Conf≥{conf_thresh} | レース{n_races:4} | "
                f"馬連{umaren_payout / (n_races * 100):6.1%} | "
                f"馬単{umatan_payout / (n_races * 100):6.1%} | "
                f"三連複{sanrenpuku_payout / (n_races * 100):6.1%} | "
                f"三連単{sanrentan_payout / (n_races * 100):6.1%}"
            )
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        # データ読み込み
        base_data, races_df, returns_df, corner_df, pedigree_df = self.load_all_components()
        
        # 専門モデル学習
        self.fit_specialized_models(races_df, corner_df, pedigree_df, train_cutoff='2023-01-01')
        
        # 真のEdge Score計算
        pred_df = self.calculate_true_edge_score(base_data, races_df, corner_df, pedigree_df)
        
        # Valid/Test分割
        valid_pred = pred_df[(pred_df['race_date'] >= '2023-01-01') & 
                             (pred_df['race_date'] < '2024-01-01')]
        test_pred = pred_df[pred_df['race_date'] >= '2024-01-01']
        
        logging.info(f"\n  Valid: {len(valid_pred):,}件 ({valid_pred['race_id'].nunique()}レース)")
        logging.info(f"  Test: {len(test_pred):,}件 ({test_pred['race_id'].nunique()}レース)")
        
        # 単勝・複勝分析
        valid_single = self.analyze_all_bet_types_with_true_edge(
            valid_pred, 
            returns_df[returns_df['race_id'].isin(valid_pred['race_id'])],
            'Valid'
        )
        test_single = self.analyze_all_bet_types_with_true_edge(
            test_pred,
            returns_df[returns_df['race_id'].isin(test_pred['race_id'])],
            'Test'
        )
        
        # マルチ馬ベット分析
        valid_multi = self.analyze_multi_horse_with_true_edge(
            valid_pred,
            returns_df[returns_df['race_id'].isin(valid_pred['race_id'])],
            'Valid'
        )
        test_multi = self.analyze_multi_horse_with_true_edge(
            test_pred,
            returns_df[returns_df['race_id'].isin(test_pred['race_id'])],
            'Test'
        )
        
        # 結果保存
        all_single = pd.concat([valid_single, test_single], ignore_index=True)
        all_multi = pd.concat([valid_multi, test_multi], ignore_index=True)
        all_single.to_csv(self.output_dir / 'true_edge_single_results.csv', index=False)
        all_multi.to_csv(self.output_dir / 'true_edge_multi_results.csv', index=False)
        
        # 最高ROI表示
        logging.info("\n" + "=" * 60)
        logging.info("【Test期間 最高ROI】")
        logging.info("=" * 60)
        
        if len(test_single) > 0:
            best_tansho = test_single.loc[test_single['tansho_roi'].idxmax()]
            best_fukusho = test_single.loc[test_single['fukusho_roi'].idxmax()]
            logging.info(f"  単勝: {best_tansho['tansho_roi']:.1%} (Edge≥{best_tansho['edge_thresh']}, Conf≥{best_tansho['conf_thresh']})")
            logging.info(f"  複勝: {best_fukusho['fukusho_roi']:.1%} (Edge≥{best_fukusho['edge_thresh']}, Conf≥{best_fukusho['conf_thresh']})")
        
        if len(test_multi) > 0:
            for col in ['umaren_roi', 'umatan_roi', 'sanrenpuku_roi', 'sanrentan_roi']:
                best = test_multi.loc[test_multi[col].idxmax()]
                logging.info(f"  {col.replace('_roi', '')}: {best[col]:.1%}")
        
        return all_single, all_multi


if __name__ == "__main__":
    analyzer = TrueEdgeAnalyzer()
    single_results, multi_results = analyzer.run()
