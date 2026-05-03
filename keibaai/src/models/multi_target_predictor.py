#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3指数統合予測モデル (Multi-Target Predictor)

【概要】
1着予測・2着以内予測・3着以内予測の3つの指数を別々のモデルで学習し、
券種に応じて最適な指数を使い分ける。

【3指数の役割】
- win_prob:   P(1着) → 単勝向け
- top2_prob:  P(2着以内) → 馬連・馬単向け
- place_prob: P(3着以内) → 複勝・ワイド向け

【設計方針】
1. 芝/ダート別モデル: 馬場ごとに閾値・重みを最適化
2. V15+V4.4+V16 Ensemble: 既存優秀モデルの良い部分を継承
3. 過学習対策: 強正則化 + 特徴量削減 + 時系列厳格分離
4. リスクオフ: 各指数の特徴を活かした券種別戦略

【リーク防止】
- finish_position: ターゲット変数 → shift(1)必須
- win_odds: レース確定後 → 使用禁止
- last_3f_time: 当該レース結果 → 使用禁止
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
from typing import Dict, Tuple, Optional
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MultiTargetPredictor:
    """
    3指数統合予測クラス
    
    【特徴】
    1. 1着/2着以内/3着以内の3モデルを同時学習
    2. 芝/ダート別に異なる正則化強度を適用
    3. V15+V4.4アンサンブル手法を継承
    """
    
    def __init__(self, 
                 surface_specific: bool = True,
                 use_v44_residual: bool = True,
                 regularization_level: str = 'strong',
                 use_early_stopping: bool = False,
                 fixed_iterations: int = 100,
                 use_sample_weight: bool = False):
        """
        Args:
            surface_specific: 芝/ダート別モデルを使用するか
            use_v44_residual: V4.4 Residual（大穴発見）を使用するか
            regularization_level: 'normal', 'strong', 'extreme' の3段階
            use_early_stopping: early_stoppingを使用するか（デフォルト: False）
            fixed_iterations: 固定イテレーション数（early_stopping無効時に使用）
            use_sample_weight: サンプル重み付け設定（6年間検証済み）
                - False or 'off': 重み付けなし（デフォルト・安全策）
                - True or 'aggressive': 2022年以降有効（推奨）
                - 'conservative': ❌非推奨（効果が薄い）
        """
        self.surface_specific = surface_specific
        self.use_v44_residual = use_v44_residual
        self.regularization_level = regularization_level
        self.use_early_stopping = use_early_stopping
        self.fixed_iterations = fixed_iterations
        self.use_sample_weight = use_sample_weight
        
        # モデル格納
        self.models = {
            'win': {},      # 1着予測
            'top2': {},     # 2着以内予測
            'place': {},    # 3着以内予測
            'v44': {},      # V4.4 Residual（大穴用）
        }
        
        self.feature_cols = []
        self.fitted = False
        
    def _get_params(self, target_type: str, surface: str = 'all') -> Dict:
        """
        ターゲットと馬場に応じたハイパーパラメータを返す
        
        【設計思想】
        - 1着予測(win): 標準正則化（ターゲットが明確）
        - 2着以内(top2): 強正則化（ノイズが増加）
        - 3着以内(place): 中程度（データ量でカバー）
        - ダート: より強い正則化（予測困難）
        """
        # ベースパラメータ
        base_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'random_state': 42,
        }
        
        # 正則化レベル別設定
        if self.regularization_level == 'extreme':
            reg_params = {
                'max_depth': 2,
                'num_leaves': 4,
                'min_child_samples': 300,
                'reg_alpha': 15.0,
                'reg_lambda': 20.0,
                'subsample': 0.4,
                'colsample_bytree': 0.4,
                'learning_rate': 0.01,
            }
        elif self.regularization_level == 'strong':
            reg_params = {
                'max_depth': 3,
                'num_leaves': 8,
                'min_child_samples': 150,
                'reg_alpha': 8.0,
                'reg_lambda': 12.0,
                'subsample': 0.5,
                'colsample_bytree': 0.5,
                'learning_rate': 0.02,
            }
        else:  # normal
            reg_params = {
                'max_depth': 4,
                'num_leaves': 15,
                'min_child_samples': 100,
                'reg_alpha': 3.0,
                'reg_lambda': 5.0,
                'subsample': 0.7,
                'colsample_bytree': 0.7,
                'learning_rate': 0.03,
            }
        
        # ターゲット別調整
        if target_type == 'top2':
            # 2着以内は過学習しやすい → より強い正則化
            reg_params['reg_alpha'] *= 1.5
            reg_params['reg_lambda'] *= 1.5
            reg_params['min_child_samples'] = int(reg_params['min_child_samples'] * 1.3)
        
        # 馬場別調整
        if surface == 'dirt':
            # ダートは予測困難 → さらに強い正則化
            reg_params['reg_alpha'] *= 1.2
            reg_params['reg_lambda'] *= 1.2
        
        return {**base_params, **reg_params}
    
    def _get_v44_params(self) -> Dict:
        """V4.4 Residual Model用パラメータ（大穴発見特化）"""
        return {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'num_leaves': 25,
            'max_depth': 4,
            'min_child_samples': 150,
            'learning_rate': 0.05,
            'reg_alpha': 8.0,
            'reg_lambda': 12.0,
            'feature_fraction': 0.5,
            'bagging_fraction': 0.6,
            'bagging_freq': 5,
            'verbosity': -1,
            'random_state': 42,
            'label_gain': list(range(100)),
        }
    
    def _create_target(self, df: pd.DataFrame, target_type: str) -> pd.Series:
        """ターゲット変数を作成"""
        if target_type == 'win':
            return (df['finish_position'] == 1).astype(int)
        elif target_type == 'top2':
            return (df['finish_position'] <= 2).astype(int)
        elif target_type == 'place':
            return (df['finish_position'] <= 3).astype(int)
        else:
            raise ValueError(f"Unknown target type: {target_type}")
    
    def _calc_sample_weight(self, df: pd.DataFrame, mode: str = 'aggressive') -> np.ndarray:
        """
        サンプル重みを計算
        
        Args:
            df: データフレーム
            mode: 'aggressive'（元の設定）または 'conservative'（保守的設定）
        
        【6年間Walk-forward検証結果に基づく設計】
        - aggressive: 2022年以降有効、2020-2021年は悪化。単勝向け。
        - conservative: 副作用を抑えた安全な設定。推奨。
        
        【aggressive設定】
        - 人気馬(1-5番人気)1着: 1.5
        - 中穴(6-9番人気)1着: 2.5
        - 拮抗レース1着: 3.0
        - 大穴(10+)1着: 0.3
        - 1番人気10着以下: 0.0
        
        【conservative設定】（推奨）
        - 人気馬(1-5番人気)1着: 1.2
        - 中穴(6-9番人気)1着: 1.5
        - 拮抗レース条件: 削除（不安定なため）
        - 大穴(10+)1着: 0.5
        - 1番人気10着以下: 0.0
        """
        n = len(df)
        weights = np.ones(n)
        
        pop = df['popularity'].values
        finish = df['finish_position'].values
        odds = df['win_odds'].values if 'win_odds' in df.columns else np.ones(n) * 5.0
        
        # 共通: 外れ値除外（1番人気で10着以下）
        outlier = (pop == 1) & (finish >= 10)
        weights[outlier] = 0.0
        
        if mode == 'conservative':
            # 保守的設定（推奨）
            # 運要素軽減: 10番人気以下または80倍以上で1着
            luck = ((pop >= 10) | (odds >= 80)) & (finish == 1)
            weights[luck] = 0.5  # 0.3→0.5（穏やか）
            
            # 人気馬1着
            popular_win = (pop <= 5) & (finish == 1) & (weights == 1.0)
            weights[popular_win] = 1.2  # 1.5→1.2（穏やか）
            
            # 中穴1着
            mid_longshot_win = (pop >= 6) & (pop <= 9) & (finish == 1) & (weights == 1.0)
            weights[mid_longshot_win] = 1.5  # 2.5→1.5（穏やか）
            
            # 拮抗レース条件は削除（不安定なため）
        else:
            # アグレッシブ設定（元の設定）
            luck = ((pop >= 10) | (odds >= 80)) & (finish == 1)
            weights[luck] = 0.3
            
            popular_win = (pop <= 5) & (finish == 1) & (weights == 1.0)
            weights[popular_win] = 1.5
            
            mid_longshot_win = (pop >= 6) & (pop <= 9) & (finish == 1) & (weights == 1.0)
            weights[mid_longshot_win] = 2.5
            
            # 拮抗レースで1着
            if 'race_id' in df.columns:
                race_odds_std = df.groupby('race_id')['win_odds'].transform('std')
                competitive = (race_odds_std < race_odds_std.median()) & (finish == 1) & (weights < 3.0)
                weights[competitive] = 3.0
        
        return weights
    
    def _create_v44_target(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        V4.4 LambdaRank用ターゲット作成
        オッズ重み付きRelevanceスコア
        """
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
        
        relevance = gain.astype(int)
        sample_weight = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        return pd.Series(relevance, index=df.index), pd.Series(sample_weight, index=df.index)
    
    def fit(self, train_df: pd.DataFrame, valid_df: pd.DataFrame, 
            feature_cols: list,
            num_boost_round: int = 300) -> 'MultiTargetPredictor':
        """
        3指数モデルを学習
        
        Args:
            train_df: 学習データ（特徴量エンジン適用済み）
            valid_df: 検証データ
            feature_cols: 特徴量カラム名リスト
            num_boost_round: ブースティング回数
        """
        logger.info("=" * 60)
        logger.info("3指数統合予測モデル 学習開始")
        logger.info("=" * 60)
        
        self.feature_cols = feature_cols
        
        # 芝/ダート分離
        surfaces = ['turf', 'dirt'] if self.surface_specific else ['all']
        
        for surface in surfaces:
            logger.info(f"\n--- {surface.upper()} モデル学習 ---")
            
            # データフィルタリング
            if surface == 'turf':
                train_sub = train_df[train_df['track_surface'].str.contains('芝', na=False)].copy()
                valid_sub = valid_df[valid_df['track_surface'].str.contains('芝', na=False)].copy()
            elif surface == 'dirt':
                train_sub = train_df[train_df['track_surface'].str.contains('ダート', na=False)].copy()
                valid_sub = valid_df[valid_df['track_surface'].str.contains('ダート', na=False)].copy()
            else:
                train_sub = train_df.copy()
                valid_sub = valid_df.copy()
            
            if len(train_sub) < 5000:
                logger.warning(f"  {surface}: データ不足 ({len(train_sub)}件) → スキップ")
                continue
            
            logger.info(f"  Train: {len(train_sub):,} / Valid: {len(valid_sub):,}")
            
            X_train = train_sub[feature_cols].fillna(0)
            X_valid = valid_sub[feature_cols].fillna(0)
            
            # サンプル重み計算
            sample_weights = None
            if self.use_sample_weight and self.use_sample_weight != 'off':
                # True or 'aggressive' -> aggressive
                # 'conservative' -> conservative
                if isinstance(self.use_sample_weight, bool) or self.use_sample_weight == 'aggressive':
                    mode = 'aggressive'
                else:
                    mode = 'conservative'
                sample_weights = self._calc_sample_weight(train_sub, mode=mode)
                logger.info(f"  サンプル重み({mode}): mean={sample_weights.mean():.2f}, zero={(sample_weights==0).sum()}")
            
            # === 1着予測モデル (win) ===
            logger.info("  [1] Win Model (1着予測) 学習中...")
            y_train_win = self._create_target(train_sub, 'win')
            y_valid_win = self._create_target(valid_sub, 'win')
            
            params_win = self._get_params('win', surface)
            train_ds_win = lgb.Dataset(X_train, y_train_win, weight=sample_weights)
            valid_ds_win = lgb.Dataset(X_valid, y_valid_win, reference=train_ds_win)
            
            # early_stoppingを条件付きで適用
            if self.use_early_stopping:
                self.models['win'][surface] = lgb.train(
                    params_win, train_ds_win,
                    num_boost_round=num_boost_round,
                    valid_sets=[valid_ds_win],
                    callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                )
            else:
                # 固定イテレーション数で学習（過学習防止）
                self.models['win'][surface] = lgb.train(
                    params_win, train_ds_win,
                    num_boost_round=self.fixed_iterations
                )
            
            # === 2着以内予測モデル (top2) ===
            logger.info("  [2] Top2 Model (2着以内予測) 学習中...")
            y_train_top2 = self._create_target(train_sub, 'top2')
            y_valid_top2 = self._create_target(valid_sub, 'top2')
            
            params_top2 = self._get_params('top2', surface)
            train_ds_top2 = lgb.Dataset(X_train, y_train_top2, weight=sample_weights)
            valid_ds_top2 = lgb.Dataset(X_valid, y_valid_top2, reference=train_ds_top2)
            
            if self.use_early_stopping:
                self.models['top2'][surface] = lgb.train(
                    params_top2, train_ds_top2,
                    num_boost_round=num_boost_round,
                    valid_sets=[valid_ds_top2],
                    callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                )
            else:
                self.models['top2'][surface] = lgb.train(
                    params_top2, train_ds_top2,
                    num_boost_round=self.fixed_iterations
                )
            
            # === 3着以内予測モデル (place) ===
            logger.info("  [3] Place Model (3着以内予測) 学習中...")
            y_train_place = self._create_target(train_sub, 'place')
            y_valid_place = self._create_target(valid_sub, 'place')
            
            params_place = self._get_params('place', surface)
            train_ds_place = lgb.Dataset(X_train, y_train_place, weight=sample_weights)
            valid_ds_place = lgb.Dataset(X_valid, y_valid_place, reference=train_ds_place)
            
            if self.use_early_stopping:
                self.models['place'][surface] = lgb.train(
                    params_place, train_ds_place,
                    num_boost_round=num_boost_round,
                    valid_sets=[valid_ds_place],
                    callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                )
            else:
                self.models['place'][surface] = lgb.train(
                    params_place, train_ds_place,
                    num_boost_round=self.fixed_iterations
                )
            
            # === V4.4 Residual Model（大穴発見用） ===
            if self.use_v44_residual:
                logger.info("  [4] V4.4 Residual Model (大穴発見) 学習中...")
                
                train_sub_sorted = train_sub.sort_values(['race_id', 'horse_number']).reset_index(drop=True)
                y_train_v44, w_train = self._create_v44_target(train_sub_sorted)
                groups_train = train_sub_sorted.groupby('race_id').size().to_list()
                
                valid_sub_sorted = valid_sub.sort_values(['race_id', 'horse_number']).reset_index(drop=True)
                y_valid_v44, _ = self._create_v44_target(valid_sub_sorted)
                groups_valid = valid_sub_sorted.groupby('race_id').size().to_list()
                
                X_train_v44 = train_sub_sorted[feature_cols].fillna(0)
                X_valid_v44 = valid_sub_sorted[feature_cols].fillna(0)
                
                params_v44 = self._get_v44_params()
                
                if self.use_early_stopping:
                    self.models['v44'][surface] = lgb.LGBMRanker(**params_v44, n_estimators=num_boost_round)
                    self.models['v44'][surface].fit(
                        X_train_v44, y_train_v44, 
                        group=groups_train,
                        sample_weight=w_train,
                        eval_set=[(X_valid_v44, y_valid_v44)],
                        eval_group=[groups_valid],
                        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                    )
                else:
                    self.models['v44'][surface] = lgb.LGBMRanker(**params_v44, n_estimators=self.fixed_iterations)
                    self.models['v44'][surface].fit(
                        X_train_v44, y_train_v44, 
                        group=groups_train,
                        sample_weight=w_train
                    )
        
        self.fitted = True
        logger.info("\n学習完了")
        return self
    
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        3指数 + V4.4 Residualを予測
        
        Returns:
            DataFrame with columns: win_prob, top2_prob, place_prob, v44_score, ensemble_score
        """
        if not self.fitted:
            raise RuntimeError("Model not fitted. Call fit() first.")
        
        result_df = df[['race_id', 'horse_number']].copy()
        X = df[self.feature_cols].fillna(0)
        
        # 芝/ダート判定
        is_turf = df['track_surface'].str.contains('芝', na=False)
        is_dirt = df['track_surface'].str.contains('ダート', na=False)
        
        # 初期化
        result_df['win_prob'] = 0.0
        result_df['top2_prob'] = 0.0
        result_df['place_prob'] = 0.0
        result_df['v44_score'] = 0.0
        
        for surface, mask in [('turf', is_turf), ('dirt', is_dirt)]:
            if mask.sum() == 0:
                continue
            
            X_sub = X[mask]
            
            # Win予測
            if surface in self.models['win']:
                result_df.loc[mask, 'win_prob'] = self.models['win'][surface].predict(X_sub)
            elif 'all' in self.models['win']:
                result_df.loc[mask, 'win_prob'] = self.models['win']['all'].predict(X_sub)
            
            # Top2予測
            if surface in self.models['top2']:
                result_df.loc[mask, 'top2_prob'] = self.models['top2'][surface].predict(X_sub)
            elif 'all' in self.models['top2']:
                result_df.loc[mask, 'top2_prob'] = self.models['top2']['all'].predict(X_sub)
            
            # Place予測
            if surface in self.models['place']:
                result_df.loc[mask, 'place_prob'] = self.models['place'][surface].predict(X_sub)
            elif 'all' in self.models['place']:
                result_df.loc[mask, 'place_prob'] = self.models['place']['all'].predict(X_sub)
            
            # V4.4 Residual予測
            if self.use_v44_residual:
                if surface in self.models['v44']:
                    result_df.loc[mask, 'v44_score'] = self.models['v44'][surface].predict(X_sub)
                elif 'all' in self.models['v44']:
                    result_df.loc[mask, 'v44_score'] = self.models['v44']['all'].predict(X_sub)
        
        # 正規化
        for col in ['win_prob', 'top2_prob', 'place_prob', 'v44_score']:
            min_val = result_df[col].min()
            max_val = result_df[col].max()
            if max_val > min_val:
                result_df[col] = (result_df[col] - min_val) / (max_val - min_val + 1e-8)
        
        # アンサンブルスコア（V15+V4.4方式）
        # win_prob: 50%, v44_score: 30%, top2_prob: 10%, place_prob: 10%
        result_df['ensemble_score'] = (
            result_df['win_prob'] * 0.50 +
            result_df['v44_score'] * 0.30 +
            result_df['top2_prob'] * 0.10 +
            result_df['place_prob'] * 0.10
        )
        
        # ========== 着順別確率の算出 ==========
        # P(exactly 1着) = win_prob
        # P(exactly 2着) = top2_prob - win_prob
        # P(exactly 3着) = place_prob - top2_prob
        result_df['prob_exactly_1st'] = result_df['win_prob']
        result_df['prob_exactly_2nd'] = (result_df['top2_prob'] - result_df['win_prob']).clip(lower=0)
        result_df['prob_exactly_3rd'] = (result_df['place_prob'] - result_df['top2_prob']).clip(lower=0)
        
        # ========== レース荒れ度分析 ==========
        # place_prob標準偏差: 低い = 実力差が見えにくい = 荒れやすい
        result_df['race_place_prob_std'] = result_df.groupby('race_id')['place_prob'].transform('std')
        
        # 荒れ度分類
        # 閾値は6年Walk-forward検証に基づく（仮: 0.10, 0.18）
        def classify_volatility(std):
            if std < 0.10:
                return 'volatile'  # 荒れやすい
            elif std < 0.18:
                return 'moderate'  # 中程度
            else:
                return 'stable'    # 堅い
        
        result_df['race_volatility_type'] = result_df['race_place_prob_std'].apply(classify_volatility)
        
        # V4.4推奨ウェイト（荒れレースでは穴馬発見能力を活用）
        weight_map = {'volatile': 0.45, 'moderate': 0.35, 'stable': 0.30}
        result_df['v44_recommended_weight'] = result_df['race_volatility_type'].map(weight_map)
        
        # 適応型アンサンブルスコア
        # 荒れレースではV4.4のウェイトを増加
        result_df['adaptive_ensemble_score'] = (
            result_df['win_prob'] * (1 - result_df['v44_recommended_weight'] - 0.20) +
            result_df['v44_score'] * result_df['v44_recommended_weight'] +
            result_df['top2_prob'] * 0.10 +
            result_df['place_prob'] * 0.10
        )
        
        return result_df
    
    def get_feature_importance(self, top_n: int = 20) -> Dict[str, pd.DataFrame]:
        """特徴量重要度を取得"""
        importance_dict = {}
        
        for model_type in ['win', 'top2', 'place']:
            for surface, model in self.models[model_type].items():
                key = f"{model_type}_{surface}"
                importance = pd.DataFrame({
                    'feature': self.feature_cols,
                    'importance': model.feature_importance(importance_type='gain')
                }).sort_values('importance', ascending=False).head(top_n)
                importance_dict[key] = importance
        
        return importance_dict


def calc_multi_roi(pred_df: pd.DataFrame, test_df: pd.DataFrame, 
                   returns_df: pd.DataFrame, 
                   score_col: str = 'ensemble_score') -> Dict:
    """
    複数券種のROIを計算
    
    Returns:
        Dict with 'tansho', 'fukusho', 'umaren' ROIs
    """
    # Top1予測抽出
    merged = pred_df.merge(
        test_df[['race_id', 'horse_number', 'finish_position', 'win_odds']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    merged['rank_pred'] = merged.groupby('race_id')[score_col].rank(ascending=False, method='first')
    
    # === 単勝ROI ===
    top1 = merged[merged['rank_pred'] == 1].copy()
    tansho_hits = top1[top1['finish_position'] == 1]
    tansho_roi = tansho_hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    tansho_hit_rate = len(tansho_hits) / len(top1) * 100 if len(top1) > 0 else 0
    
    # === 複勝ROI ===
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    top1_fukusho = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    top1_fukusho['payout'] = top1_fukusho['payout'].fillna(0)
    fukusho_roi = top1_fukusho['payout'].sum() / (len(top1) * 100) * 100 if len(top1) > 0 else 0
    fukusho_hit_rate = (top1['finish_position'] <= 3).mean() * 100
    
    return {
        'tansho_roi': tansho_roi,
        'tansho_hit_rate': tansho_hit_rate,
        'fukusho_roi': fukusho_roi,
        'fukusho_hit_rate': fukusho_hit_rate,
        'bet_count': len(top1),
    }


def calc_roi_by_prob(pred_df: pd.DataFrame, test_df: pd.DataFrame, 
                     returns_df: pd.DataFrame,
                     prob_cols: list = ['win_prob', 'top2_prob', 'place_prob', 'ensemble_score']) -> Dict:
    """
    各確率スコアごとのROIを計算（比較用）
    """
    results = {}
    
    for prob_col in prob_cols:
        roi_info = calc_multi_roi(pred_df, test_df, returns_df, score_col=prob_col)
        results[prob_col] = roi_info
    
    return results
