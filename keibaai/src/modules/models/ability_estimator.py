"""
Layer 1: Ability Estimator (能力推定層)

4つのランク学習モデルをアンサンブルし、オッズ・人気を排除した
「真の能力スコア」を算出する。

Models:
1. LightGBM LambdaRank (Graded Relevance: 1着=5, 2着=4, 3着=3)
2. LightGBM LambdaRank (Win Focus: 1着=1, 他=0)
3. CatBoost YetiRank
4. CatBoost QuerySoftMax

重要:
- オッズ・人気は特徴量から完全排除
- レース結果由来の変数（passing_order等）も排除
"""

import numpy as np
import pandas as pd
import lightgbm as lgb
from typing import Dict, List, Tuple, Optional, Union
import logging
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

# CatBoostはオプショナル（インストールされていない場合はLightGBMのみ）
try:
    from catboost import CatBoostRanker, Pool
    CATBOOST_AVAILABLE = True
except ImportError:
    CATBOOST_AVAILABLE = False
    logger.warning("CatBoost not installed. Using LightGBM only.")


class Layer1_AbilityEstimator:
    """
    Layer 1: 能力推定層
    
    複数のランク学習モデルをアンサンブルし、
    各馬の「真の能力スコア」を算出する。
    """
    
    # 禁止特徴量（オッズ・人気・レース結果由来）
    FORBIDDEN_FEATURES = [
        # 直接的オッズ情報
        'odds', 'win_odds', 'morning_odds',
        'popularity', 'morning_popularity',
        'expected_odds', 'betting_fraction',
        'win_probability', 'relative_odds', 'odds_rank',
        # レース結果由来（データリーク）
        'finish_position', 'finish_time_seconds',
        'last_3f_time', 'time_except_last3f',
        'passing_order_1', 'passing_order_2',
        'passing_order_3', 'passing_order_4',
        'pace_index', 'final_corner_to_finish',
        'position_change_1_2', 'position_change_2_3',
        'position_change_3_4', 'payout', 'margin_seconds',
        # 人気との相関が高い可能性のある変数
        'popularity_finish_diff',
    ]
    
    # モデルの重み
    MODEL_WEIGHTS = {
        'lgbm_lambda_graded': 0.30,
        'lgbm_lambda_win': 0.20,
        'catboost_yeti': 0.30,
        'catboost_query': 0.20,
    }
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Args:
            config: ハイパーパラメータ設定（オプション）
        """
        self.config = config or {}
        self.models: Dict = {}
        self.feature_names_: Optional[List[str]] = None
        self.feature_importance_: Optional[pd.Series] = None
        self.is_trained: bool = False
        
    def _filter_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        禁止特徴量を除外
        """
        valid_cols = [
            col for col in X.columns 
            if col not in self.FORBIDDEN_FEATURES
        ]
        
        removed = set(X.columns) - set(valid_cols)
        if removed:
            logger.info(f"Removed forbidden features: {removed}")
        
        return X[valid_cols]
    
    def _create_relevance_labels(
        self, 
        ranks: np.ndarray, 
        mode: str = 'graded'
    ) -> np.ndarray:
        """
        着順から関連度ラベルを生成
        
        Args:
            ranks: 着順配列
            mode: 'graded', 'binary_win', 'binary_top3', 'inverse'
        
        Returns:
            関連度スコア配列
        """
        if mode == 'graded':
            # 段階的関連度: 1着=5, 2着=4, 3着=3, 4-5着=1, 他=0
            relevance = np.zeros_like(ranks, dtype=float)
            relevance[ranks == 1] = 5
            relevance[ranks == 2] = 4
            relevance[ranks == 3] = 3
            relevance[(ranks >= 4) & (ranks <= 5)] = 1
            return relevance
            
        elif mode == 'binary_win':
            # 単勝用: 1着=1, 他=0
            return (ranks == 1).astype(float)
            
        elif mode == 'binary_top3':
            # 複勝用: 3着以内=1, 他=0
            return (ranks <= 3).astype(float)
            
        elif mode == 'inverse':
            # 逆数: 1/rank
            return 1.0 / np.maximum(ranks, 1)
            
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    def _build_lgbm_ranker(self) -> lgb.LGBMRanker:
        """LightGBM Rankerを構築"""
        return lgb.LGBMRanker(
            objective='lambdarank',
            metric='ndcg',
            n_estimators=self.config.get('n_estimators', 1000),
            learning_rate=self.config.get('learning_rate', 0.05),
            max_depth=self.config.get('max_depth', 6),
            num_leaves=self.config.get('num_leaves', 31),
            min_child_samples=self.config.get('min_child_samples', 20),
            subsample=self.config.get('subsample', 0.8),
            colsample_bytree=self.config.get('colsample_bytree', 0.8),
            reg_alpha=self.config.get('reg_alpha', 0.1),
            reg_lambda=self.config.get('reg_lambda', 0.1),
            random_state=42,
            n_jobs=-1,
            verbose=-1,
            importance_type='gain'
        )
    
    def _build_catboost_ranker(self, loss_function: str = 'YetiRank') -> 'CatBoostRanker':
        """CatBoost Rankerを構築"""
        if not CATBOOST_AVAILABLE:
            return None
            
        return CatBoostRanker(
            iterations=self.config.get('n_estimators', 1000),
            learning_rate=self.config.get('learning_rate', 0.05),
            depth=self.config.get('max_depth', 6),
            loss_function=loss_function,
            eval_metric='NDCG',
            random_seed=42,
            verbose=False,
            early_stopping_rounds=50
        )
    
    def train(
        self, 
        X: pd.DataFrame, 
        y_rank: pd.Series, 
        groups: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val_rank: Optional[pd.Series] = None,
        groups_val: Optional[pd.Series] = None
    ) -> 'Layer1_AbilityEstimator':
        """
        複数モデルを学習
        
        Args:
            X: 特徴量DataFrame
            y_rank: 着順
            groups: レースID（グループ化用）
            X_val: 検証用特徴量
            y_val_rank: 検証用着順
            groups_val: 検証用レースID
        """
        logger.info("=" * 60)
        logger.info("Layer 1: 能力推定モデルの学習開始")
        logger.info("=" * 60)
        
        # 禁止特徴量を除外
        X_filtered = self._filter_features(X)
        self.feature_names_ = list(X_filtered.columns)
        
        logger.info(f"Features after filtering: {len(self.feature_names_)} columns")
        
        # データをレースIDでソート（ランク学習の必須要件）
        sort_idx = np.argsort(groups.values)
        X_sorted = X_filtered.iloc[sort_idx].reset_index(drop=True)
        y_sorted = y_rank.iloc[sort_idx].reset_index(drop=True)
        groups_sorted = groups.iloc[sort_idx].reset_index(drop=True)
        
        # グループサイズを計算
        group_sizes = groups_sorted.value_counts().sort_index().values.tolist()
        
        # 検証データの処理
        group_sizes_val = None
        if X_val is not None:
            X_val_filtered = self._filter_features(X_val)
            sort_idx_val = np.argsort(groups_val.values)
            X_val_sorted = X_val_filtered.iloc[sort_idx_val].reset_index(drop=True)
            y_val_sorted = y_val_rank.iloc[sort_idx_val].reset_index(drop=True)
            groups_val_sorted = groups_val.iloc[sort_idx_val].reset_index(drop=True)
            group_sizes_val = groups_val_sorted.value_counts().sort_index().values.tolist()
        
        # ========================================
        # Model 1: LightGBM LambdaRank（段階的関連度）
        # ========================================
        logger.info("\n[Model 1] LightGBM LambdaRank (Graded Relevance)")
        y_graded = self._create_relevance_labels(y_sorted.values, mode='graded')
        
        self.models['lgbm_lambda_graded'] = self._build_lgbm_ranker()
        
        eval_set = None
        eval_group = None
        callbacks = None
        if X_val is not None:
            y_val_graded = self._create_relevance_labels(y_val_sorted.values, mode='graded')
            eval_set = [(X_val_sorted, y_val_graded)]
            eval_group = [group_sizes_val]
            callbacks = [lgb.early_stopping(50, verbose=False)]
        
        self.models['lgbm_lambda_graded'].fit(
            X_sorted, y_graded,
            group=group_sizes,
            eval_set=eval_set,
            eval_group=eval_group,
            callbacks=callbacks
        )
        logger.info(f"  → Best iteration: {self.models['lgbm_lambda_graded'].best_iteration_}")
        
        # ========================================
        # Model 2: LightGBM LambdaRank（単勝特化）
        # ========================================
        logger.info("\n[Model 2] LightGBM LambdaRank (Win Focus)")
        y_win = self._create_relevance_labels(y_sorted.values, mode='binary_win')
        
        self.models['lgbm_lambda_win'] = self._build_lgbm_ranker()
        
        if X_val is not None:
            y_val_win = self._create_relevance_labels(y_val_sorted.values, mode='binary_win')
            eval_set = [(X_val_sorted, y_val_win)]
        
        self.models['lgbm_lambda_win'].fit(
            X_sorted, y_win,
            group=group_sizes,
            eval_set=eval_set,
            eval_group=eval_group,
            callbacks=callbacks
        )
        logger.info(f"  → Best iteration: {self.models['lgbm_lambda_win'].best_iteration_}")
        
        # ========================================
        # Model 3 & 4: CatBoost（オプション）
        # ========================================
        if CATBOOST_AVAILABLE:
            logger.info("\n[Model 3] CatBoost YetiRank")
            train_pool = Pool(
                data=X_sorted,
                label=y_graded,
                group_id=groups_sorted.values
            )
            
            val_pool = None
            if X_val is not None:
                val_pool = Pool(
                    data=X_val_sorted,
                    label=y_val_graded,
                    group_id=groups_val_sorted.values
                )
            
            self.models['catboost_yeti'] = self._build_catboost_ranker('YetiRank')
            self.models['catboost_yeti'].fit(train_pool, eval_set=val_pool, use_best_model=True)
            logger.info(f"  → Best iteration: {self.models['catboost_yeti'].best_iteration_}")
            
            logger.info("\n[Model 4] CatBoost QuerySoftMax")
            self.models['catboost_query'] = self._build_catboost_ranker('QuerySoftMax')
            self.models['catboost_query'].fit(train_pool, eval_set=val_pool, use_best_model=True)
            logger.info(f"  → Best iteration: {self.models['catboost_query'].best_iteration_}")
        else:
            logger.warning("CatBoost not available, using LightGBM only")
            # 重みを再調整
            self.MODEL_WEIGHTS = {
                'lgbm_lambda_graded': 0.60,
                'lgbm_lambda_win': 0.40,
            }
        
        # 特徴量重要度の計算
        self._calculate_feature_importance()
        
        self.is_trained = True
        logger.info("\n" + "=" * 60)
        logger.info("Layer 1: 学習完了")
        logger.info("=" * 60)
        
        return self
    
    def _calculate_feature_importance(self):
        """特徴量重要度を統合計算"""
        importance_dict = {name: 0.0 for name in self.feature_names_}
        
        for model_name, model in self.models.items():
            weight = self.MODEL_WEIGHTS.get(model_name, 0.25)
            imp = None
            
            try:
                if 'lgbm' in model_name:
                    imp = model.feature_importances_
                elif 'catboost' in model_name and CATBOOST_AVAILABLE:
                    # CatBoostはget_feature_importance()を使用
                    imp = model.get_feature_importance()
                else:
                    continue
                
                # impが有効な配列かチェック
                if imp is not None and hasattr(imp, '__len__') and len(imp) > 0:
                    for i, name in enumerate(self.feature_names_):
                        if i < len(imp):
                            importance_dict[name] += imp[i] * weight
            except Exception as e:
                logging.warning(f"Could not get feature importance for {model_name}: {e}")
                continue
        
        self.feature_importance_ = pd.Series(importance_dict).sort_values(ascending=False)
    
    def predict(self, X: pd.DataFrame, groups: pd.Series) -> pd.DataFrame:
        """
        各モデルのスコアを予測し、アンサンブルスコアを算出
        
        Args:
            X: 特徴量
            groups: レースID
        
        Returns:
            DataFrame with ability_score and ability_rank
        """
        if not self.is_trained:
            raise ValueError("モデルが学習されていません")
        
        # 禁止特徴量を除外
        X_filtered = self._filter_features(X)
        
        # 欠損列をチェック
        missing_cols = set(self.feature_names_) - set(X_filtered.columns)
        if missing_cols:
            logger.warning(f"Missing features will be filled with 0: {missing_cols}")
            for col in missing_cols:
                X_filtered[col] = 0
        
        # 列順を揃える
        X_filtered = X_filtered[self.feature_names_]
        
        predictions = pd.DataFrame({
            'group_id': groups.values
        })
        
        # 各モデルの予測
        ensemble_score = np.zeros(len(X_filtered))
        
        for model_name, model in self.models.items():
            weight = self.MODEL_WEIGHTS.get(model_name, 0.25)
            
            if 'lgbm' in model_name:
                pred = model.predict(X_filtered)
            elif 'catboost' in model_name and CATBOOST_AVAILABLE:
                pred = model.predict(X_filtered)
            else:
                continue
            
            predictions[f'score_{model_name}'] = pred
            ensemble_score += pred * weight
        
        predictions['ability_score'] = ensemble_score
        
        # グループ内での順位
        predictions['ability_rank'] = predictions.groupby('group_id')['ability_score'].rank(
            ascending=False, method='first'
        ).astype(int)
        
        return predictions
    
    def get_feature_importance(self, top_n: int = 20) -> pd.Series:
        """特徴量重要度上位を取得"""
        if self.feature_importance_ is None:
            raise ValueError("Feature importance not calculated yet")
        return self.feature_importance_.head(top_n)
    
    def save(self, filepath: str):
        """モデルを保存"""
        import joblib
        from pathlib import Path
        
        output_dir = Path(filepath)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 各モデルを保存
        for model_name, model in self.models.items():
            if 'lgbm' in model_name:
                joblib.dump(model, output_dir / f"{model_name}.pkl")
            elif 'catboost' in model_name:
                model.save_model(str(output_dir / f"{model_name}.cbm"))
        
        # メタデータ
        import json
        metadata = {
            'feature_names': list(self.feature_names_),  # listに変換
            'model_weights': {k: float(v) for k, v in self.MODEL_WEIGHTS.items()},  # floatに変換
            'catboost_available': bool(CATBOOST_AVAILABLE)  # boolに変換
        }
        with open(output_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Models saved to {output_dir}")
    
    def load(self, filepath: str) -> 'Layer1_AbilityEstimator':
        """モデルを読み込み"""
        import joblib
        from pathlib import Path
        import json
        
        model_dir = Path(filepath)
        
        # メタデータ読み込み
        with open(model_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        
        self.feature_names_ = metadata['feature_names']
        self.MODEL_WEIGHTS = metadata['model_weights']
        
        # LightGBMモデル読み込み
        for name in ['lgbm_lambda_graded', 'lgbm_lambda_win']:
            pkl_path = model_dir / f"{name}.pkl"
            if pkl_path.exists():
                self.models[name] = joblib.load(pkl_path)
        
        # CatBoostモデル読み込み（存在する場合）
        if CATBOOST_AVAILABLE:
            for name in ['catboost_yeti', 'catboost_query']:
                cbm_path = model_dir / f"{name}.cbm"
                if cbm_path.exists():
                    self.models[name] = CatBoostRanker()
                    self.models[name].load_model(str(cbm_path))
        
        self.is_trained = True
        logger.info(f"Models loaded from {model_dir}")
        
        return self
