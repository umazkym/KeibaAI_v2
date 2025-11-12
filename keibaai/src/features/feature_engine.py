#!/usr/bin/env python3
# src/features/feature_engine.py
"""
特徴量生成エンジン
仕様書 6.3 に基づく FeatureEngine クラスの定義
"""

import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

class FeatureEngine:
    """特徴量生成エンジン"""
    
    def __init__(self, config: Dict):
        """
        Args:
            config: 特徴量設定 (configs/features.yaml)
        """
        self.config = config
        self.feature_names = []

    def generate_features(
        self,
        shutuba_df: pd.DataFrame,
        results_history_df: pd.DataFrame,
        horse_profiles_df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        jockey_stats_df: Optional[pd.DataFrame] = None,
        trainer_stats_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        特徴量生成のメイン関数
        仕様書 6.3 のフロー
        """
        logging.info("特徴量生成開始")
        
        # shutuba_df をベースに特徴量を追加
        df = shutuba_df.copy()
        
        # config の存在を安全にチェック
        basic_config = self.config.get('basic_features', {})
        past_perf_config = self.config.get('past_performance_aggregation', {})
        adjusted_speed_config = self.config.get('adjusted_speed', {})
        pedigree_config = self.config.get('pedigree_features', {})
        jockey_trainer_config = self.config.get('jockey_trainer_features', {})
        temporal_config = self.config.get('temporal_features', {})
        within_race_norm_config = self.config.get('within_race_normalization', {})

        # --- 特徴量生成メソッド呼び出し ---
        # (注: 仕様書に詳細実装がないため、スタブメソッドを呼び出します)

        df = self._add_basic_features(df, basic_config)
        df = self._add_past_performance_features(df, results_history_df, past_perf_config)
        
        if adjusted_speed_config.get("enabled", False):
            df = self._add_adjusted_speed(df, results_history_df, adjusted_speed_config)
        
        if pedigree_config.get("enabled", False):
            df = self._add_pedigree_features(df, pedigree_df, results_history_df, pedigree_config)
        
        if jockey_trainer_config.get("enabled", False):
            df = self._add_jockey_trainer_features(df, results_history_df, jockey_stats_df, trainer_stats_df, jockey_trainer_config)
        
        if temporal_config.get("enabled", False):
            df = self._add_temporal_features(df, results_history_df, temporal_config)
        
        if within_race_norm_config.get("enabled", False):
            df = self._add_relative_features(df, within_race_norm_config)
        
        df = self._handle_missing_values(df)
        
        # --- 特徴量名リストの確定 ---
        # (キーと日付カラムを除外)
        base_columns = ["race_id", "horse_id", "horse_number", "race_date", "year", "month", "day"]
        self.feature_names = [
            c for c in df.columns if c not in base_columns and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        
        logging.info(f"特徴量生成完了: {len(self.feature_names)}個の特徴量を生成しました")
        return df

    # =========================================================================
    # 特徴量生成スタブメソッド
    # (仕様書に詳細実装が定義されていないため、エラー回避用に空の関数を定義)
    # (将来的にはこれらの関数内に個別の特徴量エンジニアリングを実装してください)
    # =========================================================================

    def _add_basic_features(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_basic_features 実行")
        # 例: df['age'] = df['age'].fillna(df['age'].median())
        return df

    def _add_past_performance_features(self, df: pd.DataFrame, results_history_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_past_performance_features 実行")
        return df

    def _add_adjusted_speed(self, df: pd.DataFrame, results_history_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_adjusted_speed 実行")
        return df

    def _add_pedigree_features(self, df: pd.DataFrame, pedigree_df: pd.DataFrame, results_history_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_pedigree_features 実行")
        return df

    def _add_jockey_trainer_features(
        self, 
        df: pd.DataFrame, 
        results_history_df: pd.DataFrame, 
        jockey_stats_df: Optional[pd.DataFrame], 
        trainer_stats_df: Optional[pd.DataFrame],
        config: Dict
    ) -> pd.DataFrame:
        logging.debug("STUB: _add_jockey_trainer_features 実行")
        return df

    def _add_temporal_features(self, df: pd.DataFrame, results_history_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_temporal_features 実行")
        return df

    def _add_relative_features(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        logging.debug("STUB: _add_relative_features 実行")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("STUB: _handle_missing_values 実行")
        # 仕様書 6.3 に基づき、カテゴリ変数は 'indicator', 数値変数は 'median' で埋める想定
        # (ここでは簡易的に全NAを0で埋める)
        # df = df.fillna(0) 
        return df

    # =========================================================================
    # 保存メソッド (generate_features.py から呼び出されるため)
    # =========================================================================
    
    def save_features(
        self,
        features_df: pd.DataFrame,
        output_dir: str,
        partition_cols: Optional[List[str]] = None
    ):
        """
        特徴量をParquet形式で保存
        仕様書 6.6 および 17.2 に基づく
        """
        logging.info(f"特徴量を {output_dir} に保存中...")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if features_df.empty:
            logging.warning("保存対象の特徴量データが空です。")
            return

        try:
            # Parquet保存
            features_df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                partition_cols=partition_cols,
                existing_data_behavior='overwrite_or_ignore'
            )
            logging.info(f"{len(features_df)}行を {output_dir} に保存しました")
            
            # 特徴量リストを保存 (仕様書 6.6)
            # Parquet の親ディレクトリ (e.g., data/features/) に保存
            feature_names_path = output_path.parent / 'feature_names.yaml'
            feature_data = {'feature_names': self.feature_names}
            
            with open(feature_names_path, 'w', encoding='utf-8') as f:
                yaml.dump(feature_data, f, allow_unicode=True)
            logging.info(f"特徴量リストを {feature_names_path} に保存しました")

        except Exception as e:
            logging.error(f"特徴量のParquet保存に失敗: {e}", exc_info=True)