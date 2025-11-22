#!/usr/bin/env python3
# src/features/feature_engine.py
"""
特徴量生成エンジン (レシピベース)
"""
import logging
import re
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import yaml
from pathlib import Path

class FeatureEngine:
    """
    YAML設定ファイルに基づいて動的に特徴量を生成するエンジン。
    """
    
    def __init__(self, config_or_path):
        """
        Args:
            config_or_path (str | Path | dict): features.yamlへのパス または 設定辞書
        """
        if isinstance(config_or_path, (str, Path)):
            with open(config_or_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        elif isinstance(config_or_path, dict):
            self.config = config_or_path
        else:
            raise TypeError(f"config_or_path must be str, Path, or dict. Got {type(config_or_path)}")

        self.recipes = self.config.get('feature_recipes', {})
        self.feature_names_ = []
        logging.info(f"FeatureEngine (v{self.config.get('feature_engine', {}).get('version', 'N/A')}) が初期化されました")

    def generate_features(
        self,
        shutuba_df: pd.DataFrame,
        results_history_df: pd.DataFrame,
        horse_profiles_df: pd.DataFrame,
        pedigree_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        shutuba_df と各種履歴データから特徴量を生成する
        """
        logging.info("レシピベースの特徴量生成パイプラインを開始します...")
        df = shutuba_df.copy()

        # --- 前処理: 必要なIDをマージ ---
        if not horse_profiles_df.empty:
            ped_cols = ['horse_id', 'sire_id', 'damsire_id']
            ped_cols_to_merge = [col for col in ped_cols if col in horse_profiles_df.columns]
            if 'horse_id' in ped_cols_to_merge:
                df = df.merge(horse_profiles_df[ped_cols_to_merge], on='horse_id', how='left')

        # --- レシピに基づいて特徴量生成メソッドをディスパッチ ---
        if self.recipes.get('basic_features', {}).get('enabled', False):
            df = self._add_basic_features(df)
        
        # 共通の時系列データフレームを作成
        combined_df = self._create_combined_timeseries_df(df, results_history_df)

        if self.recipes.get('career_stats', {}).get('enabled', False):
            combined_df = self._add_career_stats(combined_df)

        if self.recipes.get('past_performance', {}).get('enabled', False):
            combined_df = self._add_past_performance_features(combined_df)

        if self.recipes.get('change_flags', {}).get('enabled', False):
            combined_df = self._add_change_flags(combined_df)
        
        # --- 集計ベースの特徴量をマージ ---
        # 騎手・調教師・血統などの集計特徴量
        if self.recipes.get('entity_stats', {}).get('enabled', False):
            df = self._add_entity_stats(df, results_history_df)

        # --- 交互作用特徴量 ---
        if self.recipes.get('interaction_features', {}).get('enabled', False):
            df = self._add_interaction_features(df, results_history_df)

        # --- レース内相対特徴量 ---
        # career_stats など、他の特徴量が出揃った後で計算
        # まずは生成された特徴量をマージ
        if 'is_target_race' in combined_df.columns:
            generated_features = combined_df[combined_df['is_target_race'] == 1]
            # is_target_race と is_win はマージ対象から除外
            cols_to_merge = [c for c in generated_features.columns if c not in df.columns or c in ['race_id', 'horse_id']]
            df = df.merge(generated_features[cols_to_merge], on=['race_id', 'horse_id'], how='left')

        if self.recipes.get('relative_features', {}).get('enabled', False):
            df = self._add_relative_features(df)

        # --- 高度な特徴量 (Advanced Features) ---
        # Phase D: ROI向上のため、未使用の高度な特徴量を追加
        try:
            # srcがパスに通っている前提
            from features.advanced_features import AdvancedFeatureEngine
            adv_engine = AdvancedFeatureEngine()

            logging.info("AdvancedFeatureEngine を使用して高度な特徴量を生成します...")

            # 1. ペース・脚質
            df = adv_engine.generate_pace_features(df)

            # 2. 血統 (Deep)
            if pedigree_df is not None and not pedigree_df.empty:
                df = adv_engine.generate_deep_pedigree_features(df, pedigree_df, results_history_df)

            # 3. コースバイアス
            df = adv_engine.generate_course_bias_features(df, results_history_df)

            # 4. パフォーマンストレンド
            df = adv_engine.generate_performance_trend_features(df, results_history_df)

            # 5. 騎手×調教師の相性 (Synergy)
            df = adv_engine.generate_jockey_trainer_synergy(df, results_history_df)

            # 🆕 Phase D: 以下3つの特徴量を新規追加 (ROI向上のため)

            # 6. コース適性特徴量 (競馬場別・距離別・馬場別成績)
            logging.info("コース適性特徴量を生成中...")
            df = adv_engine.generate_course_affinity_features(df, results_history_df)

            # 7. レース条件特徴量 (フィールドサイズ・季節性・レース重要度)
            logging.info("レース条件特徴量を生成中...")
            df = adv_engine.generate_race_condition_features(df)

            # 8. 相対指標 (タイム偏差値・上がり3F相対値・オッズ順位)
            logging.info("レース内相対指標を生成中...")
            df = adv_engine.calculate_relative_metrics(df)

            logging.info("✓ Phase D: 新規特徴量 3カテゴリを追加完了")

        except ImportError as e:
            logging.warning(f"AdvancedFeatureEngineのインポートに失敗しました: {e}")
            # パスが通っていない場合のフォールバック（相対インポート）
            try:
                from .advanced_features import AdvancedFeatureEngine
                adv_engine = AdvancedFeatureEngine()
                # 再試行... (コード重複を避けるためここではログのみ)
                logging.info("相対インポートで成功しました。再実行は実装されていません。")
            except:
                pass
        except Exception as e:
            logging.error(f"高度な特徴量の生成中にエラー: {e}", exc_info=True)

        # --- 後処理 ---
        df = self._handle_missing_values(df)
        self.feature_names_ = self._select_features(df)
        
        # ⚠️ データリーク防止: ターゲット変数を明示的に除外
        target_variables = [
            'finish_position', 'finish_time_seconds', 'is_win',
            'prize_money', 'odds', 'win_odds', 'popularity',
            'margin_seconds', 'finish_time_str', 'margin_str'
        ]
        
        # self.feature_names_からターゲット変数を除外
        self.feature_names_ = [col for col in self.feature_names_ if col not in target_variables]
        logging.info(f"ターゲット変数を除外しました: {[v for v in target_variables if v in df.columns]}")
        
        # ★ race_dateを明示的に保持（パーティション化に必要）★
        # _select_features()は数値型のみを選択するため、datetime型のrace_dateは除外される
        # しかし、race_dateはパーティション化（year, month分割）に必須なので明示的に追加
        final_cols = ['race_id', 'horse_id']
        if 'race_date' in df.columns:
            final_cols.append('race_date')
        final_cols.extend(self.feature_names_)
        final_cols_exist = [col for col in final_cols if col in df.columns]

        # ★ 最終的な重複排除（安全対策）★
        # 特徴量生成過程でのマージ操作により意図しない重複が発生する可能性があるため、
        # 最終段階で(race_id, horse_id)の組み合わせが一意になるよう保証する
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['race_id', 'horse_id'], keep='first')
        final_rows = len(df)

        if initial_rows > final_rows:
            duplicates_removed = initial_rows - final_rows
            logging.warning(f"重複行を検出し削除しました: {duplicates_removed}行 ({duplicates_removed/initial_rows*100:.2f}%)")
            logging.warning(f"  重複前: {initial_rows:,}行 → 重複後: {final_rows:,}行")
        else:
            logging.debug(f"重複チェック完了: 重複なし（{final_rows:,}行）")

        logging.info(f"特徴量生成完了。{len(self.feature_names_)}個の特徴量を生成しました。")
        return df[final_cols_exist]

    def _add_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいて基本特徴量を追加中...")
        recipe = self.recipes.get('basic_features', {})
        
        # One-Hot Encoding
        for ohe_recipe in recipe.get('one_hot_encoding', []):
            col = ohe_recipe.get('column')
            prefix = ohe_recipe.get('prefix')
            if col in df.columns:
                dummies = pd.get_dummies(df[col], prefix=prefix, dtype=int)
                df = pd.concat([df, dummies], axis=1)
        
        # Bracket Categorization
        bracket_recipe = recipe.get('bracket_categorization', {})
        if bracket_recipe.get('enabled') and 'bracket_number' in df.columns:
            df['bracket_is_inner'] = df['bracket_number'].isin(bracket_recipe.get('inner', [])).astype(int)
            df['bracket_is_middle'] = df['bracket_number'].isin(bracket_recipe.get('middle', [])).astype(int)
            df['bracket_is_outer'] = df['bracket_number'].isin(bracket_recipe.get('outer', [])).astype(int)
            
        return df

    def _create_combined_timeseries_df(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("時系列分析用の統合データフレームを作成中...")
        history_df = history_df.copy()
        history_df['is_target_race'] = 0
        df = df.copy()
        df['is_target_race'] = 1
        
        combined = pd.concat([history_df, df], ignore_index=True, sort=False)
        combined = combined.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        
        combined['race_date'] = pd.to_datetime(combined['race_date']).dt.tz_localize(None)
        combined = combined.sort_values(by=['horse_id', 'race_date'], ascending=[True, True])
        
        # is_win を事前に計算
        if 'finish_position' in combined.columns:
            combined['is_win'] = (combined['finish_position'] == 1).fillna(False).astype(int)
        else:
            combined['is_win'] = 0
            
        return combined

    def _add_career_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいて通算成績特徴量を追加中...")
        recipe = self.recipes.get('career_stats', {})
        stats_to_calc = recipe.get('stats', [])
        
        grouped = df.groupby('horse_id', sort=False)
        
        if 'career_starts' in stats_to_calc:
            df['career_starts'] = grouped.cumcount()
        
        if 'career_wins' in stats_to_calc:
            df['career_wins'] = grouped['is_win'].cumsum().shift(1).fillna(0).astype(int)
            
        if 'prize_total' in stats_to_calc:
            if 'prize_money' not in df.columns:
                df['prize_money'] = 0
            df['prize_money'] = df['prize_money'].fillna(0)
            df['prize_total'] = grouped['prize_money'].cumsum().shift(1).fillna(0)
            
        return df

    def _add_past_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいて過去走集約特徴量を追加中...")
        recipe = self.recipes.get('past_performance', {})
        agg_cols = recipe.get('columns', [])
        windows = recipe.get('windows', [])
        aggregations = recipe.get('aggregations', [])
        
        grouped = df.groupby('horse_id', sort=False)
        
        for col in agg_cols:
            if col not in df.columns:
                logging.warning(f"過去走集約: カラム '{col}' が存在しません。スキップします。")
                continue
            
            shifted = grouped[col].shift(1)
            
            for w in windows:
                rolled = shifted.rolling(window=w, min_periods=1)
                for agg in aggregations:
                    feat_name = f'past_{w}_{col}_{agg}'
                    try:
                        agg_result = getattr(rolled, agg)()
                        df[feat_name] = agg_result.reset_index(level=0, drop=True)
                    except AttributeError:
                        logging.error(f"集計関数 '{agg}' はサポートされていません。")
        
        # 前走からの日数
        df['days_since_last_race'] = grouped['race_date'].diff().dt.days
        
        return df

    def _add_change_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいて乗り替わりフラグを追加中...")
        recipe = self.recipes.get('change_flags', {})
        columns = recipe.get('columns', [])
        
        grouped = df.groupby('horse_id', sort=False)
        
        for col in columns:
            if col in df.columns:
                prev_col_name = f'prev_{col}'
                flag_col_name = f'is_{col}_changed'
                df[prev_col_name] = grouped[col].shift(1)
                is_changed = (df[col] != df[prev_col_name]) & (df[prev_col_name].notna())
                df[flag_col_name] = is_changed.fillna(False).astype(int)
        
        return df

    def _add_entity_stats(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいてエンティティ集計特徴量を追加中...")
        recipe = self.recipes.get('entity_stats', {})
        entities = recipe.get('entities', [])
        
        history_df = history_df.copy()
        if 'finish_position' in history_df.columns:
            history_df['is_win'] = (history_df['finish_position'] == 1).fillna(False).astype(int)
        
        for entity_recipe in entities:
            entity_name = entity_recipe.get('name')
            id_col = entity_recipe.get('id_column')
            target_col = entity_recipe.get('target')
            agg_func = entity_recipe.get('agg')
            feature_name = entity_recipe.get('feature_name')

            if id_col not in history_df.columns:
                logging.warning(f"エンティティ集計 ({entity_name}): IDカラム '{id_col}' が履歴にありません。")
                continue
            if target_col not in history_df.columns:
                logging.warning(f"エンティティ集計 ({entity_name}): ターゲットカラム '{target_col}' が履歴にありません。")
                continue
            
            # 欠損を除外して集計
            stats = history_df.dropna(subset=[id_col, target_col]).groupby(id_col)[target_col].agg(agg_func).reset_index()
            stats = stats.rename(columns={target_col: feature_name})
            
            if id_col in df.columns and not stats.empty:
                df = df.merge(stats, on=id_col, how='left')
        
        return df

    def _add_interaction_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        交互作用特徴量を追加（騎手×競馬場、種牡馬×馬場など）

        Args:
            df: 予測対象データ
            history_df: 過去のレース履歴データ

        Returns:
            交互作用特徴量が追加されたDataFrame
        """
        logging.debug("レシピに基づいて交互作用特徴量を追加中...")
        interaction_config = self.recipes.get('interaction_features', {})

        # YAML構造: interaction_features.interactions がリスト
        if isinstance(interaction_config, dict):
            interaction_recipes = interaction_config.get('interactions', [])
        else:
            interaction_recipes = []

        if not interaction_recipes:
            logging.debug("交互作用特徴量のレシピが定義されていません。")
            return df

        # distance_categoryが存在しない場合は作成
        if 'distance_m' in df.columns and 'distance_category' not in df.columns:
            df = self._add_distance_category(df)
        if 'distance_m' in history_df.columns and 'distance_category' not in history_df.columns:
            history_df = self._add_distance_category(history_df)

        # is_winカラムの確保
        history_df = history_df.copy()
        if 'finish_position' in history_df.columns and 'is_win' not in history_df.columns:
            history_df['is_win'] = (history_df['finish_position'] == 1).fillna(False).astype(int)

        for interaction_recipe in interaction_recipes:
            if not isinstance(interaction_recipe, dict):
                continue

            interaction_name = interaction_recipe.get('name')
            id_col = interaction_recipe.get('id_column')
            context_col = interaction_recipe.get('context_column')
            target_col = interaction_recipe.get('target')
            agg_func = interaction_recipe.get('agg')
            feature_template = interaction_recipe.get('feature_template')

            # 必須カラムのチェック
            if id_col not in history_df.columns:
                logging.warning(f"交互作用 ({interaction_name}): IDカラム '{id_col}' が履歴にありません。")
                continue
            if context_col not in history_df.columns:
                logging.warning(f"交互作用 ({interaction_name}): コンテキストカラム '{context_col}' が履歴にありません。")
                continue
            if target_col not in history_df.columns:
                logging.warning(f"交互作用 ({interaction_name}): ターゲットカラム '{target_col}' が履歴にありません。")
                continue

            # 欠損を除外して集計
            valid_history = history_df.dropna(subset=[id_col, context_col, target_col])

            if valid_history.empty:
                logging.warning(f"交互作用 ({interaction_name}): 有効な履歴データがありません。")
                continue

            # 交互作用ごとに集計
            grouped = valid_history.groupby([id_col, context_col])[target_col].agg(agg_func).reset_index()

            # コンテキスト値ごとに個別のカラムを作成（ピボット）
            for context_value in grouped[context_col].unique():
                # コンテキスト値のクリーニング（ファイル名に使える形式に）
                context_value_clean = str(context_value).replace(' ', '_').replace('/', '_')
                feature_name = feature_template.format(context=context_value_clean)

                # この特定のコンテキスト値でフィルタリング
                context_stats = grouped[grouped[context_col] == context_value][[id_col, target_col]].copy()
                context_stats = context_stats.rename(columns={target_col: feature_name})

                # マージ
                if id_col in df.columns and not context_stats.empty:
                    df = df.merge(context_stats, on=id_col, how='left')
                    logging.debug(f"交互作用特徴量 '{feature_name}' を追加しました。")

        return df

    def _add_distance_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        距離カテゴリを追加（sprint, mile, intermediate, long, marathon）
        """
        if 'distance_m' not in df.columns:
            return df

        def categorize_distance(distance):
            if pd.isna(distance):
                return 'unknown'
            if distance < 1400:
                return 'sprint'
            elif distance < 1800:
                return 'mile'
            elif distance < 2200:
                return 'intermediate'
            elif distance < 2800:
                return 'long'
            else:
                return 'marathon'

        df['distance_category'] = df['distance_m'].apply(categorize_distance)
        return df

    def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("レシピに基づいてレース内相対特徴量を追加中...")
        recipe = self.recipes.get('relative_features', {})
        zscore_cols = recipe.get('z_score', {}).get('columns', [])
        
        if 'race_id' not in df.columns:
            return df
             
        grouped = df.groupby('race_id')
        
        for col in zscore_cols:
            if col in df.columns:
                feat_name = f'{col}_zscore'
                try:
                    df[feat_name] = grouped[col].transform(
                        lambda x: (x - x.mean()) / (x.std() + 1e-6)
                    )
                except Exception as e:
                     logging.warning(f"Z-score ({col}) の計算に失敗: {e}")
                     df[feat_name] = 0.0
        
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug("欠損値を処理中...")
        recipe = self.config.get('imputation', {})
        strategy = recipe.get('numeric_strategy', 'median')
        default_value = recipe.get('default_value', 0.0)
        
        num_cols = df.select_dtypes(include=np.number).columns
        
        # 特徴量として選択される可能性のある数値カラムのみを対象
        # この時点ではまだ self.feature_names_ が確定していないので、除外パターンで判定
        temp_feature_names = self._select_features(df.copy())
        cols_to_fill = [col for col in num_cols if col in temp_feature_names]
        
        for col in cols_to_fill:
            if df[col].isnull().any():
                if strategy == 'median':
                    fill_value = df[col].median()
                elif strategy == 'mean':
                    fill_value = df[col].mean()
                else: # zero
                    fill_value = 0
                
                if pd.isna(fill_value):
                    fill_value = default_value
                    
                df[col] = df[col].fillna(fill_value)
        
        return df

    def _select_features(self, df: pd.DataFrame) -> List[str]:
        logging.debug("使用する特徴量を選択中...")
        recipe = self.config.get('feature_selection', {})
        exclude_patterns = recipe.get('exclude_patterns', [])
        
        feature_cols = []
        all_cols = df.columns.tolist()

        for col in all_cols:
            is_excluded = False
            for pattern in exclude_patterns:
                try:
                    if re.match(pattern, col):
                        is_excluded = True
                        break
                except re.error as e:
                    logging.warning(f"特徴量選択の除外パターンで正規表現エラー: '{pattern}' - {e}")
                    continue
            
            if not is_excluded:
                # さらに数値型であることも確認
                if pd.api.types.is_numeric_dtype(df[col]):
                    feature_cols.append(col)

        return sorted(list(dict.fromkeys(feature_cols)))

    def save_features(
        self,
        features_df: pd.DataFrame,
        output_dir: str,
        partition_cols: List[str]
    ):
        """
        生成した特徴量をParquet形式で保存
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        try:
            valid_partition_cols = [col for col in partition_cols if col in features_df.columns]
            
            if valid_partition_cols:
                 logging.info(f"パーティションカラム {valid_partition_cols} を使用して保存します")
                 features_df.to_parquet(
                    output_path,
                    engine='pyarrow',
                    compression='snappy',
                    partition_cols=valid_partition_cols,
                    existing_data_behavior='delete_matching'
                )
            else:
                 logging.warning(f"パーティションカラム {partition_cols} がDFにないため、単一ファイルで保存します")
                 features_df.to_parquet(
                    output_path / "features.parquet",
                    engine='pyarrow',
                    compression='snappy'
                )
            logging.info(f"特徴量を {output_path} に保存しました")

            # 特徴量リストを保存
            features_list_path = output_path / "feature_names.yaml"
            try:
                with open(features_list_path, 'w', encoding='utf-8') as f:
                    yaml.dump(self.feature_names_, f, allow_unicode=True)
                logging.info(f"特徴量リストを {features_list_path} に保存しました")
            except Exception as e:
                 logging.error(f"特徴量リスト (feature_names.yaml) の保存に失敗: {e}")

        except Exception as e:
            logging.error(f"特徴量のParquet保存に失敗: {e}", exc_info=True)
            if "partition_cols" in str(e):
                 logging.error("ヒント: パーティションカラム (year, month) がDataFrameに存在するか確認してください。")
