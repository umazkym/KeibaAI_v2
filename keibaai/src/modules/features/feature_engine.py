# #!/usr/bin/env python3
# # src/features/feature_engine.py
# """
# 特徴量生成エンジン
# 仕様書 6.3章 に基づく実装
# """
# import logging
# from typing import Dict, List, Optional
# from pathlib import Path
# import pandas as pd
# import numpy as np
# import yaml

# class FeatureEngine:
#     """特徴量生成エンジン"""
    
#     def __init__(self, config: Dict):
#         """
#         Args:
#             config: configs/features.yaml の内容
#         """
#         self.config = config
#         self.feature_names_ = []
#         logging.info("FeatureEngine (v1.0) が初期化されました")

#     def generate_features(
#         self,
#         shutuba_df: pd.DataFrame,
#         results_history_df: pd.DataFrame,
#         horse_profiles_df: pd.DataFrame,
#         pedigree_df: pd.DataFrame
#     ) -> pd.DataFrame:
#         """
#         メインの特徴量生成関数
        
#         Args:
#             shutuba_df (pd.DataFrame): 対象レースの出馬表データ (日付フィルタ済み)
#             results_history_df (pd.DataFrame): 過去の全レース結果
#             horse_profiles_df (pd.DataFrame): 全馬のプロフィール
#             pedigree_df (pd.DataFrame): 全馬の血統データ

#         Returns:
#             pd.DataFrame: 特徴量が付加されたDataFrame
#         """
#         logging.info("特徴量生成開始...")
        
#         # (1) ベースとなる出馬表データを使用
#         df = shutuba_df.copy()
        
#         # (2) 馬のプロフィール情報をマージ (年齢、性別など)
#         if not horse_profiles_df.empty:
#             df = df.merge(
#                 horse_profiles_df,
#                 on='horse_id',
#                 how='left',
#                 suffixes=('', '_profile')
#             )
        
#         # (3) 基本特徴量の生成 (仕様書 6.2 basic_features)
#         df = self._add_basic_features(df)
        
#         # (4) 過去走集約 (仕様書 6.2 past_performance_aggregation)
#         if not results_history_df.empty:
#             df = self._add_past_performance_features(df, results_history_df)

#         # (5) 血統特徴量 (仕様書 6.2 pedigree_features)
#         if not pedigree_df.empty:
#             df = self._add_pedigree_features(df, pedigree_df, results_history_df)

#         # (6) 騎手・調教師特徴量 (仕様書 6.2 jockey_trainer_features)
#         if not results_history_df.empty:
#              df = self._add_jockey_trainer_features(df, results_history_df)

#         # (7) レース内正規化 (仕様書 6.2 within_race_normalization)
#         df = self._add_relative_features(df)
        
#         # (8) 欠損値処理 (仕様書 6.2 missing_value_strategy)
#         df = self._handle_missing_values(df)

#         # (9) 特徴量リストの確定
#         self.feature_names_ = self._select_features(df)
        
#         logging.info(f"特徴量生成完了: {len(self.feature_names_)}個の特徴量を生成")
        
#         # 必要なカラム + 特徴量のみを返す
#         # ★修正★: key_cols に 'jockey_id', 'trainer_id' を含める（モデル学習や後続の分析で利用するため）
#         key_cols = ['race_id', 'horse_id', 'horse_number', 'race_date', 'jockey_id', 'trainer_id']
#         # (学習に必要な目的変数も残す)
#         target_cols = ['finish_position', 'finish_time_seconds', 'win_odds', 'popularity']
        
#         # df[final_cols] を行うために、dfに存在するカラムのみを対象とする
#         existing_key_cols = [col for col in key_cols if col in df.columns]
#         existing_target_cols = [col for col in target_cols if col in df.columns]
        
#         final_cols = existing_key_cols + existing_target_cols + self.feature_names_
        
#         # 重複を除外
#         final_cols = list(dict.fromkeys(final_cols))
        
#         # 最終的なDataFrameに必要なカラムが存在するか確認
#         missing_in_df = [col for col in final_cols if col not in df.columns]
#         if missing_in_df:
#             logging.warning(f"最終カラムリストに含まれるべきカラムがDataFrameに存在しません: {missing_in_df}")
#             # 存在しないカラムを除外して続行
#             final_cols = [col for col in final_cols if col in df.columns]

#         # 最終的な重複排除
#         if df.duplicated(subset=['race_id', 'horse_id']).any():
#             logging.warning(f"特徴量生成の最終段階で重複が検出されました。重複を排除します。")
#             df = df.drop_duplicates(subset=['race_id', 'horse_id'], keep='first')

#         return df[final_cols]

#     def _add_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 basic_features に基づく基本特徴量の生成
#         (例: カテゴリ変数のOne-Hotエンコーディング)
#         """
#         logging.debug("基本特徴量を追加中...")
        
#         # (1) 性別 (One-Hot)
#         if 'sex' in df.columns:
#             sex_dummies = pd.get_dummies(df['sex'], prefix='sex', dtype=int)
#             df = pd.concat([df, sex_dummies], axis=1)

#         # (2) 斤量
#         # (basis_weight は shutuba_parser.py ですでに数値化されている)
        
#         # (3) レース基本情報 (distance_m, track_surface, head_count, bracket_number, horse_number)
#         if 'track_surface' in df.columns:
#              track_dummies = pd.get_dummies(df['track_surface'], prefix='track', dtype=int)
#              df = pd.concat([df, track_dummies], axis=1)

#         # (4) 枠番 (内枠・中枠・外枠)
#         if 'bracket_number' in df.columns:
#             df['bracket_is_inner'] = df['bracket_number'].isin([1, 2, 3]).astype(int)
#             df['bracket_is_middle'] = df['bracket_number'].isin([4, 5, 6]).astype(int)
#             df['bracket_is_outer'] = df['bracket_number'].isin([7, 8]).astype(int)

#         return df

#     def _add_past_performance_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 past_performance_aggregation に基づく過去走集約
#         """
#         logging.debug("過去走集約特徴量を追加中...")
        
#         history = history_df.copy()
        
#         # horse_id と race_date が必要
#         if 'horse_id' not in history.columns or 'race_date' not in history.columns:
#             logging.warning("history_df に 'horse_id' または 'race_date' がないため、過去走集約をスキップします。")
#             return df
            
#         history['race_date'] = pd.to_datetime(history['race_date']).dt.tz_localize(None)
        
#         # 集約対象カラム
#         agg_cols = self.config.get('past_performance_aggregation', {}).get('columns', [
#             'finish_position', 'finish_time_seconds', 'margin_seconds', 'last_3f_time'
#         ])
        
#         # 集約ウィンドウ
#         windows = self.config.get('past_performance_aggregation', {}).get('windows', [1, 3, 5])
        
#         # horse_id ごとに過去走をソート
#         history = history.sort_values(by=['horse_id', 'race_date'], ascending=[True, True])
        
#         # --- グローバル集約 ---
        
#         if 'race_date' not in df.columns:
#              logging.error("shutuba_df (df) に 'race_date' がありません。過去走集約が正しく行えません。")
#              return df
        
#         df['race_date'] = pd.to_datetime(df['race_date']).dt.tz_localize(None)
        
#         # df (shutuba_df) と history_df を結合して、各レース時点での過去走を参照できるようにする
#         all_data = pd.concat([
#             df[['horse_id', 'race_date']].assign(is_target_race=1),
#             history[['horse_id', 'race_date'] + [col for col in agg_cols if col in history.columns]].assign(is_target_race=0)
#         ]).sort_values(by=['horse_id', 'race_date'], ascending=[True, True])
        
#         # horse_id ごとにグループ化
#         grouped = all_data.groupby('horse_id')
        
#         # shift(1) を使い、今走（または未来の走）を含めない過去N走の集約を行う
#         for col in agg_cols:
#             if col not in all_data.columns:
#                 continue
                
#             for w in windows:
#                 feat_name = f'past_{w}_{col}_mean'
#                 # 過去N走の平均
#                 all_data[feat_name] = grouped[col].shift(1).rolling(window=w, min_periods=1).mean()

#             # (2) Total Win Rate
#             if col == 'finish_position':
#                  all_data['is_win'] = (all_data['finish_position'] == 1).astype(int)
#                  # 過去の通算勝率
#                  all_data['career_win_rate'] = grouped['is_win'].shift(1).expanding().mean()
        
#         # (3) Days Since Last Race
#         # diff() は直前の行との差を計算
#         all_data['days_since_last_race_diff'] = grouped['race_date'].diff().dt.days.abs()
#         # shift(1) を使って、今走時点での「前走との間隔」を取得
#         all_data['days_since_last_race'] = grouped['days_since_last_race_diff'].shift(1)


#         # 集約結果を df (shutuba_df) にマージ
#         target_races = all_data[all_data['is_target_race'] == 1].copy()
        
#         merge_cols = [col for col in target_races.columns if col.startswith('past_') or col == 'career_win_rate' or col == 'days_since_last_race']
        
#         # 重複カラムを避けるため、マージ前にdfから削除（既にあれば）
#         cols_to_drop_from_df = [col for col in merge_cols if col in df.columns]
#         if cols_to_drop_from_df:
#             df = df.drop(columns=cols_to_drop_from_df)
            
#         df = df.merge(
#             target_races[['horse_id', 'race_date'] + merge_cols],
#             on=['horse_id', 'race_date'],
#             how='left'
#         )

#         return df

#     def _add_pedigree_features(self, df: pd.DataFrame, pedigree_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 pedigree_features に基づく血統特徴量
#         (簡易版: Target Encoding)
#         """
#         logging.debug("血統特徴量を追加中...")
        
#         if not self.config.get('pedigree_features', {}).get('enabled', False):
#             return df

#         # (1) history_df に pedigree_df をマージ
#         if 'horse_id' not in history_df.columns or 'horse_id' not in pedigree_df.columns:
#             logging.warning("血統特徴量の生成に必要な horse_id がありません。")
#             return df
            
#         if 'finish_position' not in history_df.columns:
#              logging.warning("血統特徴量の生成に必要な 'finish_position' がないためスキップします。")
#              return df
             
#         history_with_ped = history_df.merge(pedigree_df, on='horse_id', how='left')

#         # (2) sire_id (父) ごとに集約 (平均着順)
#         if 'sire_id' in history_with_ped.columns:
#             # .mean() の前に数値型であることを確認
#             history_with_ped['finish_position'] = pd.to_numeric(history_with_ped['finish_position'], errors='coerce')
            
#             sire_encoding = history_with_ped.groupby('sire_id')['finish_position'].mean().reset_index()
#             sire_encoding = sire_encoding.rename(columns={'finish_position': 'sire_target_encoding'})
            
#             # (3) 元の df (shutuba_df) にマージ
#             #    (shutuba_df にも pedigree_df をマージしておく必要がある)
#             if 'sire_id' not in df.columns:
#                  df = df.merge(pedigree_df[['horse_id', 'sire_id', 'damsire_id']], on='horse_id', how='left')
                 
#             df = df.merge(sire_encoding, on='sire_id', how='left')

#         return df

#     def _add_jockey_trainer_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 jockey_trainer_features に基づく騎手・調教師特徴量
#         (簡易版: 過去の勝率)
#         """
#         logging.debug("騎手・調教師特徴量を追加中...")
        
#         if not self.config.get('jockey_trainer_features', {}).get('enabled', False):
#             return df
            
#         if 'jockey_id' not in history_df.columns or 'trainer_id' not in history_df.columns:
#             logging.warning("騎手・調教師特徴量の生成に必要なIDがありません。")
#             return df
            
#         if 'jockey_id' not in df.columns or 'trainer_id' not in df.columns:
#             logging.warning("df (shutuba_df) に jockey_id または trainer_id がありません。マージできません。")
#             return df

#         history_df['is_win'] = (history_df['finish_position'] == 1).astype(int)

#         # (1) 騎手勝率 (リークを防ぐため expanding().mean().shift(1) を使うべきだが、簡易的に全体平均で代用)
#         jockey_win_rate = history_df.groupby('jockey_id')['is_win'].mean().reset_index()
#         jockey_win_rate = jockey_win_rate.rename(columns={'is_win': 'jockey_win_rate'})
        
#         # (2) 調教師勝率
#         trainer_win_rate = history_df.groupby('trainer_id')['is_win'].mean().reset_index()
#         trainer_win_rate = trainer_win_rate.rename(columns={'is_win': 'trainer_win_rate'})

#         # (3) df (shutuba_df) にマージ
#         df = df.merge(jockey_win_rate, on='jockey_id', how='left')
#         df = df.merge(trainer_win_rate, on='trainer_id', how='left')

#         return df

#     def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 within_race_normalization に基づくレース内正規化
#         (例: Z-score)
#         """
#         logging.debug("レース内正規化特徴量を追加中...")
        
#         if 'race_id' not in df.columns:
#              return df
             
#         grouped = df.groupby('race_id')
        
#         zscore_cols = self.config.get('within_race_normalization', {}).get('zscore_columns', [
#              'horse_weight', 'basis_weight'
#         ])
        
#         for col in zscore_cols:
#             if col in df.columns:
#                 feat_name = f'{col}_zscore'
#                 # transform を使ってレース内のZ-scoreを計算
#                 df[feat_name] = grouped[col].transform(
#                     lambda x: (x - x.mean()) / (x.std() + 1e-6)
#                 )
        
#         return df

#     def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
#         """
#         仕様書 6.2 missing_value_strategy に基づく欠損値処理
#         """
#         logging.debug("欠損値を処理中...")
        
#         strategy = self.config.get('missing_value_strategy', {})
#         num_strategy = strategy.get('numerical', {}).get('method', 'median')
        
#         # 数値カラムのみを対象
#         num_cols = df.select_dtypes(include=np.number).columns
        
#         for col in num_cols:
#             # キーや目的変数は除外
#             if col in ['race_id', 'horse_id', 'horse_number', 'finish_position', 'year', 'month', 'day']:
#                 continue
                
#             if num_strategy == 'median':
#                 median_val = df[col].median()
#                 if pd.isna(median_val):
#                     median_val = 0 # 全てNaNだった場合のフォールバック
#                 df[col] = df[col].fillna(median_val)
#             elif num_strategy == 'mean':
#                 mean_val = df[col].mean()
#                 if pd.isna(mean_val):
#                     mean_val = 0
#                 df[col] = df[col].fillna(mean_val)
#             elif num_strategy == 'zero':
#                  df[col] = df[col].fillna(0)
        
#         return df

#     def _select_features(self, df: pd.DataFrame) -> List[str]:
#         """
#         最終的に使用する特徴量のリストを確定する
#         """
#         all_cols = df.columns
        
#         # 元データ由来のカラム (キー、ターゲット、ID、文字列など)
#         # ★★★ 修正箇所 ★★★
#         # 'jockey_id', 'trainer_id' を除外リストから削除
#         exclude_cols = [
#             'race_id', 'horse_id', 'horse_number', 'horse_name', 'jockey_name',
#             'trainer_name', 'owner_name', 'sire_id', 'sire_name',
#             'dam_id', 'dam_name', 'damsire_id', 'damsire_name',
#             'finish_position', 'finish_time_str', 'margin_str', 'passing_order',
#             'race_date', 'race_date_str', 'year', 'month', 'day',
#             'sex', 'track_surface', 'track_condition', 'weather', 'sex_age', 'birth_date',
#             'coat_color', 'breeder_name', 'producing_area',
#             # 過去走集約の中間カラムも除外
#             'is_target_race', 'is_win', 'days_since_last_race_diff'
#         ]
        
#         # 過去走集約で生成された中間カラム (shift前のもの)
#         # (この簡易実装では中間カラムは生成していない)

#         feature_cols = [
#             col for col in all_cols 
#             if col not in exclude_cols 
#             and not col.endswith('_profile') # マージ時の重複カラム
#         ]
        
#         return feature_cols

#     def save_features(
#         self,
#         features_df: pd.DataFrame,
#         output_dir: str,
#         partition_cols: List[str]
#     ):
#         """
#         生成した特徴量をParquet形式で保存
#         """
#         output_path = Path(output_dir)
#         output_path.mkdir(parents=True, exist_ok=True)
        
#         try:
#             # (仕様書 17.2 の実装に合わせてパーティション保存)
#             if partition_cols and all(c in features_df.columns for c in partition_cols):
#                  features_df.to_parquet(
#                     output_path,
#                     engine='pyarrow',
#                     compression='snappy',
#                     partition_cols=partition_cols,
#                     existing_data_behavior='overwrite_or_ignore'
#                 )
#             else:
#                 # パーティションキーがない場合は単一ファイルで保存
#                  features_df.to_parquet(
#                     output_path / "features.parquet",
#                     engine='pyarrow',
#                     compression='snappy'
#                 )
#             logging.info(f"特徴量を {output_path} に保存しました")

#             # 特徴量リストを保存
#             features_list_path = output_path / "feature_names.yaml"
#             with open(features_list_path, 'w', encoding='utf-8') as f:
#                 yaml.dump(self.feature_names_, f)
#             logging.info(f"特徴量リストを {features_list_path} に保存しました")

#         except Exception as e:
#             logging.error(f"特徴量のParquet保存に失敗: {e}")
#             if "partition_cols" in str(e):
#                  logging.error("ヒント: パーティションカラム (year, month) がDataFrameに存在するか確認してください。")