#!/usr/bin/env python3
# src/features/feature_engine.py
"""
特徴量生成エンジン
仕様書 6.3章 に基づく実装
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
            config: configs/features.yaml の内容
        """
        self.config = config
        self.feature_names_ = []
        logging.info("FeatureEngine (v1.0) が初期化されました")

    def generate_features(
        self,
        shutuba_df: pd.DataFrame,
        results_history_df: pd.DataFrame,
        horse_profiles_df: pd.DataFrame,
        pedigree_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        メインの特徴量生成関数
        
        Args:
            shutuba_df (pd.DataFrame): 対象レースの出馬表データ (日付フィルタ済み)
            results_history_df (pd.DataFrame): 過去の全レース結果
            horse_profiles_df (pd.DataFrame): 全馬のプロフィール
            pedigree_df (pd.DataFrame): 全馬の血統データ

        Returns:
            pd.DataFrame: 特徴量が付加されたDataFrame
        """
        logging.info("特徴量生成開始...")
        
        # (1) ベースとなる出馬表データを使用
        df = shutuba_df.copy()
        
        # (2) 馬のプロフィール情報をマージ (年齢、性別など)
        if not horse_profiles_df.empty:
            df = df.merge(
                horse_profiles_df,
                on='horse_id',
                how='left',
                suffixes=('', '_profile')
            )
        
        # (3) 過去の戦績から集計特徴量を生成
        if not results_history_df.empty:
            df = self._add_horse_history_features(df, results_history_df)
        # (もし history_df が空でも、_add_horse_history_features がデフォルト値 (0) を設定する)
        elif 'horse_id' in df.columns:
             # history がない場合 (新馬など) もカラムを 0 で初期化
             logging.warning("results_history_df が空です。賞金・キャリア特徴量を 0 で初期化します。")
             df['prize_total'] = 0.0
             df['career_starts'] = 0
             df['career_wins'] = 0


        # (4) 基本特徴量の生成 (仕様書 6.2 basic_features)
        df = self._add_basic_features(df)
        
        # (5) 過去走集約 (仕様書 6.2 past_performance_aggregation)
        if not results_history_df.empty:
            df = self._add_past_performance_features(df, results_history_df)

        # (6) 血統特徴量 (仕様書 6.2 pedigree_features)
        if not pedigree_df.empty:
            df = self._add_pedigree_features(df, pedigree_df, results_history_df)

        # (7) 騎手・調教師特徴量 (仕様書 6.2 jockey_trainer_features)
        if not results_history_df.empty:
             df = self._add_jockey_trainer_features(df, results_history_df)

        # (8) レース内正規化 (仕様書 6.2 within_race_normalization)
        df = self._add_relative_features(df)
        
        # (9) 欠損値処理 (仕様書 6.2 missing_value_strategy)
        df = self._handle_missing_values(df)

        # (10) 特徴量リストの確定
        self.feature_names_ = self._select_features(df)
        
        logging.info(f"特徴量生成完了: {len(self.feature_names_)}個の特徴量を生成")
        
        # 必要なカラム + 特徴量のみを返す
        key_cols = ['race_id', 'horse_id', 'horse_number', 'race_date']
        # (学習に必要な目的変数も残す)
        target_cols = ['finish_position', 'finish_time_seconds', 'win_odds', 'popularity']
        
        final_cols = key_cols + [col for col in target_cols if col in df.columns] + self.feature_names_
        # 重複を除外
        final_cols = list(dict.fromkeys(final_cols))
        
        # 最終カラムリストに存在しないカラムがdfにあれば警告 (デバッグ用)
        missing_in_final = [col for col in df.columns if col not in final_cols and col not in self.feature_names_]
        if missing_in_final and logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"最終返却DFに含まれないカラム: {missing_in_final}")

        # 最終カラムリストに含まれるカラムのみを厳選して返す
        return df[[col for col in final_cols if col in df.columns]]

    def _add_horse_history_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        馬の過去の全成績から集計特徴量を生成する (リーク対策版・欠損対応版)
        (prize_total, career_starts, career_wins など)
        """
        logging.debug("馬の過去成績集約特徴量を追加中...")
        
        # ===== ここから修正 (Phase 2-1) =====
        # prize_money が全て欠損しているか、history_df が空の場合はスキップ
        if history_df.empty or 'prize_money' not in history_df.columns:
            logging.warning("history_df に prize_money がないか空のため、賞金ベースの特徴量生成をスキップします")
            df['prize_total'] = 0.0  # デフォルト値
            df['career_starts'] = 0
            df['career_wins'] = 0
            return df

        # prize_money の有効率をチェック
        try:
            valid_prize_rate = history_df['prize_money'].notna().sum() / len(history_df)
            if valid_prize_rate < 0.1:  # 10%未満の場合
                logging.warning(f"prize_money の有効率が低すぎます ({valid_prize_rate:.1%})。賞金ベースの特徴量は信頼性が低いです。")
        except ZeroDivisionError:
             logging.warning("history_df は空（ZeroDivisionError）です。賞金ベースの特徴量生成をスキップします。")
             df['prize_total'] = 0.0
             df['career_starts'] = 0
             df['career_wins'] = 0
             return df
        # ===== ここまで修正 (Phase 2-1) =====
        
        # is_target_race フラグを準備
        # history_df にはないカラムを追加することで、マージ後の識別を容易にする
        history_df = history_df.copy()
        history_df['is_target_race'] = 0
        df = df.copy()
        df['is_target_race'] = 1
        
        # 結合して時系列に並べる
        # (df には prize_money がない場合があるため、先に history_df でチェックする)
        if 'prize_money' not in history_df.columns:
             history_df['prize_money'] = 0
        if 'prize_money' not in df.columns:
             df['prize_money'] = 0
             
        combined = pd.concat([history_df, df], ignore_index=True)
        # df と history_df に重複するレースがある場合、df側を優先
        combined = combined.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        
        # 日付でソートするためにdatetime型に変換
        combined['race_date'] = pd.to_datetime(combined['race_date']).dt.tz_localize(None)
        combined = combined.sort_values(by=['horse_id', 'race_date'], ascending=[True, True])
        
        # is_win と prize_money を準備
        combined['is_win'] = (combined['finish_position'] == 1).astype(int)
        
        # (fillna(0) は既に追加済み)
        combined['prize_money'] = combined['prize_money'].fillna(0)

        grouped = combined.groupby('horse_id', sort=False)
        
        # expanding/cumsum と shift を使って、各レース時点での過去の累計を計算
        combined['career_starts'] = grouped.cumcount() # 0から始まるので、これが過去の出走回数になる
        combined['career_wins'] = grouped['is_win'].cumsum().shift(1).fillna(0).astype(int)
        combined['prize_total'] = grouped['prize_money'].cumsum().shift(1).fillna(0)
        
        # 予測対象のレースデータに、計算した特徴量をマージ
        feature_cols = ['race_id', 'horse_id', 'career_starts', 'career_wins', 'prize_total']
        
        # 存在しないカラムを除外 (is_target_race == 1 のデータに prize_money がない場合など)
        cols_to_merge = [col for col in feature_cols if col in combined.columns]
        
        target_races_with_features = combined[combined['is_target_race'] == 1][cols_to_merge]
        
        # 元のdfからis_target_raceを削除
        if 'is_target_race' in df.columns:
            df = df.drop(columns=['is_target_race'])
        if 'prize_money' in df.columns and 'prize_money' not in shutuba_df.columns:
             # df に一時的に追加した prize_money を削除
             df = df.drop(columns=['prize_money'])
        
        # マージ
        df = df.merge(target_races_with_features, on=['race_id', 'horse_id'], how='left')
        
        return df

    def _add_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 basic_features に基づく基本特徴量の生成
        (例: カテゴリ変数のOne-Hotエンコーディング)
        """
        logging.debug("基本特徴量を追加中...")
        
        # (1) 性別 (One-Hot)
        if 'sex' in df.columns:
            sex_dummies = pd.get_dummies(df['sex'], prefix='sex', dtype=int)
            df = pd.concat([df, sex_dummies], axis=1)

        # (2) 斤量
        # (basis_weight は shutuba_parser.py ですでに数値化されている)
        
        # (3) レース基本情報 (distance_m, track_surface, head_count, bracket_number, horse_number)
        if 'track_surface' in df.columns:
             track_dummies = pd.get_dummies(df['track_surface'], prefix='track', dtype=int)
             df = pd.concat([df, track_dummies], axis=1)

        # (4) 枠番 (内枠・中枠・外枠)
        if 'bracket_number' in df.columns:
            df['bracket_is_inner'] = df['bracket_number'].isin([1, 2, 3]).astype(int)
            df['bracket_is_middle'] = df['bracket_number'].isin([4, 5, 6]).astype(int)
            df['bracket_is_outer'] = df['bracket_number'].isin([7, 8]).astype(int)

        return df

    def _add_past_performance_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 past_performance_aggregation に基づく過去走集約 (バグ修正版)
        """
        logging.debug("過去走集約特徴量を追加中...")

        if 'horse_id' not in history_df.columns or 'race_date' not in history_df.columns:
            logging.warning("history_df に 'horse_id' または 'race_date' がないため、過去走集約をスキップします。")
            return df

        # --- 特徴量計算 ---
        # 1. 履歴データと予測対象データを結合して、時系列でソート
        #    これにより、shift() を使って安全に過去のデータを参照できる
        history_df = history_df.copy() # 念のためコピー
        history_df['is_target_race'] = 0
        df = df.copy() # 念のためコピー
        df['is_target_race'] = 1
        
        combined = pd.concat([history_df, df], ignore_index=True)
        
        # race_id と horse_id で重複を削除 (df と history_df の重複分)
        combined = combined.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        
        combined['race_date'] = pd.to_datetime(combined['race_date']).dt.tz_localize(None)
        
        # 過去から未来へソート
        combined = combined.sort_values(by=['horse_id', 'race_date'], ascending=[True, True])

        # 2. horse_id でグループ化し、rolling/expanding で特徴量を計算
        grouped = combined.groupby('horse_id', sort=False) # sort=False でソート順を維持

        # 集約対象カラム
        agg_cols = self.config.get('past_performance_aggregation', {}).get('columns', [
            'finish_position', 'finish_time_seconds', 'margin_seconds', 'last_3f_time'
        ])
        windows = self.config.get('past_performance_aggregation', {}).get('windows', [1, 3, 5])

        for col in agg_cols:
            if col not in combined.columns:
                logging.warning(f"過去走集約: カラム '{col}' が combined に存在しません。スキップします。")
                continue
            
            # shift(1) を使い、今走を含めない過去の成績を参照する
            shifted = grouped[col].shift(1)
            
            for w in windows:
                feat_name = f'past_{w}_{col}_mean'
                # rolling の結果を元のインデックスに戻す (grouped.rolling はマルチインデックスを返すため)
                rolled_mean = shifted.rolling(window=w, min_periods=1).mean()
                # horse_id のインデックスを削除して、元の combined のインデックスに合わせる
                combined[feat_name] = rolled_mean.reset_index(level=0, drop=True)

        # 勝率
        if 'finish_position' in combined.columns:
            combined['is_win'] = (combined['finish_position'] == 1).astype(int)
            # expanding().mean() でキャリア勝率を計算し、shift()で今走を含めない
            career_win_rate = grouped['is_win'].expanding().mean().shift(1)
            career_win_rate = career_win_rate.reset_index(level=0, drop=True)
            combined['career_win_rate'] = career_win_rate

        # 前走からの日数
        combined['days_since_last_race'] = grouped['race_date'].diff().dt.days

        # 3. 計算した特徴量を元のdfに戻す
        #    is_target_race == 1 の行が、もともと予測対象だった行
        feature_cols_to_merge = [
            f'past_{w}_{c}_mean' for w in windows for c in agg_cols
        ] + ['career_win_rate', 'days_since_last_race']
        
        key_cols = ['race_id', 'horse_id']
        
        # 生成された特徴量カラムのみを抽出
        final_feature_cols = [col for col in feature_cols_to_merge if col in combined.columns]
        
        # 予測対象のレースデータに、計算した特徴量をマージ
        target_races_with_features = combined[combined['is_target_race'] == 1][key_cols + final_feature_cols]
        
        # 元のdfからis_target_raceを削除
        if 'is_target_race' in df.columns:
            df = df.drop(columns=['is_target_race'])
        
        # マージ
        df = df.merge(target_races_with_features, on=key_cols, how='left')

        return df

    def _add_pedigree_features(self, df: pd.DataFrame, pedigree_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 pedigree_features に基づく血統特徴量
        (簡易版: Target Encoding)
        """
        logging.debug("血統特徴量を追加中...")
        
        if not self.config.get('pedigree_features', {}).get('enabled', False):
            return df

        # (1) history_df に pedigree_df をマージ
        if 'horse_id' not in history_df.columns or 'horse_id' not in pedigree_df.columns:
            logging.warning("血統特徴量の生成に必要な horse_id がありません。")
            return df
            
        # 目的変数 (例: 賞金)
        if 'prize_money' not in history_df.columns:
             logging.warning("血統特徴量の生成に必要な 'prize_money' がないためスキップします。")
             return df
             
        history_with_ped = history_df.merge(pedigree_df, on='horse_id', how='left')

        # (2) sire_id (父) ごとに集約
        if 'sire_id' in history_with_ped.columns:
            # 欠損賞金を除外して平均を計算
            sire_encoding = history_with_ped.dropna(subset=['prize_money']).groupby('sire_id')['prize_money'].mean().reset_index()
            sire_encoding = sire_encoding.rename(columns={'prize_money': 'sire_target_encoding'})
            
            # (3) 元の df (shutuba_df) にマージ
            #    (shutuba_df にも pedigree_df をマージしておく必要がある)
            if 'sire_id' not in df.columns and 'horse_id' in df.columns:
                 # 必要なカラムのみマージ
                 ped_cols_to_merge = [col for col in ['horse_id', 'sire_id', 'damsire_id'] if col in pedigree_df.columns]
                 if 'horse_id' in ped_cols_to_merge:
                    df = df.merge(pedigree_df[ped_cols_to_merge], on='horse_id', how='left')
                 
            if 'sire_id' in df.columns and not sire_encoding.empty:
                 df = df.merge(sire_encoding, on='sire_id', how='left')

        return df

    def _add_jockey_trainer_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 jockey_trainer_features に基づく騎手・調教師特徴量
        (簡易版: 過去の勝率)
        """
        logging.debug("騎手・調教師特徴量を追加中...")
        
        if not self.config.get('jockey_trainer_features', {}).get('enabled', False):
            return df
            
        if 'jockey_id' not in history_df.columns or 'trainer_id' not in history_df.columns:
            logging.warning("騎手・調教師特徴量の生成に必要なIDがありません。")
            return df
            
        if 'finish_position' not in history_df.columns:
            logging.warning("騎手・調教師特徴量の生成に必要な 'finish_position' がありません。")
            return df

        history_df = history_df.copy() #
        history_df['is_win'] = (history_df['finish_position'] == 1).astype(int)

        # (1) 騎手勝率 (欠損を除外)
        jockey_win_rate = history_df.dropna(subset=['jockey_id', 'is_win']).groupby('jockey_id')['is_win'].mean().reset_index()
        jockey_win_rate = jockey_win_rate.rename(columns={'is_win': 'jockey_win_rate'})
        
        # (2) 調教師勝率 (欠損を除外)
        trainer_win_rate = history_df.dropna(subset=['trainer_id', 'is_win']).groupby('trainer_id')['is_win'].mean().reset_index()
        trainer_win_rate = trainer_win_rate.rename(columns={'is_win': 'trainer_win_rate'})

        # (3) df (shutuba_df) にマージ
        if 'jockey_id' in df.columns and not jockey_win_rate.empty:
            df = df.merge(jockey_win_rate, on='jockey_id', how='left')
        if 'trainer_id' in df.columns and not trainer_win_rate.empty:
            df = df.merge(trainer_win_rate, on='trainer_id', how='left')

        return df

    def _add_relative_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 within_race_normalization に基づくレース内正規化
        (例: Z-score)
        """
        logging.debug("レース内正規化特徴量を追加中...")
        
        if 'race_id' not in df.columns:
             return df
             
        grouped = df.groupby('race_id')
        
        zscore_cols = self.config.get('within_race_normalization', {}).get('zscore_columns', [
             'horse_weight', 'basis_weight', 'prize_total', 'career_starts', 'career_wins'
        ])
        
        for col in zscore_cols:
            if col in df.columns:
                feat_name = f'{col}_zscore'
                # transform を使ってレース内のZ-scoreを計算
                # (stdが0の場合に備えて 1e-6 を追加)
                try:
                    df[feat_name] = grouped[col].transform(
                        lambda x: (x - x.mean()) / (x.std() + 1e-6)
                    )
                except Exception as e:
                     logging.warning(f"Z-score ({col}) の計算に失敗: {e}")
                     df[feat_name] = 0.0 # 失敗した場合は 0 で埋める
        
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        仕様書 6.2 missing_value_strategy に基づく欠損値処理
        """
        logging.debug("欠損値を処理中...")
        
        strategy = self.config.get('missing_value_strategy', {})
        num_strategy = strategy.get('numerical', {}).get('method', 'median')
        
        # 数値カラムのみを対象
        num_cols = df.select_dtypes(include=np.number).columns
        
        # キーカラムとターゲットカラムは除外
        key_target_cols = [
            'race_id', 'horse_id', 'horse_number', 
            'finish_position', 'finish_time_seconds', 'win_odds', 'popularity'
        ]
        
        num_cols_to_fill = [col for col in num_cols if col not in key_target_cols]
        
        for col in num_cols_to_fill:
            # (col がそもそも df に存在しない場合もあるためチェック)
            if col not in df.columns:
                continue

            if df[col].isnull().any():
                if num_strategy == 'median':
                    median_val = df[col].median()
                    if pd.isna(median_val): # median が nan (全欠損など)
                        median_val = 0
                    df[col] = df[col].fillna(median_val)
                elif num_strategy == 'mean':
                    mean_val = df[col].mean()
                    if pd.isna(mean_val):
                        mean_val = 0
                    df[col] = df[col].fillna(mean_val)
                elif num_strategy == 'zero':
                     df[col] = df[col].fillna(0)
                
                # (デバッグ用) どのカラムがどの戦略で埋められたか
                # logging.debug(f"欠損値処理: '{col}' を '{num_strategy}' (値: {median_val or mean_val or 0}) で補完")
        
        return df

    def _select_features(self, df: pd.DataFrame) -> List[str]:
        """
        最終的に使用する特徴量のリストを確定する
        """
        all_cols = df.columns
        
        # 元データ由来のカラム (キー、ターゲット、ID、文字列など)
        # (仕様書 6.2.11)
        exclude_cols_base = [
            'race_id', 'horse_id', 'horse_number', 'horse_name', 'jockey_id', 'jockey_name',
            'trainer_id', 'trainer_name', 'owner_name', 'sire_id', 'sire_name',
            'dam_id', 'dam_name', 'damsire_id', 'damsire_name',
            'finish_position', 'finish_time_str', 'margin_str', 'passing_order',
            'race_date', 'race_date_str', 'year', 'month', 'day',
            'sex', 'track_surface', 'track_condition', 'weather', 'sex_age', 'birth_date',
            'coat_color', 'breeder_name', 'producing_area'
        ]
        
        # (仕様書 6.2.1) 基本特徴量で One-Hot/Label Encoding された元のカテゴリ変数
        exclude_cols_categorical_source = [
            'sex', 'track_surface', # (base に含まれているが明記)
            'bracket_number' # bracket_is_inner などに変換されたため
        ]
        
        # ターゲット変数 (特徴量ではない)
        exclude_cols_targets = [
             'finish_position', 'finish_time_seconds', 'win_odds', 'popularity',
             'margin_seconds', 'last_3f_time', # これらは過去走集約の *元* データ
             'is_win' # 中間生成カラム
        ]
        
        # マージ時に生成された重複カラム
        exclude_cols_suffix = ['_profile', '_pedigree', '_history']

        exclude_cols = set(exclude_cols_base + exclude_cols_categorical_source + exclude_cols_targets)

        feature_cols = []
        for col in all_cols:
            if col in exclude_cols:
                continue
            
            is_excluded_suffix = False
            for suffix in exclude_cols_suffix:
                if col.endswith(suffix):
                    is_excluded_suffix = True
                    break
            
            if is_excluded_suffix:
                continue
            
            # (数値型のみを特徴量とする場合)
            # if pd.api.types.is_numeric_dtype(df[col]):
            #     feature_cols.append(col)
            
            # (今回は型に関わらず、除外リストになければ特徴量とみなす)
            feature_cols.append(col)
            
        # 重複を除外
        feature_cols = sorted(list(dict.fromkeys(feature_cols)))
        
        return feature_cols

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
            # (仕様書 17.2 の実装に合わせてパーティション保存)
            # DataFrame に partition_cols が存在するか再確認
            valid_partition_cols = [col for col in partition_cols if col in features_df.columns]
            
            if valid_partition_cols:
                 logging.info(f"パーティションカラム {valid_partition_cols} を使用して保存します")
                 features_df.to_parquet(
                    output_path,
                    engine='pyarrow',
                    compression='snappy',
                    partition_cols=valid_partition_cols,
                    existing_data_behavior='overwrite_or_ignore'
                )
            else:
                # パーティションキーがない場合は単一ファイルで保存
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