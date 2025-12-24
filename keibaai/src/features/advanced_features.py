import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union
import logging
from tqdm import tqdm

class AdvancedFeatureEngine:
    """モデル精度向上のための高度な特徴量生成"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def generate_performance_trend_features(
        self, 
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """パフォーマンストレンド特徴量の生成（groupby最適化版）"""
        
        self.logger.info(f"パフォーマンストレンド特徴量を生成中... (馬数: {df['horse_id'].nunique():,}頭)")
        
        # ID型変換
        if 'horse_id' in df.columns:
            df['horse_id'] = df['horse_id'].astype(str)
        if 'horse_id' in performance_df.columns:
            performance_df = performance_df.copy()
            performance_df['horse_id'] = performance_df['horse_id'].astype(str)
        
        # 直近N走の成績トレンド
        windows = [3, 5, 10]
        performance_sorted = performance_df.sort_values('race_date')
        
        # is_win, is_place作成
        if 'is_win' not in performance_sorted.columns:
            performance_sorted['is_win'] = (performance_sorted['finish_position'] == 1).fillna(False).astype(int)
        performance_sorted['is_place'] = (performance_sorted['finish_position'] <= 3).fillna(False).astype(int)
        
        for w in windows:
            self.logger.info(f"  → 直近{w}走の統計を計算中...")
            
            # 各馬の直近w走を抽出
            recent = performance_sorted.groupby('horse_id', observed=True).tail(w)
            
            # 集計
            stats = recent.groupby('horse_id', observed=True).agg({
                'finish_position': ['mean', 'std'],
                'is_win': 'mean',
                'is_place': 'mean'
            })
            
            # カラム名のフラット化
            stats.columns = [
                f'avg_finish_last{w}', f'finish_std_last{w}',
                f'win_rate_last{w}', f'place_rate_last{w}'
            ]
            
            # 変動係数 (CV)
            stats[f'finish_cv_last{w}'] = stats[f'finish_std_last{w}'] / (stats[f'avg_finish_last{w}'] + 1e-6)
            
            stats = stats.reset_index()
            df = df.merge(stats, on='horse_id', how='left')
        
        self.logger.info("パフォーマンストレンド特徴量の生成完了")
        return df
    
    def generate_course_affinity_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """コース適性特徴量の生成"""

        # 必要なカラムの存在チェック
        required_cols = ['horse_id', 'venue', 'distance_m', 'track_surface', 'finish_position']
        missing_cols = [col for col in required_cols if col not in performance_df.columns]
        if missing_cols:
            self.logger.warning(f"コース適性特徴量に必要なカラムがありません: {missing_cols}")
            return df

        self.logger.info(f"コース適性特徴量を生成中... (データ数: {len(performance_df):,}行)")

        # ID型変換
        if 'horse_id' in df.columns:
            df['horse_id'] = df['horse_id'].astype(str)
        if 'horse_id' in performance_df.columns:
            performance_df = performance_df.copy()
            performance_df['horse_id'] = performance_df['horse_id'].astype(str)

        # 競馬場別成績
        agg_dict = {
            'finish_position': ['mean', 'count']
        }
        if 'morning_odds' in performance_df.columns:
            agg_dict['morning_odds'] = 'mean'

        venue_stats = performance_df.groupby(['horse_id', 'venue'], observed=True).agg(agg_dict).reset_index()

        if 'morning_odds' in performance_df.columns:
            venue_stats.columns = ['horse_id', 'venue', 'venue_avg_finish',
                                  'venue_races', 'venue_avg_odds']
        else:
            venue_stats.columns = ['horse_id', 'venue', 'venue_avg_finish',
                                  'venue_races']

        self.logger.info(f"競馬場別成績を計算完了: {len(venue_stats):,}パターン")

        # 距離別成績
        performance_df['distance_category'] = pd.cut(
            performance_df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )

        if 'finish_time_seconds' in performance_df.columns:
            distance_stats = performance_df.groupby(['horse_id', 'distance_category'], observed=True).agg({
                'finish_position': ['mean', 'count'],
                'finish_time_seconds': 'mean'
            }).reset_index()

            distance_stats.columns = ['horse_id', 'distance_category',
                                     'dist_avg_finish', 'dist_races', 'dist_avg_time']
        else:
            self.logger.warning("finish_time_secondsカラムがありません。タイム統計をスキップします。")
            distance_stats = performance_df.groupby(['horse_id', 'distance_category'], observed=True).agg({
                'finish_position': ['mean', 'count']
            }).reset_index()
            distance_stats.columns = ['horse_id', 'distance_category',
                                     'dist_avg_finish', 'dist_races']

        self.logger.info(f"距離別成績を計算完了: {len(distance_stats):,}パターン")

        # 馬場別成績
        if 'last_3f_time' in performance_df.columns:
            surface_stats = performance_df.groupby(['horse_id', 'track_surface'], observed=True).agg({
                'finish_position': ['mean', 'count'],
                'last_3f_time': 'mean'
            }).reset_index()

            surface_stats.columns = ['horse_id', 'track_surface',
                                    'surface_avg_finish', 'surface_races', 'surface_avg_last3f']
        else:
            self.logger.warning("last_3f_timeカラムがありません。上がり3F統計をスキップします。")
            surface_stats = performance_df.groupby(['horse_id', 'track_surface'], observed=True).agg({
                'finish_position': ['mean', 'count']
            }).reset_index()
            surface_stats.columns = ['horse_id', 'track_surface',
                                    'surface_avg_finish', 'surface_races']

        self.logger.info(f"馬場別成績を計算完了: {len(surface_stats):,}パターン")

        # メインデータフレームにマージ
        df = df.merge(venue_stats, on=['horse_id', 'venue'], how='left')
        df = df.merge(distance_stats, on=['horse_id', 'distance_category'], how='left')
        df = df.merge(surface_stats, on=['horse_id', 'track_surface'], how='left')

        self.logger.info("コース適性特徴量のマージ完了")

        return df
    
    def generate_jockey_trainer_synergy(
        self,
        df: pd.DataFrame,
        historical_df: pd.DataFrame
    ) -> pd.DataFrame:
        """騎手・調教師の相性特徴量"""
        self.logger.info(f"騎手×調教師相性特徴量を生成中... (データ数: {len(historical_df):,}行)")
        
        # IDカラムを文字列型に統一
        for col in ['jockey_id', 'trainer_id']:
            if col in df.columns:
                df[col] = df[col].astype(str)
            if col in historical_df.columns:
                historical_df[col] = historical_df[col].astype(str)

        # 騎手×調教師のコンビ成績
        agg_dict = {
            'finish_position': ['mean', 'count'],
            'is_win': 'mean'
        }

        # is_winがない場合は作成
        if 'is_win' not in historical_df.columns and 'finish_position' in historical_df.columns:
            historical_df = historical_df.copy()
            historical_df['is_win'] = (historical_df['finish_position'] == 1).fillna(False).astype(int)

        if 'popularity' in historical_df.columns:
            agg_dict['popularity'] = 'mean'

        if 'morning_odds' in historical_df.columns:
            agg_dict['morning_odds'] = 'mean'

        combo_stats = historical_df.groupby(['jockey_id', 'trainer_id'], observed=True).agg(agg_dict).reset_index()

        # カラム名の設定
        combo_stats.columns = ['_'.join(col).strip() if col[1] else col[0] for col in combo_stats.columns.values]
        
        # リネーム
        rename_map = {
            'jockey_id': 'jockey_id',
            'trainer_id': 'trainer_id',
            'finish_position_mean': 'combo_avg_finish',
            'finish_position_count': 'combo_races',
            'is_win_mean': 'combo_win_rate'
        }
        if 'popularity_mean' in combo_stats.columns:
            rename_map['popularity_mean'] = 'combo_avg_popularity'
        if 'morning_odds_mean' in combo_stats.columns:
            rename_map['morning_odds_mean'] = 'combo_avg_odds'
            
        combo_stats = combo_stats.rename(columns=rename_map)

        # 期待値超え率 (Overperform)
        if 'combo_avg_popularity' in combo_stats.columns:
            combo_stats['combo_overperform'] = \
                combo_stats['combo_avg_popularity'] - combo_stats['combo_avg_finish']

        # マージ
        df = df.merge(combo_stats, on=['jockey_id', 'trainer_id'], how='left')
        
        self.logger.info("騎手×調教師相性特徴量の生成完了")
        return df

    def generate_bloodline_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        historical_df: pd.DataFrame
    ) -> pd.DataFrame:
        """基本血統特徴量 (Sire/BMS成績)"""
        self.logger.info(f"基本血統特徴量(Sire/BMS)を生成中... (血統データ数: {len(pedigree_df):,}行)")

        # 1. 血統データの準備
        # 父 (generation=1)
        sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sire_df.columns = ['horse_id', 'sire_id']
        sire_df = sire_df.drop_duplicates(subset=['horse_id'])

        # 母父 (generation=2, 3番目)
        # groupby().nth(2) を使用して正確に母父を抽出
        damsire_df = pedigree_df[pedigree_df['generation'] == 2].copy()
        # horse_idごとにグループ化し、各グループの3番目の要素(index 2)を取得
        # ancestor_idの順序が保証されている前提 (父父, 父母, 母父, 母母)
        damsire_df = damsire_df.groupby('horse_id', as_index=False).nth(2)[['horse_id', 'ancestor_id']]
        damsire_df.columns = ['horse_id', 'damsire_id']

        # ID型変換
        sire_df['horse_id'] = sire_df['horse_id'].astype(str)
        sire_df['sire_id'] = sire_df['sire_id'].astype(str)
        damsire_df['horse_id'] = damsire_df['horse_id'].astype(str)
        damsire_df['damsire_id'] = damsire_df['damsire_id'].astype(str)

        # dfにマージ
        if 'sire_id' not in df.columns:
            df = df.merge(sire_df, on='horse_id', how='left')
        if 'damsire_id' not in df.columns:
            df = df.merge(damsire_df, on='horse_id', how='left')
        
        # 履歴データにもマージ（集計用）
        if 'sire_id' not in historical_df.columns:
            historical_df = historical_df.merge(sire_df, on='horse_id', how='left')
        if 'damsire_id' not in historical_df.columns:
            historical_df = historical_df.merge(damsire_df, on='horse_id', how='left')

        # is_winがない場合は作成
        if 'is_win' not in historical_df.columns and 'finish_position' in historical_df.columns:
            historical_df = historical_df.copy()
            historical_df['is_win'] = (historical_df['finish_position'] == 1).fillna(False).astype(int)

        # 2. 種牡馬成績の集計
        sire_stats = historical_df.groupby('sire_id', observed=True).agg({
            'finish_position': ['mean', 'count'],
            'is_win': 'mean'
        }).reset_index()
        sire_stats.columns = ['sire_id', 'sire_avg_finish', 'sire_races', 'sire_win_rate']
        
        # 信頼度フィルタ
        sire_stats.loc[sire_stats['sire_races'] < 10, ['sire_avg_finish', 'sire_win_rate']] = np.nan

        # 3. BMS (母父) 成績の集計
        damsire_stats = historical_df.groupby('damsire_id', observed=True).agg({
            'finish_position': ['mean', 'count'],
            'is_win': 'mean'
        }).reset_index()
        damsire_stats.columns = ['damsire_id', 'bms_avg_finish', 'bms_races', 'bms_win_rate']
        
        damsire_stats.loc[damsire_stats['bms_races'] < 10, ['bms_avg_finish', 'bms_win_rate']] = np.nan

        # マージ
        df = df.merge(sire_stats[['sire_id', 'sire_avg_finish', 'sire_win_rate']], on='sire_id', how='left')
        df = df.merge(damsire_stats[['damsire_id', 'bms_avg_finish', 'bms_win_rate']], on='damsire_id', how='left')

        self.logger.info("基本血統特徴量の生成完了")
        return df

    def generate_deep_pedigree_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        historical_df: pd.DataFrame
    ) -> pd.DataFrame:
        """詳細血統特徴量 (ニックス・コース適性)"""
        self.logger.info("詳細血統特徴量(Deep)を生成中...")

        # ID確保 (generate_bloodline_featuresで付与されているはずだが念のため)
        # historical_dfにも必要なので、どちらかに無ければ再取得・マージを行う
        if 'sire_id' not in df.columns or 'damsire_id' not in df.columns or \
           'sire_id' not in historical_df.columns or 'damsire_id' not in historical_df.columns:
            # 簡易的に再取得
            sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates(subset=['horse_id'])
            sire_df.columns = ['horse_id', 'sire_id']
            
            damsire_df = pedigree_df[pedigree_df['generation'] == 2].groupby('horse_id', as_index=False).nth(2)[['horse_id', 'ancestor_id']]
            damsire_df.columns = ['horse_id', 'damsire_id']
            
            # ID型変換
            sire_df['horse_id'] = sire_df['horse_id'].astype(str)
            sire_df['sire_id'] = sire_df['sire_id'].astype(str)
            damsire_df['horse_id'] = damsire_df['horse_id'].astype(str)
            damsire_df['damsire_id'] = damsire_df['damsire_id'].astype(str)
            
            if 'sire_id' not in df.columns:
                df = df.merge(sire_df, on='horse_id', how='left')
            if 'damsire_id' not in df.columns:
                df = df.merge(damsire_df, on='horse_id', how='left')
                
            # historical_dfにも
            if 'sire_id' not in historical_df.columns:
                historical_df = historical_df.merge(sire_df, on='horse_id', how='left')
            if 'damsire_id' not in historical_df.columns:
                historical_df = historical_df.merge(damsire_df, on='horse_id', how='left')

        # is_winがない場合は作成
        if 'is_win' not in historical_df.columns and 'finish_position' in historical_df.columns:
            historical_df = historical_df.copy()
            historical_df['is_win'] = (historical_df['finish_position'] == 1).fillna(False).astype(int)

        # 1. ニックス (Sire x Damsire)
        nicks_stats = historical_df.groupby(['sire_id', 'damsire_id'], observed=True).agg({
            'finish_position': ['mean', 'count'],
            'is_win': 'mean'
        }).reset_index()
        nicks_stats.columns = ['sire_id', 'damsire_id', 'nicks_avg_finish', 'nicks_races', 'nicks_win_rate']
        
        # 信頼度フィルタ
        nicks_stats.loc[nicks_stats['nicks_races'] < 5, ['nicks_avg_finish', 'nicks_win_rate']] = np.nan
        
        df = df.merge(nicks_stats[['sire_id', 'damsire_id', 'nicks_avg_finish', 'nicks_win_rate']], on=['sire_id', 'damsire_id'], how='left')

        # 2. 種牡馬 x コース適性 (Sire x Venue/Surface/Distance)
        # ここでは代表して Sire x Track Surface (芝/ダート) を実装
        sire_surface_stats = historical_df.groupby(['sire_id', 'track_surface'], observed=True).agg({
            'finish_position': ['mean', 'count'],
            'is_win': 'mean'
        }).reset_index()
        sire_surface_stats.columns = ['sire_id', 'track_surface', 'sire_course_avg_finish', 'sire_course_races', 'sire_course_win_rate']
        
        sire_surface_stats.loc[sire_surface_stats['sire_course_races'] < 10, ['sire_course_avg_finish', 'sire_course_win_rate']] = np.nan
        
        df = df.merge(sire_surface_stats[['sire_id', 'track_surface', 'sire_course_avg_finish', 'sire_course_win_rate']], on=['sire_id', 'track_surface'], how='left')

        self.logger.info("詳細血統特徴量の生成完了")
        return df

    def generate_course_bias_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """コースバイアス特徴量 (枠順有利不利など)"""
        self.logger.info(f"コースバイアス特徴量を生成中... (データ数: {len(performance_df):,}行)")

        performance_df['distance_category'] = pd.cut(
            performance_df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )

        bracket_stats = performance_df.groupby(
            ['venue', 'distance_category', 'track_surface', 'bracket_number'],
            observed=True
        ).agg({
            'finish_position': 'mean'
        }).reset_index()
        bracket_stats.columns = ['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_avg_finish']

        self.logger.info(f"枠順バイアス統計を計算完了: {len(bracket_stats):,}パターン")

        if 'distance_category' not in df.columns:
             df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )

        df = df.merge(bracket_stats, on=['venue', 'distance_category', 'track_surface', 'bracket_number'], how='left')

        self.logger.info("コースバイアス特徴量のマージ完了")

        return df
    
    def generate_race_condition_features(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """レース条件に関する特徴量"""
        
        # 1. フィールドサイズの影響
        if 'head_count' in df.columns:
            df['field_size_category'] = pd.cut(
                df['head_count'],
                bins=[0, 10, 14, 18, 24],
                labels=['small', 'medium', 'large', 'extra_large']
            )
        
        # 2. 季節性
        df['race_month'] = pd.to_datetime(df['race_date']).dt.month
        df['race_season'] = df['race_month'].map({
            12: 'winter', 1: 'winter', 2: 'winter',
            3: 'spring', 4: 'spring', 5: 'spring',
            6: 'summer', 7: 'summer', 8: 'summer',
            9: 'autumn', 10: 'autumn', 11: 'autumn'
        })
        
        # 3. レースの重要度（賞金ベース）
        prize_col = None
        if 'prize_1st' in df.columns:
            prize_col = 'prize_1st'
        elif 'prize_money' in df.columns:
            prize_col = 'prize_money'
        if prize_col:
            df['race_importance'] = df[prize_col].fillna(500).apply(
                lambda x: 'high' if x >= 2000 else ('medium' if x >= 1000 else 'low')
            )
        else:
            df['race_importance'] = 'medium'
            self.logger.warning("賞金カラム（prize_1st/prize_money）が見つかりません。race_importanceをデフォルト値に設定します。")
        
        return df
    
    def calculate_relative_metrics(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """レース内での相対的な指標（オッズ除外版）"""
        
        self.logger.info(f"レース内相対指標を生成中... (レース数: {df['race_id'].nunique():,})")
        
        # 1. 斤量の相対値（レース内平均との差）
        if 'basis_weight' in df.columns:
            race_avg_weight = df.groupby('race_id', observed=True)['basis_weight'].transform('mean')
            df['weight_diff_from_avg'] = df['basis_weight'] - race_avg_weight
            self.logger.info("  斤量の相対値を計算完了")
        else:
            self.logger.warning("basis_weightカラムがないため、weight_diff_from_avgをスキップします")
        
        # 2. 馬体重の相対値（レース内平均との差）
        if 'horse_weight' in df.columns:
            race_avg_hw = df.groupby('race_id', observed=True)['horse_weight'].transform('mean')
            df['horse_weight_diff_from_avg'] = df['horse_weight'] - race_avg_hw
            self.logger.info("  馬体重の相対値を計算完了")
        
        self.logger.info("レース内相対指標の生成完了（オッズ除外）")
        return df

    def generate_condition_change_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """条件変化特徴量（距離変更、馬場変更、場所変更）"""
        self.logger.info("条件変化特徴量を生成中...")
        
        last_race = performance_df.sort_values('race_date').groupby('horse_id').last().reset_index()
        
        cols_to_use = ['horse_id', 'distance_m', 'track_surface', 'venue']
        last_race = last_race[cols_to_use].rename(columns={
            'distance_m': 'prev_distance_m',
            'track_surface': 'prev_track_surface',
            'venue': 'prev_venue'
        })
        
        df = df.merge(last_race, on='horse_id', how='left')
        
        if 'distance_m' in df.columns and 'prev_distance_m' in df.columns:
            df['distance_change'] = (df['distance_m'] - df['prev_distance_m']).abs()
            df['is_distance_shortened'] = (df['distance_m'] < df['prev_distance_m']).fillna(False).astype(int)
            df['is_distance_lengthened'] = (df['distance_m'] > df['prev_distance_m']).fillna(False).astype(int)
            
        if 'track_surface' in df.columns and 'prev_track_surface' in df.columns:
            df['surface_change'] = (df['track_surface'] != df['prev_track_surface']).fillna(False).astype(int)
            
        if 'venue' in df.columns and 'prev_venue' in df.columns:
            df['venue_change'] = (df['venue'] != df['prev_venue']).fillna(False).astype(int)
            
        self.logger.info("条件変化特徴量の生成完了")
        return df
    
    def generate_rest_period_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """休養明け・間隔特徴量"""
        self.logger.info("休養明け特徴量を生成中...")
        
        last_race_date = performance_df.sort_values('race_date').groupby('horse_id')['race_date'].last().reset_index()
        last_race_date = last_race_date.rename(columns={'race_date': 'prev_race_date'})
        
        df = df.merge(last_race_date, on='horse_id', how='left')
        
        if 'race_date' in df.columns and 'prev_race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            df['prev_race_date'] = pd.to_datetime(df['prev_race_date'])
            
            df['days_since_last_race'] = (df['race_date'] - df['prev_race_date']).dt.days
            
            df['is_rest_return'] = (df['days_since_last_race'] > 90).fillna(False).astype(int)
        
        self.logger.info("休養明け特徴量の生成完了")
        return df

    def generate_seasonal_bias_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        季節性バイアス特徴量 (Seasonal Static Bias)
        過去の「同一開催回（round_of_year）」における傾向を集計。
        """
        self.logger.info("季節性バイアス特徴量を生成中...")
        
        # 必要なカラムの確認
        required_cols = ['venue', 'round_of_year', 'track_surface', 'distance_m', 'bracket_number', 'finish_position', 'track_condition']
        for col in required_cols:
            if col not in performance_df.columns:
                self.logger.warning(f"季節性バイアス: 必須カラム {col} が不足しています。スキップします。")
                return df

        # 距離カテゴリの作成（なければ）
        if 'distance_category' not in performance_df.columns:
            performance_df['distance_category'] = pd.cut(
                performance_df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )
        
        if 'distance_category' not in df.columns:
            df['distance_category'] = pd.cut(
                df['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )

        # 集計: 場所×開催回×馬場×距離×枠番×馬場状態
        # 平均着順と勝率（1着率）を計算
        # NaN処理を追加してTypeErrorを防止
        performance_df['is_win'] = (performance_df['finish_position'].fillna(0) == 1).astype(int)
        
        bias_stats = performance_df.groupby(
            ['venue', 'round_of_year', 'track_surface', 'distance_category', 'bracket_number', 'track_condition'],
            observed=True
        ).agg({
            'finish_position': 'mean',
            'is_win': 'mean'
        }).reset_index()
        
        bias_stats.columns = [
            'venue', 'round_of_year', 'track_surface', 'distance_category', 'bracket_number', 'track_condition',
            'seasonal_avg_finish', 'seasonal_win_rate'
        ]
        
        # マージ
        if 'track_condition' not in df.columns:
             self.logger.warning("季節性バイアス: Target DFに 'track_condition' がありません。バイアス特徴量はNaNになります。")
        
        df = df.merge(
            bias_stats,
            on=['venue', 'round_of_year', 'track_surface', 'distance_category', 'bracket_number', 'track_condition'],
            how='left'
        )
        
        # スコア化
        df['bias_seasonal_score'] = df['seasonal_win_rate'].fillna(0.05)
        
        # 不要な中間カラムを削除
        df.drop(columns=['seasonal_avg_finish', 'seasonal_win_rate'], inplace=True)
        
        return df

    def generate_dynamic_bias_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        動的バイアス特徴量 (Dynamic Rolling Bias)
        「同一開催（年・場所・回）」内の「当日以前」のレース結果を集計。
        """
        self.logger.info("動的バイアス特徴量を生成中...")
        
        # 必要なカラム
        req_cols = ['race_date', 'year', 'venue', 'round_of_year', 'day_of_meeting', 'track_surface', 'bracket_number', 'finish_position']
        
        # yearがない場合は作成
        if 'year' not in performance_df.columns:
            performance_df['year'] = pd.to_datetime(performance_df['race_date']).dt.year
        if 'year' not in df.columns:
            df['year'] = pd.to_datetime(df['race_date']).dt.year
            
        # ターゲットデータフレームのインデックスを保持
        df['__orig_index'] = df.index
        
        # 開催キー: year_venue_round_surface
        def make_meeting_key(row):
            return f"{row['year']}_{row['venue']}_{row['round_of_year']}_{row['track_surface']}"
            
        # 履歴データにキー付与
        performance_df['meeting_key'] = performance_df.apply(make_meeting_key, axis=1)
        # NaN処理を追加してTypeErrorを防止
        performance_df['is_win'] = (performance_df['finish_position'].fillna(0) == 1).astype(int)
        
        # 開催日ごとの集計データを作成
        daily_bracket_stats = performance_df.groupby(
            ['meeting_key', 'day_of_meeting', 'bracket_number']
        ).agg({
            'finish_position': ['sum', 'count'],
            'is_win': 'sum'
        }).reset_index()
        daily_bracket_stats.columns = ['meeting_key', 'day_of_meeting', 'bracket_number', 'finish_sum', 'count', 'win_sum']
        
        # 累積計算
        grouped = daily_bracket_stats.groupby(['meeting_key', 'bracket_number'])
        daily_bracket_stats['cum_finish_sum'] = grouped['finish_sum'].transform(lambda x: x.cumsum().shift(1))
        daily_bracket_stats['cum_count'] = grouped['count'].transform(lambda x: x.cumsum().shift(1))
        daily_bracket_stats['cum_win_sum'] = grouped['win_sum'].transform(lambda x: x.cumsum().shift(1))
        
        # 平均値算出
        daily_bracket_stats['dynamic_avg_finish'] = daily_bracket_stats['cum_finish_sum'] / daily_bracket_stats['cum_count']
        daily_bracket_stats['dynamic_win_rate'] = daily_bracket_stats['cum_win_sum'] / daily_bracket_stats['cum_count']
        
        df['meeting_key'] = df.apply(make_meeting_key, axis=1)
        
        # マージ
        df = df.merge(
            daily_bracket_stats[['meeting_key', 'day_of_meeting', 'bracket_number', 'dynamic_win_rate']],
            on=['meeting_key', 'day_of_meeting', 'bracket_number'],
            how='left'
        )
        
        # スコア化
        df['bias_dynamic_score'] = df['dynamic_win_rate'].fillna(0)
        
        # クリーンアップ
        df.drop(columns=['__orig_index', 'meeting_key', 'dynamic_win_rate'], inplace=True)
        
        return df

    def generate_pace_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        強化ペース特徴量
        レース内の先行馬の数などを計算し、展開適合スコアを算出。
        """
        self.logger.info("ペース特徴量を生成中...")
        
        # 前走の通過順位(passing_order_1)があることを前提
        # なければ計算できないのでスキップ
        if 'past_1_passing_order_1_mean' not in df.columns:
            # past_1_passing_order_1_mean がない場合、passing_order_1 (今回の結果) は使えない（リーク）
            # feature_engine で past_N 系が作られているはず
            self.logger.warning("ペース特徴量: 'past_1_passing_order_1_mean' が見つかりません。スキップします。")
            return df
            
        # レースごとの先行馬（通過順位平均 <= 3.0 と定義）の数をカウント
        # まず、各馬が先行型かどうかを判定
        df['is_leader_candidate'] = (df['past_1_passing_order_1_mean'] <= 3.0).astype(int)
        
        # レースIDごとに集計
        race_stats = df.groupby('race_id').agg({
            'is_leader_candidate': 'sum',
            'horse_id': 'count' # 頭数
        }).reset_index()
        race_stats.rename(columns={'is_leader_candidate': 'n_leaders', 'horse_id': 'n_horses'}, inplace=True)
        
        # 先行馬比率
        race_stats['leader_ratio'] = race_stats['n_leaders'] / race_stats['n_horses']
        
        # マージ
        df = df.merge(race_stats[['race_id', 'n_leaders', 'leader_ratio']], on='race_id', how='left')
        
        # 単騎逃げフラグ
        df['is_lone_leader'] = (df['n_leaders'] == 1).astype(int)
        
        # ペース適合スコア (Interaction)
        # 先行馬が多い(High Pace) -> 差し馬(passing_orderが大きい)が有利
        # 先行馬が少ない(Slow Pace) -> 先行馬(passing_orderが小さい)が有利
        
        # 自分の脚質（前走平均通過順位）
        my_style = df['past_1_passing_order_1_mean'].fillna(df['past_1_passing_order_1_mean'].mean())
        
        # ロジック:
        # Leader Ratioが高い（ハイペース）: my_styleが大きい（後ろ）ほどプラス
        # Leader Ratioが低い（スローペース）: my_styleが小さい（前）ほどプラス
        
        # 正規化
        normalized_style = (my_style - my_style.mean()) / (my_style.std() + 1e-6)
        normalized_pace = (df['leader_ratio'] - df['leader_ratio'].mean()) / (df['leader_ratio'].std() + 1e-6)
        
        # Interaction: Pace * Style
        # Pace(High=Pos) * Style(Back=Pos) -> Pos (High Pace favors Back)
        # Pace(Low=Neg) * Style(Front=Neg) -> Pos (Low Pace favors Front)
        # Pace(High=Pos) * Style(Front=Neg) -> Neg (High Pace disfavors Front)
        df['pace_fit_score'] = normalized_pace * normalized_style
        
        # 不要な一時カラム削除
        df.drop(columns=['is_leader_candidate'], inplace=True)
        
        return df

    def generate_breeder_producing_area_features(
        self,
        df: pd.DataFrame,
        horse_profiles_df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """生産者・産地特徴量"""
        self.logger.info("生産者・産地特徴量を生成中...")
        
        # プロファイル情報（生産者、産地）をパフォーマンスデータに結合
        cols_to_use = ['horse_id', 'breeder_name', 'producing_area']
        cols_to_merge = [c for c in cols_to_use if c in horse_profiles_df.columns]
        
        if len(cols_to_merge) < 2:
            self.logger.warning("生産者・産地情報が不足しています。スキップします。")
            return df
            
        perf_extended = performance_df.merge(horse_profiles_df[cols_to_merge], on='horse_id', how='left')
        
        # is_win
        perf_extended['is_win'] = (perf_extended['finish_position'] == 1).astype(int)
        
        # 1. 生産者別成績
        # データ数が少ない生産者は「その他」にまとめるなどの処理が必要だが、
        # ここでは単純に集計し、データ数(count)でフィルタリングするアプローチをとる
        breeder_stats = perf_extended.groupby('breeder_name', observed=True).agg({
            'finish_position': 'mean',
            'is_win': ['mean', 'count']
        }).reset_index()
        breeder_stats.columns = ['breeder_name', 'breeder_avg_finish', 'breeder_win_rate', 'breeder_race_count']
        
        # 信頼度のため、一定数以上の出走がある場合のみ採用
        breeder_stats.loc[breeder_stats['breeder_race_count'] < 10, ['breeder_avg_finish', 'breeder_win_rate']] = np.nan
        
        # 2. 産地別成績
        area_stats = perf_extended.groupby('producing_area', observed=True).agg({
            'finish_position': 'mean',
            'is_win': ['mean', 'count']
        }).reset_index()
        area_stats.columns = ['producing_area', 'area_avg_finish', 'area_win_rate', 'area_race_count']
        
        # マージ
        # dfにbreeder_name, producing_areaがない場合は追加
        if 'breeder_name' not in df.columns:
            df = df.merge(horse_profiles_df[['horse_id', 'breeder_name', 'producing_area']], on='horse_id', how='left')
            
        df = df.merge(breeder_stats[['breeder_name', 'breeder_avg_finish', 'breeder_win_rate']], on='breeder_name', how='left')
        df = df.merge(area_stats[['producing_area', 'area_avg_finish', 'area_win_rate']], on='producing_area', how='left')
        
        self.logger.info("生産者・産地特徴量の生成完了")
        return df

    def generate_recent_form_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """騎手・調教師の近走成績 (Recent Form)"""
        try:
            print("DEBUG: Entered generate_recent_form_features")
            self.logger.info("騎手・調教師の近走成績を生成中...")
            self.logger.info(f"Performance DF shape: {performance_df.shape}")

            # 日付でソート
            performance_df = performance_df.sort_values('race_date')
            
            # is_win作成
            if 'is_win' not in performance_df.columns:
                performance_df['is_win'] = (performance_df['finish_position'] == 1).fillna(False).astype(int)
            
            # 騎手・調教師ごとの累積成績を計算 (Expanding)
            
            # 騎手
            jockey_stats = performance_df.groupby('jockey_id')[['finish_position', 'is_win']].expanding().mean().reset_index()
            # level_1は元のindex
            jockey_stats = jockey_stats.set_index('level_1')
            
            # 元のDFに結合して、shift(1)することで「前走までの平均」にする
            performance_df['jockey_avg_finish_cum'] = jockey_stats['finish_position']
            performance_df['jockey_win_rate_cum'] = jockey_stats['is_win']
            
            # shift within group
            performance_df['jockey_recent_avg_finish'] = performance_df.groupby('jockey_id')['jockey_avg_finish_cum'].shift(1)
            performance_df['jockey_recent_win_rate'] = performance_df.groupby('jockey_id')['jockey_win_rate_cum'].shift(1)
            
            # 調教師も同様
            trainer_stats = performance_df.groupby('trainer_id')[['finish_position', 'is_win']].expanding().mean().reset_index()
            trainer_stats = trainer_stats.set_index('level_1')
            
            performance_df['trainer_avg_finish_cum'] = trainer_stats['finish_position']
            performance_df['trainer_win_rate_cum'] = trainer_stats['is_win']
            
            performance_df['trainer_recent_avg_finish'] = performance_df.groupby('trainer_id')['trainer_avg_finish_cum'].shift(1)
            performance_df['trainer_recent_win_rate'] = performance_df.groupby('trainer_id')['trainer_win_rate_cum'].shift(1)
            
            # 最新の値をdfにマージ
            # dfにあるhorse_id, race_date (or race_id) に対応する値をマージしたいが、
            # dfは予測対象のレース（未来）かもしれない
            # なので、jockey_id, trainer_idごとの「最新」の値を取得してマージする
            # ただし、これはリークの可能性がある（未来の成績が含まれる）
            # 正しくは、dfのrace_date時点での最新の値を取得する必要がある
            
            # ここでは簡易的に、dfとperformance_dfをマージして、対応する値を取得する
            # dfにjockey_id, trainer_idがある前提
            
            # performance_dfから必要なカラムだけ抽出
            cols = ['race_id', 'horse_id', 'jockey_recent_avg_finish', 'jockey_recent_win_rate', 'trainer_recent_avg_finish', 'trainer_recent_win_rate']
            
            # dfとperformance_dfを結合 (race_id, horse_idで一意に決まるはず)
            # もしdfがperformance_dfの一部ならこれでOK
            
            df = df.merge(performance_df[cols], on=['race_id', 'horse_id'], how='left')
            
            self.logger.info("近走成績特徴量の生成完了")
            return df
        except Exception as e:
            import traceback
            print(f"DEBUG: Critical error in generate_recent_form_features: {e}")
            traceback.print_exc()
            self.logger.error(f"Critical error in generate_recent_form_features: {e}")
            self.logger.error(traceback.format_exc())
            raise e

    def generate_jockey_venue_affinity(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """騎手×競馬場 相性特徴量"""
        print("DEBUG: Entered generate_jockey_venue_affinity")
        self.logger.info("騎手×競馬場相性特徴量を生成中...")
        
        try:
            # 騎手×競馬場ごとの成績
            agg_dict = {
                'finish_position': 'mean',
                'is_win': ['mean', 'count']
            }
            
            # is_win再確認
            if 'is_win' not in performance_df.columns:
                performance_df = performance_df.copy()
                performance_df['is_win'] = (performance_df['finish_position'] == 1).fillna(False).astype(int)
                
            affinity_stats = performance_df.groupby(['jockey_id', 'venue'], observed=True).agg(agg_dict).reset_index()
            affinity_stats.columns = ['jockey_id', 'venue', 'jockey_venue_avg_finish', 'jockey_venue_win_rate', 'jockey_venue_count']
            
            # 信頼度フィルタ (5走以上)
            affinity_stats.loc[affinity_stats['jockey_venue_count'] < 5, ['jockey_venue_avg_finish', 'jockey_venue_win_rate']] = np.nan
            
            df = df.merge(affinity_stats[['jockey_id', 'venue', 'jockey_venue_avg_finish', 'jockey_venue_win_rate']], on=['jockey_id', 'venue'], how='left')
            
            self.logger.info("騎手×競馬場相性特徴量の生成完了")
            return df
        except Exception as e:
            import traceback
            print(f"DEBUG: Critical error in generate_jockey_venue_affinity: {e}")
            traceback.print_exc()
            self.logger.error(f"Critical error in generate_jockey_venue_affinity: {e}")
            raise e

    # =========================================================================
    # V15 Legacy Features (Ported for v2.6 Restoration)
    # =========================================================================
    
    def generate_v15_legacy_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame,
        corners_df: Optional[pd.DataFrame] = None,
        min_corner_races: int = 5,
        front_runner_threshold: float = 0.3
    ) -> pd.DataFrame:
        """
        V15の重要特徴量を生成する
        
        【追加特徴量】
        - horse_c4_gap_avg: 過去C4馬身差平均
        - horse_relative_c4_avg: 過去相対C4位置平均
        - post_style_conflict: 馬番×脚質不適合スコア
        - race_front_runner_count: レース内逃げ予備軍数
        - front_runner_competition: 逃げ競合スコア
        
        【リーク対策】
        - 累積統計は expanding().mean().shift(1) で計算
        - 当該レースの情報は使用しない
        
        Args:
            df: 特徴量を追加するデータフレーム
            performance_df: 過去成績データ（race_date, horse_id, finish_position等）
            corners_df: コーナー通過データ（race_id, horse_number, corner, position, gap_from_leader）
            min_corner_races: C4統計の最小レース数要件
            front_runner_threshold: 逃げ予備軍判定閾値
        
        Returns:
            V15特徴量が追加されたデータフレーム
        """
        self.logger.info("V15レガシー特徴量を生成中...")
        
        # 1. C4馬身差・相対位置統計 (corners_dfが必要)
        if corners_df is not None and not corners_df.empty:
            df = self._calc_v15_c4_features(df, performance_df, corners_df, min_corner_races)
        else:
            self.logger.warning("corners_dfが未提供のため、C4特徴量をスキップします")
            df['horse_c4_gap_avg'] = np.nan
            df['horse_relative_c4_avg'] = np.nan
            df['post_style_conflict'] = np.nan
        
        # 2. 逃げ予備軍特徴量 (horse_front_runner_rateが必要)
        df = self._calc_v15_front_runner_features(df, front_runner_threshold)
        
        self.logger.info("V15レガシー特徴量の生成完了")
        return df
    
    def _calc_v15_c4_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame,
        corners_df: pd.DataFrame,
        min_corner_races: int
    ) -> pd.DataFrame:
        """
        C4馬身差平均と馬番×脚質不適合スコアを計算
        
        【リーク対策】
        - cumsum() + shift(1) で当該レースの情報を除外
        """
        self.logger.info("  C4馬身差・不適合スコアを計算中...")
        
        # C4データを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
        
        # 履歴データとマージ
        hist = performance_df[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数を追加
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        c4_with_id = c4.merge(hist, on=['race_id', 'horse_number'], how='left')
        c4_with_id = c4_with_id.dropna(subset=['horse_id'])
        c4_with_id = c4_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 相対C4位置を計算
        c4_with_id['relative_c4'] = c4_with_id['c4_pos'] / c4_with_id['field_size']
        
        # ★ リーク防止: 累積統計 + shift(1)
        c4_with_id['cum_gap_avg'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        c4_with_id['cum_count'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        c4_with_id['cum_relative_c4'] = c4_with_id.groupby('horse_id')['relative_c4'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 最小レース数フィルタ
        c4_with_id.loc[c4_with_id['cum_count'] < min_corner_races, 'cum_gap_avg'] = np.nan
        c4_with_id.loc[c4_with_id['cum_count'] < min_corner_races, 'cum_relative_c4'] = np.nan
        
        # 馬ごとの最新統計を取得
        latest = c4_with_id.groupby('horse_id').last()[['cum_gap_avg', 'cum_relative_c4']].reset_index()
        horse_c4_gap_stats = latest.set_index('horse_id')['cum_gap_avg'].to_dict()
        horse_relative_c4_stats = latest.set_index('horse_id')['cum_relative_c4'].to_dict()
        
        # dfにマップ
        df['horse_id'] = df['horse_id'].astype(str)
        df['horse_c4_gap_avg'] = df['horse_id'].map(horse_c4_gap_stats)
        df['horse_relative_c4_avg'] = df['horse_id'].map(horse_relative_c4_stats)
        
        # 馬番×脚質不適合スコア
        if 'field_size' not in df.columns:
            df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        df['_relative_post'] = df['horse_number'] / df['field_size']
        df['post_style_conflict'] = (df['_relative_post'] - df['horse_relative_c4_avg']).abs()
        df = df.drop(columns=['_relative_post'], errors='ignore')
        
        non_null = df['horse_c4_gap_avg'].notna().sum()
        self.logger.info(f"    horse_c4_gap_avg非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_v15_front_runner_features(
        self,
        df: pd.DataFrame,
        threshold: float
    ) -> pd.DataFrame:
        """
        逃げ予備軍数と競合スコアを計算
        
        【前提】
        - horse_front_runner_rate はすでにdfに存在する（過去累積統計）
        
        【リーク対策】
        - horse_front_runner_rate は shift 済みと仮定
        """
        self.logger.info("  逃げ予備軍特徴量を計算中...")
        
        df = df.copy()
        
        # horse_front_runner_rate がない場合はスキップ
        if 'horse_front_runner_rate' not in df.columns:
            self.logger.warning("horse_front_runner_rateがないため、逃げ予備軍特徴量をスキップします")
            df['race_front_runner_count'] = np.nan
            df['front_runner_competition'] = np.nan
            return df
        
        # 逃げ予備軍フラグ
        df['_is_front_candidate'] = (df['horse_front_runner_rate'] > threshold).astype(float)
        df['_is_front_candidate'] = df['_is_front_candidate'].fillna(0)
        
        # レース内逃げ予備軍数
        df['race_front_runner_count'] = df.groupby('race_id')['_is_front_candidate'].transform('sum')
        
        # 逃げ競合スコア（自分以外）
        df['front_runner_competition'] = df['race_front_runner_count'] - df['_is_front_candidate']
        
        # 一時カラムを削除
        df = df.drop(columns=['_is_front_candidate'], errors='ignore')
        
        self.logger.info(f"    race_front_runner_count平均: {df['race_front_runner_count'].mean():.2f}")
        
        return df