import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

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
        
        # 直近N走の成績トレンド（groupby + apply で最適化）
        windows = [3, 5, 10]
        performance_sorted = performance_df.sort_values('race_date')
        
        for w in windows:
            self.logger.info(f"  → 直近{w}走の統計を計算中...")
            
            def calc_stats(group):
                recent = group.tail(w)
                if len(recent) == 0:
                    return pd.Series({
                        f'avg_finish_last{w}': np.nan,
                        f'win_rate_last{w}': np.nan,
                        f'place_rate_last{w}': np.nan,
                        f'improvement_rate_{w}': np.nan,
                        f'finish_std_last{w}': np.nan,
                        f'finish_cv_last{w}': np.nan
                    })
                
                avg_finish = recent['finish_position'].mean()
                win_rate = (recent['finish_position'] == 1).mean()
                place_rate = (recent['finish_position'] <= 2).mean()
                finish_std = recent['finish_position'].std()
                
                if len(recent) >= 2:
                    half = len(recent) // 2
                    first_avg = recent.iloc[:half]['finish_position'].mean()
                    second_avg = recent.iloc[half:]['finish_position'].mean()
                    improvement = (first_avg - second_avg) / first_avg if first_avg > 0 else 0
                else:
                    improvement = 0
                
                if avg_finish > 0:
                    finish_cv = finish_std / avg_finish
                else:
                    finish_cv = 0

                return pd.Series({
                    f'avg_finish_last{w}': avg_finish,
                    f'win_rate_last{w}': win_rate,
                    f'place_rate_last{w}': place_rate,
                    f'improvement_rate_{w}': improvement,
                    f'finish_std_last{w}': finish_std,
                    f'finish_cv_last{w}': finish_cv
                })
            
            trend_stats = performance_sorted.groupby('horse_id', observed=True).apply(calc_stats).reset_index()
            df = df.merge(trend_stats, on='horse_id', how='left')
        
        self.logger.info("✓ パフォーマンストレンド特徴量の生成完了")
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

        # 競馬場別成績
        # Note: morning_oddsを使用（win_oddsはデータリークを引き起こす）
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
        
        # 騎手×調教師のコンビ成績
        # Note: morning_oddsを使用（win_oddsはデータリークを引き起こす）
        agg_dict = {
            'finish_position': ['mean', 'count'],
            'popularity': 'mean'
        }

        # is_winがない場合は作成
        if 'is_win' not in historical_df.columns and 'finish_position' in historical_df.columns:
            historical_df = historical_df.copy()
            historical_df['is_win'] = (historical_df['finish_position'] == 1).astype(int)

        agg_dict = {
            'finish_position': ['mean', 'count'],
            'popularity': 'mean',
            'is_win': 'mean'  # この行を追加
        }

        if 'morning_odds' in historical_df.columns:
            agg_dict['morning_odds'] = 'mean'

        combo_stats = historical_df.groupby(['jockey_id', 'trainer_id'], observed=True).agg(agg_dict).reset_index()

        if 'morning_odds' in historical_df.columns:
            combo_stats.columns = ['jockey_id', 'trainer_id', 'combo_avg_finish',
                                  'combo_races', 'combo_avg_odds', 'combo_avg_popularity', 'combo_win_rate']
        else:
            combo_stats.columns = ['jockey_id', 'trainer_id', 'combo_avg_finish',
                                  'combo_races', 'combo_avg_popularity', 'combo_win_rate']
        
        # 期待値を上回る度合い
        combo_stats['combo_overperform'] = \
            combo_stats['combo_avg_popularity'] - combo_stats['combo_avg_finish']
        
        df = df.merge(combo_stats, on=['jockey_id', 'trainer_id'], how='left')
        
        return df
    
    def generate_bloodline_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """血統特徴量の生成"""
        
        # 父系の成績集計
        # Note: morning_oddsを使用（win_oddsはデータリークを引き起こす）
        perf_with_sire = performance_df.merge(
            pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']],
            on='horse_id'
        )

        agg_dict = {
            'finish_position': ['mean', 'std'],
            'distance_m': 'mean'
        }
        if 'morning_odds' in perf_with_sire.columns:
            agg_dict['morning_odds'] = 'mean'

        sire_stats = perf_with_sire.groupby('ancestor_id', observed=True).agg(agg_dict).reset_index()

        if 'morning_odds' in perf_with_sire.columns:
            sire_stats.columns = ['sire_id', 'sire_avg_finish', 'sire_std_finish',
                                 'sire_avg_distance', 'sire_avg_odds']
        else:
            sire_stats.columns = ['sire_id', 'sire_avg_finish', 'sire_std_finish',
                                 'sire_avg_distance']
        
        df = df.merge(sire_stats, left_on='sire_id', right_on='sire_id', how='left')
        
        return df

    def generate_deep_pedigree_features(
        self,
        df: pd.DataFrame,
        pedigree_df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """詳細な血統特徴量（ニックス、コース適性）"""

        try:
            # 1. ニックス（父×母父）
            # まず、各馬の父と母父を特定
            sires = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].rename(columns={'ancestor_id': 'sire_id'})
            damsires = pedigree_df[(pedigree_df['generation'] == 2) & (pedigree_df['ancestor_name'].str.contains('母', na=False))][['horse_id', 'ancestor_id']].rename(columns={'ancestor_id': 'damsire_id'})

            if sires.empty or damsires.empty:
                self.logger.warning("血統データ（父または母父）が見つかりません。血統特徴量をスキップします。")
                return df

            horse_pedigree = sires.merge(damsires, on='horse_id', how='inner')

            if horse_pedigree.empty:
                self.logger.warning("父×母父の組み合わせが見つかりません。血統特徴量をスキップします。")
                return df

            # パフォーマンスデータに血統情報を結合
            perf_ped = performance_df.merge(horse_pedigree, on='horse_id', how='inner')

            if perf_ped.empty or 'sire_id' not in perf_ped.columns or 'damsire_id' not in perf_ped.columns:
                self.logger.warning("血統情報のマージに失敗しました。血統特徴量をスキップします。")
                return df

            # ニックスごとの成績集計
            # Note: morning_oddsを使用（win_oddsはデータリークを引き起こす）
            agg_dict = {
                'finish_position': ['mean', 'count', 'std']
            }
            if 'morning_odds' in perf_ped.columns:
                agg_dict['morning_odds'] = 'mean'

            nicks_stats = perf_ped.groupby(['sire_id', 'damsire_id'], observed=True).agg(agg_dict).reset_index()

            if 'morning_odds' in perf_ped.columns:
                nicks_stats.columns = ['sire_id', 'damsire_id', 'nicks_avg_finish', 'nicks_count', 'nicks_std_finish', 'nicks_avg_odds']
            else:
                nicks_stats.columns = ['sire_id', 'damsire_id', 'nicks_avg_finish', 'nicks_count', 'nicks_std_finish']

            # 信頼度のため、ある程度の出走数があるもののみ採用
            nicks_stats = nicks_stats[nicks_stats['nicks_count'] >= 5]

            # メインデータフレームに結合（父・母父が必要）
            # dfにsire_id, damsire_idがある前提、なければ結合してから
            if 'sire_id' not in df.columns or 'damsire_id' not in df.columns:
                 df = df.merge(horse_pedigree, on='horse_id', how='left')

            df = df.merge(nicks_stats, on=['sire_id', 'damsire_id'], how='left')

            # 2. 種牡馬×コース適性
            # 種牡馬ごとの、競馬場・距離カテゴリ・芝ダート別の成績
            if 'distance_m' not in perf_ped.columns:
                self.logger.warning("distance_mカラムがありません。種牡馬×コース適性をスキップします。")
                return df

            perf_ped['distance_category'] = pd.cut(
                perf_ped['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 4000],
                labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
            )

            # 必要なカラムの存在チェック
            required_cols = ['sire_id', 'venue', 'distance_category', 'track_surface']
            missing_cols = [col for col in required_cols if col not in perf_ped.columns]
            if missing_cols:
                self.logger.warning(f"種牡馬×コース適性に必要なカラムがありません: {missing_cols}")
                return df

            # Note: morning_oddsを使用（win_oddsはデータリークを引き起こす）
            agg_dict = {'finish_position': 'mean'}
            if 'morning_odds' in perf_ped.columns:
                agg_dict['morning_odds'] = 'mean'

            sire_course_stats = perf_ped.groupby(['sire_id', 'venue', 'distance_category', 'track_surface'], observed=True).agg(agg_dict).reset_index()

            if 'morning_odds' in perf_ped.columns:
                sire_course_stats.columns = ['sire_id', 'venue', 'distance_category', 'track_surface', 'sire_course_avg_finish', 'sire_course_avg_odds']
            else:
                sire_course_stats.columns = ['sire_id', 'venue', 'distance_category', 'track_surface', 'sire_course_avg_finish']

            # メインデータフレームに結合
            # dfにdistance_categoryなどが必要
            if 'distance_category' not in df.columns:
                 if 'distance_m' in df.columns:
                     df['distance_category'] = pd.cut(
                        df['distance_m'],
                        bins=[0, 1400, 1800, 2200, 3000, 4000],
                        labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
                    )
                 else:
                     self.logger.warning("distance_mカラムがないため、distance_categoryを生成できません。")
                     return df

            df = df.merge(sire_course_stats, on=['sire_id', 'venue', 'distance_category', 'track_surface'], how='left')

            return df

        except Exception as e:
            self.logger.error(f"血統特徴量の生成中にエラー: {e}", exc_info=True)
            return df  # エラー時は元のDataFrameを返す

    def generate_course_bias_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """コースバイアス（枠順など）"""

        # 必要なカラムの存在チェック
        required_cols = ['venue', 'distance_m', 'track_surface', 'bracket_number', 'finish_position']
        missing_cols = [col for col in required_cols if col not in performance_df.columns]
        if missing_cols:
            self.logger.warning(f"コースバイアス特徴量に必要なカラムがありません: {missing_cols}")
            return df

        # データが少なすぎる場合はスキップ
        if len(performance_df) < 100:
            self.logger.warning(f"コースバイアス特徴量: データ数が少なすぎます（{len(performance_df)}行）。スキップします。")
            return df

        self.logger.info(f"コースバイアス特徴量を生成中... (データ数: {len(performance_df):,}行)")

        # 枠順バイアス
        # コース（競馬場、距離、芝ダート）ごとの枠番別成績

        performance_df['distance_category'] = pd.cut(
            performance_df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )

        # observed=True でパフォーマンス改善
        bracket_stats = performance_df.groupby(
            ['venue', 'distance_category', 'track_surface', 'bracket_number'],
            observed=True
        ).agg({
            'finish_position': 'mean'
        }).reset_index()
        bracket_stats.columns = ['venue', 'distance_category', 'track_surface', 'bracket_number', 'bracket_avg_finish']

        self.logger.info(f"枠順バイアス統計を計算完了: {len(bracket_stats):,}パターン")

        # メインデータフレームに結合
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
        # prize_1st または prize_money カラムを使用
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
            # デフォルト値を設定
            df['race_importance'] = 'medium'
            self.logger.warning("賞金カラム（prize_1st/prize_money）が見つかりません。race_importanceをデフォルト値に設定します。")
        
        return df
    
    def calculate_relative_metrics(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """レース内での相対的な指標（オッズ除外版）"""
        
        self.logger.info(f"レース内相対指標を生成中... (レース数: {df['race_id'].nunique():,})")
        
        # ⚠️ データリーク防止: オッズと結果データは除外
        # × finish_time_seconds → レース後にしか判明しない
        # × last_3f_time → レース後にしか判明しない
        # × オッズ (morning_odds / win_odds) → 学習には使わない（評価時のROI計算のみ使用）
        # ○ basis_weight → レース前に確定
        # ○ horse_weight → レース前に計測（当日朝）
        
        # groupby + transform で最適化（ループ不要）
        
        # 1. 斤量の相対値（レース内平均との差）
        if 'basis_weight' in df.columns:
            race_avg_weight = df.groupby('race_id', observed=True)['basis_weight'].transform('mean')
            df['weight_diff_from_avg'] = df['basis_weight'] - race_avg_weight
            self.logger.info("  ✓ 斤量の相対値を計算完了")
        else:
            self.logger.warning("basis_weightカラムがないため、weight_diff_from_avgをスキップします")
        
        # 2. 馬体重の相対値（レース内平均との差）
        if 'horse_weight' in df.columns:
            race_avg_hw = df.groupby('race_id', observed=True)['horse_weight'].transform('mean')
            df['horse_weight_diff_from_avg'] = df['horse_weight'] - race_avg_hw
            self.logger.info("  ✓ 馬体重の相対値を計算完了")
        
        self.logger.info("✓ レース内相対指標の生成完了（オッズ除外）")
        return df

    def generate_condition_change_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """条件変化特徴量（距離変更、馬場変更、場所変更）"""
        self.logger.info("条件変化特徴量を生成中...")
        
        # 時系列順にソートして前走データを取得
        last_race = performance_df.sort_values('race_date').groupby('horse_id').last().reset_index()
        
        # 必要なカラム: distance_m, track_surface, venue
        cols_to_use = ['horse_id', 'distance_m', 'track_surface', 'venue']
        last_race = last_race[cols_to_use].rename(columns={
            'distance_m': 'prev_distance_m',
            'track_surface': 'prev_track_surface',
            'venue': 'prev_venue'
        })
        
        df = df.merge(last_race, on='horse_id', how='left')
        
        # 距離変更
        if 'distance_m' in df.columns and 'prev_distance_m' in df.columns:
            df['distance_change'] = (df['distance_m'] - df['prev_distance_m']).abs()
            df['is_distance_shortened'] = (df['distance_m'] < df['prev_distance_m']).astype(int)
            df['is_distance_lengthened'] = (df['distance_m'] > df['prev_distance_m']).astype(int)
            
        # 馬場変更
        if 'track_surface' in df.columns and 'prev_track_surface' in df.columns:
            df['surface_change'] = (df['track_surface'] != df['prev_track_surface']).astype(int)
            
        # 場所変更
        if 'venue' in df.columns and 'prev_venue' in df.columns:
            df['venue_change'] = (df['venue'] != df['prev_venue']).astype(int)
            
        self.logger.info("✓ 条件変化特徴量の生成完了")
        return df
    
    def generate_rest_period_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """休養明け・間隔特徴量"""
        self.logger.info("休養明け特徴量を生成中...")
        
        # 前走日付の取得
        last_race_date = performance_df.sort_values('race_date').groupby('horse_id')['race_date'].last().reset_index()
        last_race_date = last_race_date.rename(columns={'race_date': 'prev_race_date'})
        
        df = df.merge(last_race_date, on='horse_id', how='left')
        
        # 休養明けフラグ
        if 'race_date' in df.columns and 'prev_race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            df['prev_race_date'] = pd.to_datetime(df['prev_race_date'])
            
            df['days_since_last_race'] = (df['race_date'] - df['prev_race_date']).dt.days
            
            # 休養明けフラグ (90日以上)
            df['is_rest_return'] = (df['days_since_last_race'] > 90).astype(int)
        
        self.logger.info("✓ 休養明け特徴量の生成完了")
        return df

    def generate_pace_features(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """展開・ペース特徴量（過去走データから計算）"""
        
        # ⚠️ データリーク防止: 現在のレースの passing_order や last_3f_time は使用しない
        # 過去の履歴データから各馬の傾向を計算する
        
        self.logger.info("ペース適性特徴量を生成中...")
        
        # 過去走からペース適性を計算（データリーク防止）
        def get_first_passing(s):
            if not isinstance(s, str): 
                return np.nan
            try:
                return int(s.split('-')[0])
            except:
                return np.nan
        
        if 'passing_order' in performance_df.columns and 'head_count' in performance_df.columns:
            performance_df = performance_df.copy()
            performance_df['passing_order_1'] = performance_df['passing_order'].apply(get_first_passing)
            performance_df['early_speed_index'] = performance_df['passing_order_1'] / performance_df['head_count']
            
            # 各馬の平均早期スピード指標
            horse_early = performance_df.groupby('horse_id', observed=True)['early_speed_index'].mean().reset_index()
            horse_early.columns = ['horse_id', 'horse_early_speed_index']
            
            df = df.merge(horse_early, on='horse_id', how='left')
        
        if 'last_3f_time' in performance_df.columns and 'distance_m' in performance_df.columns:
            performance_df = performance_df.copy()
            performance_df['late_speed_index'] = performance_df['last_3f_time'] / performance_df['distance_m']
            
            # 各馬の平均後期スピード指標
            horse_late = performance_df.groupby('horse_id', observed=True)['late_speed_index'].mean().reset_index()
            horse_late.columns = ['horse_id', 'horse_late_speed_index']
            
            df = df.merge(horse_late, on='horse_id', how='left')
        
        self.logger.info("✓ ペース適性特徴量の生成完了")
        return df