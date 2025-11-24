#!/usr/bin/env python3
"""
mu_v2.0 追加特徴量モジュール
カテゴリ2〜6の特徴量を生成する
"""
import logging
import pandas as pd
import numpy as np
from typing import Dict

class MuV2Features:
    """μv2.0で追加する新特徴量を生成するクラス"""
    
    @staticmethod
    def add_condition_affinity_features(df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        カテゴリ2: 条件適性特徴量（15特徴量）
        馬・騎手の距離/馬場/競馬場/馬場状態/天候ごとの成績
        """
        logging.debug("カテゴリ2: 条件適性特徴量を追加中（mu_v2.0）...")
        
        if history_df.empty:
            logging.warning("履歴データが空のため、条件適性特徴量をスキップします。")
            return df
        
        # 履歴に勝利フラグと着順情報を追加
        history = history_df.copy()
        if 'finish_position' in history.columns:
            history['is_win'] = (history['finish_position'] == 1).fillna(False).astype(int)
            history['is_top3'] = (history['finish_position'] <= 3).fillna(False).astype(int)
        
        # 距離カテゴリを作成（100m刻み）
        if 'distance_m' in df.columns and 'distance_m' in history.columns:
            df['distance_bin'] = (df['distance_m'] // 100) * 100
            history['distance_bin'] = (history['distance_m'] // 100) * 100
            
            # 1-2. 距離別成績（当該距離±100m範囲）
            distance_stats = history.groupby(['horse_id', 'distance_bin']).agg({
                'is_win': 'mean',
                'finish_position': 'mean'
            }).reset_index()
            distance_stats.columns = ['horse_id', 'distance_bin', 'distance_win_rate', 'distance_avg_finish']
            
            df = df.merge(distance_stats, on=['horse_id', 'distance_bin'], how='left')
        
        # 3-4. 馬場別成績（芝/ダート）
        if 'track_surface' in df.columns and 'track_surface' in history.columns:
            surface_stats = history.groupby(['horse_id', 'track_surface']).agg({
                'finish_position': 'mean',
                'is_win': 'mean'
            }).reset_index()
            surface_stats.columns = ['horse_id', 'track_surface', 'surface_avg_finish', 'surface_win_rate']
            
            df = df.merge(surface_stats, on=['horse_id', 'track_surface'], how='left')
        
        # 5-6. 競馬場別成績
        if 'venue' in df.columns and 'venue' in history.columns:
            venue_stats = history.groupby(['horse_id', 'venue']).agg({
                'is_win': 'mean',
                'is_top3': 'mean'
            }).reset_index()
            venue_stats.columns = ['horse_id', 'venue', 'venue_win_rate', 'venue_top3_rate']
            
            df = df.merge(venue_stats, on=['horse_id', 'venue'], how='left')
        
        # 7-8. 馬場状態別成績
        if 'track_condition' in df.columns and 'track_condition' in history.columns:
            condition_stats = history.groupby(['horse_id', 'track_condition']).agg({
                'finish_position': 'mean',
                'is_win': 'mean'
            }).reset_index()
            condition_stats.columns = ['horse_id', 'track_condition', 'condition_avg_finish', 'condition_win_rate']
            
            df = df.merge(condition_stats, on=['horse_id', 'track_condition'], how='left')
        
        # 9-10. 天候別成績
        if 'weather' in df.columns and 'weather' in history.columns:
            weather_stats = history.groupby(['horse_id', 'weather']).agg({
                'is_win': 'mean',
                'finish_position': 'mean'
            }).reset_index()
            weather_stats.columns = ['horse_id', 'weather', 'weather_win_rate', 'weather_avg_finish']
            
            df = df.merge(weather_stats, on=['horse_id', 'weather'], how='left')
        
        # 11-13. 騎手の条件適性（距離/馬場/競馬場）
        if 'jockey_id' in df.columns and 'jockey_id' in history.columns:
            # 騎手の距離別成績
            if 'distance_bin' in df.columns and 'distance_bin' in history.columns:
                jockey_distance = history.groupby(['jockey_id', 'distance_bin'])['is_win'].mean().reset_index()
                jockey_distance.columns = ['jockey_id', 'distance_bin', 'jockey_distance_win_rate']
                df = df.merge(jockey_distance, on=['jockey_id', 'distance_bin'], how='left')
            
            # 騎手の馬場別成績
            if 'track_surface' in df.columns and 'track_surface' in history.columns:
                jockey_surface = history.groupby(['jockey_id', 'track_surface'])['is_win'].mean().reset_index()
                jockey_surface.columns = ['jockey_id', 'track_surface', 'jockey_surface_win_rate']
                df = df.merge(jockey_surface, on=['jockey_id', 'track_surface'], how='left')
            
            # 騎手の競馬場別成績
            if 'venue' in df.columns and 'venue' in history.columns:
                jockey_venue = history.groupby(['jockey_id', 'venue'])['is_win'].mean().reset_index()
                jockey_venue.columns = ['jockey_id', 'venue', 'jockey_venue_win_rate']
                df = df.merge(jockey_venue, on=['jockey_id', 'venue'], how='left')
        
        # 14-15. 複合条件適性（距離×馬場、競馬場×馬場状態）
        if all(col in df.columns for col in ['distance_bin', 'track_surface']) and \
           all(col in history.columns for col in ['distance_bin', 'track_surface']):
            dist_surf = history.groupby(['horse_id', 'distance_bin', 'track_surface'])['is_win'].mean().reset_index()
            dist_surf.columns = ['horse_id', 'distance_bin', 'track_surface', 'dist_surf_win_rate']
            df = df.merge(dist_surf, on=['horse_id', 'distance_bin', 'track_surface'], how='left')
        
        # 不要なカラムを削除
        if 'distance_bin' in df.columns:
            df = df.drop('distance_bin', axis=1)
        
        logging.debug("カテゴリ2: 条件適性15特徴量の追加完了")
        return df
    
    @staticmethod
    def add_pace_affinity_features(df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        カテゴリ3: ペース適性特徴量（10特徴量）
        """
        logging.debug("カテゴリ3: ペース適性特徴量を追加中（mu_v2.0）...")
        
        if history_df.empty:
            return df
        
        history = history_df.copy()
        
        # 1-2. 馬の脚質（passing_orderから計算）
        if all(col in history.columns for col in ['horse_id', 'passing_order_2', 'passing_order_4']):
            # 前半と後半の位置取りの比率
            history['pass_ratio'] = history['passing_order_2'] / (history['passing_order_4'] + 1)
            
            running_style = history.groupby('horse_id').agg({
                'pass_ratio': 'mean',
                'passing_order_2': 'mean'
            }).reset_index()
            running_style.columns = ['horse_id', 'horse_running_style', 'horse_early_position']
            
            df = df.merge(running_style, on='horse_id', how='left')
        
        # 3-4. 上がり3F適性
        if 'last_3f_time' in history.columns:
            # 馬の平均上がり3F
            history_3f = history.groupby('horse_id')['last_3f_time'].mean().reset_index()
            history_3f.columns = ['horse_id', 'horse_avg_last_3f']
            df = df.merge(history_3f, on='horse_id', how='left')
        
        # 5. レースの予想ペース（出走馬の平均脚質から）
        if 'horse_running_style' in df.columns:
            df['race_expected_pace'] = df.groupby('race_id')['horse_running_style'].transform('mean')
        
        # 6-7. ペース適性（前半速いレースでの成績 vs 遅いレース）
        if 'passing_order_2' in history.columns and 'finish_position' in history.columns:
            # 前半速いレース（2コーナー時点で先頭集団）での成績
            history['early_race'] = (history['passing_order_2'] <= 3).fillna(False).astype(int)
            pace_stats = history.groupby(['horse_id', 'early_race'])['finish_position'].mean().unstack(fill_value=np.nan)
            if 0 in pace_stats.columns and 1 in pace_stats.columns:
                pace_stats['slow_pace_affinity'] = pace_stats[0]  # 後方スタート時の成績
                pace_stats['fast_pace_affinity'] = pace_stats[1]  # 先行時の成績
                pace_stats = pace_stats[['slow_pace_affinity', 'fast_pace_affinity']].reset_index()
                df = df.merge(pace_stats, on='horse_id', how='left')
        
        # 8-9. 位置取りの安定性
        if all(col in history.columns for col in ['horse_id', 'passing_order_2', 'passing_order_4']):
            position_std = history.groupby('horse_id').agg({
                'passing_order_2': 'std',
                'passing_order_4': 'std'
            }).reset_index()
            position_std.columns = ['horse_id', 'passing_order2_std', 'passing_order4_std']
            df = df.merge(position_std, on='horse_id', how='left')
        
        # 10. 差し脚スコア（後半追い上げ度）
        if all(col in history.columns for col in ['passing_order_2', 'passing_order_4']):
            history['closing_kick'] = history['passing_order_2'] - history['passing_order_4']
            closing_score = history.groupby('horse_id')['closing_kick'].mean().reset_index()
            closing_score.columns = ['horse_id', 'horse_closing_kick_avg']
            df = df.merge(closing_score, on='horse_id', how='left')
        
        logging.debug("カテゴリ3: ペース適性10特徴量の追加完了")
        return df
    
    @staticmethod  
    def add_combination_features(df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        カテゴリ4: 騎手・調教師の相性特徴量（8特徴量）
        """
        logging.debug("カテゴリ4: 騎手・調教師相性特徴量を追加中（mu_v2.0）...")
        
        if history_df.empty:
            return df
        
        history = history_df.copy()
        if 'finish_position' in history.columns:
            history['is_win'] = (history['finish_position'] == 1).fillna(False).astype(int)
        
        # 1-2. 騎手×馬の組み合わせ
        if all(col in df.columns for col in ['jockey_id', 'horse_id']) and \
           all(col in history.columns for col in ['jockey_id', 'horse_id']):
            jockey_horse = history.groupby(['jockey_id', 'horse_id']).agg({
                'finish_position': 'mean',
                'is_win': 'mean'
            }).reset_index()
            jockey_horse.columns = ['jockey_id', 'horse_id', 'jockey_horse_avg_finish', 'jockey_horse_win_rate']
            df = df.merge(jockey_horse, on=['jockey_id', 'horse_id'], how='left')
        
        # 3-4. 調教師×馬の組み合わせ
        if all(col in df.columns for col in ['trainer_id', 'horse_id']) and \
           all(col in history.columns for col in ['trainer_id', 'horse_id']):
            trainer_horse = history.groupby(['trainer_id', 'horse_id']).agg({
                'finish_position': 'mean',
                'is_win': 'mean'
            }).reset_index()
            trainer_horse.columns = ['trainer_id', 'horse_id', 'trainer_horse_avg_finish', 'trainer_horse_win_rate']
            df = df.merge(trainer_horse, on=['trainer_id', 'horse_id'], how='left')
        
        # 5-6. 騎手×調教師の組み合わせ
        if all(col in df.columns for col in ['jockey_id', 'trainer_id']) and \
           all(col in history.columns for col in ['jockey_id', 'trainer_id']):
            jockey_trainer = history.groupby(['jockey_id', 'trainer_id']).agg({
                'is_win': 'mean',
                'finish_position': 'mean'
            }).reset_index()
            jockey_trainer.columns = ['jockey_id', 'trainer_id', 'jockey_trainer_win_rate', 'jockey_trainer_avg_finish']
            df = df.merge(jockey_trainer, on=['jockey_id', 'trainer_id'], how='left')
        
        # 7-8. 騎手の継続騎乗効果
        if all(col in df.columns for col in ['horse_id', 'jockey_id']) and \
           all(col in history.columns for col in ['horse_id', 'jockey_id', 'race_date']):
            history_sorted = history.sort_values(['horse_id', 'race_date'])
            history_sorted['prev_jockey'] = history_sorted.groupby('horse_id')['jockey_id'].shift(1)
            history_sorted['jockey_継続'] = (history_sorted['jockey_id'] == history_sorted['prev_jockey']).fillna(False).astype(int)
            
            # 継続騎乗時の成績
            jockey_継続_stats = history_sorted.groupby(['horse_id', 'jockey_id', 'jockey_継続'])['finish_position'].mean().reset_index()
            
            # pivot_tableではなく、unstackを使用（より柔軟）
            jockey_継続_pivot = jockey_継続_stats.pivot_table(
                index=['horse_id', 'jockey_id'], 
                columns='jockey_継続', 
                values='finish_position',
                aggfunc='mean'
            ).reset_index()
            
            # カラム名を動的に設定（0と1が両方あるかチェック）
            # reset_index後のカラム名は ['horse_id', 'jockey_id', 0, 1] または ['horse_id', 'jockey_id', 0] など
            col_names = ['horse_id', 'jockey_id']
            if 0 in jockey_継続_pivot.columns:
                jockey_継続_pivot = jockey_継続_pivot.rename(columns={0: 'jockey_new_avg_finish'})
                col_names.append('jockey_new_avg_finish')
            if 1 in jockey_継続_pivot.columns:
                jockey_継続_pivot = jockey_継続_pivot.rename(columns={1: 'jockey_継続_avg_finish'})
                col_names.append('jockey_継続_avg_finish')
            
            # 存在するカラムのみを選択してマージ
            jockey_継続_pivot = jockey_継続_pivot[col_names]
            df = df.merge(jockey_継続_pivot, on=['horse_id', 'jockey_id'], how='left')
        
        logging.debug("カテゴリ4: 騎手・調教師相性8特徴量の追加完了")
        return df
    
    @staticmethod
    def add_pedigree_detailed_features(df: pd.DataFrame, pedigree_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        カテゴリ5: 血統詳細特徴量（適性ベース）
        父・母父の産駒成績（距離・馬場適性）を集計
        """
        logging.debug("カテゴリ5: 血統詳細特徴量を追加中（mu_v2.0 適性ベース）...")
        
        if pedigree_df.empty or history_df.empty:
            return df
        
        # 1. 血統データの横持ち変換（簡易版）
        # horse_idごとに generation=1, 2 の祖先IDを抽出
        # 順序: gen1=[父, 母], gen2=[父父, 父母, 母父, 母母] と仮定
        
        # 必要な世代のみ抽出
        peds = pedigree_df[pedigree_df['generation'].isin([1, 2])].copy()
        
        # グループ化してリストにする
        # 注意: groupbyの順序は元のデータの順序に依存するため、sort_values等はしない（元の順序を信じる）
        ped_map = {}
        
        # 処理速度向上のため、必要な馬（dfに含まれる馬 + historyに含まれる馬）のみに絞るのが理想だが
        # ここではシンプルに実装
        
        # horse_idごとの祖先リストを作成
        # pandasの機能でpivotする
        peds['seq'] = peds.groupby(['horse_id', 'generation']).cumcount()
        peds_pivot = peds.pivot_table(
            index='horse_id', 
            columns=['generation', 'seq'], 
            values='ancestor_id', 
            aggfunc='first'
        )
        
        # カラム名をフラットにする (1_0, 1_1, 2_0, ... 2_3)
        peds_pivot.columns = [f'gen{g}_{s}' for g, s in peds_pivot.columns]
        peds_pivot = peds_pivot.reset_index()
        
        # 必要なカラム: 父(gen1_0), 母父(gen2_2) ※0始まりの場合
        # gen1: 0=父, 1=母
        # gen2: 0=父父, 1=父母, 2=母父, 3=母母
        
        target_cols = {}
        if 'gen1_0' in peds_pivot.columns:
            target_cols['sire_id'] = 'gen1_0'
        if 'gen2_2' in peds_pivot.columns:
            target_cols['damsire_id'] = 'gen2_2'
            
        if not target_cols:
            logging.warning("血統データから父・母父を抽出できませんでした。")
            return df
            
        # IDカラムをリネームして抽出
        ped_ids = peds_pivot[['horse_id'] + list(target_cols.values())].rename(columns={v: k for k, v in target_cols.items()})
        
        # 2. 履歴データに血統IDをマージ
        history = history_df.copy()
        # horse_idの型合わせ（文字列に統一）
        history['horse_id'] = history['horse_id'].astype(str)
        ped_ids['horse_id'] = ped_ids['horse_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        history_with_ped = history.merge(ped_ids, on='horse_id', how='left')
        
        # 3. 適性集計
        # 距離区分を作成
        if 'distance_m' in history_with_ped.columns:
            history_with_ped['dist_cat'] = pd.cut(
                history_with_ped['distance_m'],
                bins=[0, 1400, 1800, 2200, 3000, 10000],
                labels=['sprint', 'mile', 'middle', 'long', 'ultra']
            )
        
        # 集計対象のID（父、母父）
        for id_col, prefix in [('sire_id', 'sire'), ('damsire_id', 'damsire')]:
            if id_col not in history_with_ped.columns:
                continue
                
            # 3-1. 距離別成績
            if 'dist_cat' in history_with_ped.columns and 'finish_position' in history_with_ped.columns:
                dist_stats = history_with_ped.groupby([id_col, 'dist_cat'])['finish_position'].mean().reset_index()
                dist_stats.columns = [id_col, 'dist_cat', f'{prefix}_dist_avg_finish']
                
                # dfにマージするための準備
                if 'distance_m' in df.columns:
                    df['dist_cat'] = pd.cut(
                        df['distance_m'],
                        bins=[0, 1400, 1800, 2200, 3000, 10000],
                        labels=['sprint', 'mile', 'middle', 'long', 'ultra']
                    )
                    
                    # dfにも血統IDをマージ
                    if id_col not in df.columns:
                        df = df.merge(ped_ids[['horse_id', id_col]], on='horse_id', how='left')
                    
                    df = df.merge(dist_stats, on=[id_col, 'dist_cat'], how='left')
                    
            # 3-2. 馬場別成績
            if 'track_surface' in history_with_ped.columns and 'finish_position' in history_with_ped.columns:
                surf_stats = history_with_ped.groupby([id_col, 'track_surface'])['finish_position'].mean().reset_index()
                surf_stats.columns = [id_col, 'track_surface', f'{prefix}_surf_avg_finish']
                
                if 'track_surface' in df.columns:
                    # dfにも血統IDをマージ（未実施なら）
                    if id_col not in df.columns:
                        df = df.merge(ped_ids[['horse_id', id_col]], on='horse_id', how='left')
                        
                    df = df.merge(surf_stats, on=[id_col, 'track_surface'], how='left')

        # 不要カラム削除
        cols_to_drop = ['dist_cat', 'sire_id', 'damsire_id']
        df = df.drop([c for c in cols_to_drop if c in df.columns], axis=1)
        
        logging.debug("カテゴリ5: 血統詳細特徴量の追加完了")
        return df
    
    @staticmethod
    def add_rest_interval_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        カテゴリ6: レース間隔特徴量（5特徴量）
        注: days_since_last_raceは既存の_add_past_performance_featuresで追加済み
        """
        logging.debug("カテゴリ6: レース間隔特徴量を追加中（mu_v2.0）...")
        
        # 1. 休養明けフラグ
        if 'days_since_last_race' in df.columns:
            df['is_after_long_rest'] = (df['days_since_last_race'] > 90).astype(int)
            df['is_after_mid_rest'] = ((df['days_since_last_race'] > 30) & (df['days_since_last_race'] <= 90)).astype(int)
        
        # 2. 連闘フラグ
        if 'days_since_last_race' in df.columns:
            df['is_consecutive_race'] = (df['days_since_last_race'] < 14).astype(int)
        
        # 3. 前走からの体重変化
        if 'horse_weight' in df.columns:
            df['weight_change_from_last'] = df.groupby('horse_id')['horse_weight'].diff()
        
        # 4. 体重変化率（パーセンテージ）
        if 'horse_weight' in df.columns and 'weight_change_from_last' in df.columns:
            prev_weight = df.groupby('horse_id')['horse_weight'].shift(1)
            df['weight_change_pct'] = (df['weight_change_from_last'] / (prev_weight + 1)) * 100
        
        logging.debug("カテゴリ6: レース間隔5特徴量の追加完了")
        return df
    
