"""
Corner Position Deep Analysis & Existing Feature Differentiation
=================================================================
corner_positions.parquetの高度活用と既存V15特徴量との差別化
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple

# ============================================================================
# 既存V15特徴量との比較・差別化
# ============================================================================

EXISTING_V15_FEATURES = """
【既存V15の55特徴量（推定）】

■ 馬累積統計 (Horse Cumulative Stats)
- horse_cum_wins: 累積勝利数
- horse_cum_win_rate: 累積勝率
- horse_last3_avg_finish: 直近3走平均着順

■ 騎手・調教師統計 (Jockey/Trainer Stats)
- jockey_win_rate: 騎手勝率
- trainer_win_rate: 調教師勝率
- gap_trainer_pop_lf: 調教師人気ギャップ
- gap_jockey_pop_lf: 騎手人気ギャップ

■ 血統統計 (Pedigree Stats)
- sire_win_rate_fixed: 父勝率（固定値）
- gap_sire_pop_lf: 父人気ギャップ

■ ポジショニング統計 (Positioning Stats)
- avg_c4_pos: 平均4コーナー順位
- horse_c4_gap_avg: 平均4コーナー馬身差
- front_runner_rate: 逃げ率

■ クラス変更 (Class Changes)
- class_change: クラス変更量
- is_class_up: 昇級フラグ

■ ペース・脚質 (Pace/Running Style)
- front_runner_competition: 逃げ馬競合
- pace_style_match_score: ペース適合スコア

■ 前走情報 (Previous Race)
- prev_finish_category: 前走着順カテゴリ

■ レース条件 (Race Conditions)
- distance_m: 距離
- track_condition: 馬場状態
- bracket_number: 枠番
"""

# ============================================================================
# 新特徴量と既存特徴量の差別化マトリックス
# ============================================================================

DIFFERENTIATION_MATRIX = """
┌──────────────────────────────┬──────────────────────────────┬─────────────────────────────┐
│ 新特徴量                      │ 最も近い既存特徴量            │ 差別化ポイント               │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ elo_rating                   │ horse_cum_win_rate           │ 相手の強さを考慮             │
│                              │                              │ 勝率だけでは見えない実力     │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ recency_bias_indicator       │ prev_finish_category         │ 市場の過剰反応を直接計測     │
│                              │                              │ 人気との乖離を数値化         │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ expected_pace                │ front_runner_competition     │ 全出走馬の脚質構成を考慮     │
│                              │                              │ ペースを予測値として出力     │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ horse_pace_preference        │ pace_style_match_score       │ 馬のペース別成績を直接分析   │
│                              │                              │ ハイ/スロー適性を定量化      │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ broodmare_sire_win_rate      │ sire_win_rate_fixed          │ 母父の影響を独立して捕捉     │
│                              │                              │ スタミナ・気性の遺伝         │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ gap_progression_pattern      │ horse_c4_gap_avg             │ C1→C2→C3→C4の推移パターン    │
│                              │                              │ 追い込みのタイミングを識別   │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ class_change_history_score   │ is_class_up                  │ 過去の昇級時成績を考慮       │
│                              │                              │ 昇級の「成功率」を予測       │
├──────────────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ speed_figure_standardized    │ horse_last3_avg_finish       │ 走破タイムを条件補正         │
│                              │                              │ 「速さ」を絶対値で評価       │
└──────────────────────────────┴──────────────────────────────┴─────────────────────────────┘
"""

# ============================================================================
# Corner Position詳細活用（gap_from_leaderの高度分析）
# ============================================================================

class CornerPositionAnalyzer:
    """
    corner_positions.parquetの詳細活用
    
    データ構造:
    - race_id: レースID
    - corner: コーナー番号 (1-4)
    - horse_number: 馬番
    - position: 通過順位
    - gap_from_leader: 先頭からの累積馬身差
    
    ギャップ記号:
    - () 内: 0馬身（並走）
    - なし: +1.0馬身（デフォルト）
    - ,: +1.5馬身（1-2馬身差）
    - -: +3.5馬身（2-5馬身差）
    - =: +7.0馬身（5馬身以上）
    """
    
    @staticmethod
    def gap_progression_features(
        horse_id: str,
        corners_df: pd.DataFrame,
        races_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        馬身差の推移パターン分析（既存c4_gap_avgの大幅強化版）
        
        新規追加特徴量:
        1. gap_c1_avg: 1コーナー平均馬身差
        2. gap_c2_avg: 2コーナー平均馬身差
        3. gap_c3_avg: 3コーナー平均馬身差
        4. gap_c4_avg: 4コーナー平均馬身差（既存強化）
        5. gap_reduction_c1_c4: C1→C4での馬身差縮小量
        6. gap_reduction_c3_c4: C3→C4での馬身差縮小量（直線での追い込み力）
        7. gap_consistency: 馬身差の安定性（標準偏差）
        
        リーク防止:
        - 当該レースのコーナーデータは使用不可
        - 過去レースのコーナーデータのみ使用
        """
        # 馬の過去レースを取得
        horse_races = races_df[races_df['horse_id'] == horse_id]
        past_race_ids = horse_races['race_id'].unique()
        
        # 過去レースのコーナーデータを取得
        # horse_numberは各レースごとに異なるため、race_idとの結合が必要
        past_corners = corners_df[
            corners_df['race_id'].isin(past_race_ids)
        ]
        
        # race_idとhorse_numberの対応付けが必要
        # races_dfにhorse_numberがあると仮定
        horse_corners = past_corners.merge(
            horse_races[['race_id', 'horse_number']],
            on=['race_id', 'horse_number']
        )
        
        if len(horse_corners) < 12:  # 最低3レース×4コーナー
            return {f'gap_c{i}_avg': np.nan for i in range(1, 5)}
        
        # コーナー別平均馬身差
        features = {}
        for corner in [1, 2, 3, 4]:
            corner_data = horse_corners[horse_corners['corner'] == corner]
            if len(corner_data) > 0:
                features[f'gap_c{corner}_avg'] = corner_data['gap_from_leader'].mean()
                features[f'gap_c{corner}_std'] = corner_data['gap_from_leader'].std()
            else:
                features[f'gap_c{corner}_avg'] = np.nan
                features[f'gap_c{corner}_std'] = np.nan
        
        # 馬身差縮小量（追い込み力の指標）
        if not np.isnan(features.get('gap_c1_avg', np.nan)) and \
           not np.isnan(features.get('gap_c4_avg', np.nan)):
            features['gap_reduction_c1_c4'] = \
                features['gap_c1_avg'] - features['gap_c4_avg']
        else:
            features['gap_reduction_c1_c4'] = np.nan
        
        # 直線での追い込み力（C3→C4）
        if not np.isnan(features.get('gap_c3_avg', np.nan)) and \
           not np.isnan(features.get('gap_c4_avg', np.nan)):
            features['gap_reduction_c3_c4'] = \
                features['gap_c3_avg'] - features['gap_c4_avg']
        else:
            features['gap_reduction_c3_c4'] = np.nan
        
        return features
    
    @staticmethod
    def running_style_detailed(
        horse_id: str,
        corners_df: pd.DataFrame,
        races_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        脚質の詳細分類（既存front_runner_rateの拡張）
        
        脚質分類:
        1. 逃げ（C1で1番手）
        2. 先行（C1で2-4番手）
        3. 差し（C1で5-8番手、C4で上昇）
        4. 追込（C1で9番手以降、直線勝負）
        5. 自在（位置取りが一定しない）
        
        各脚質での成績も同時に計算
        """
        horse_races = races_df[races_df['horse_id'] == horse_id]
        past_race_ids = horse_races['race_id'].unique()
        
        # レースごとの脚質を判定
        styles = []
        style_results = {'逃げ': [], '先行': [], '差し': [], '追込': []}
        
        for race_id in past_race_ids:
            race_corners = corners_df[corners_df['race_id'] == race_id]
            race_info = horse_races[horse_races['race_id'] == race_id].iloc[0]
            horse_number = race_info['horse_number']
            
            c1_data = race_corners[
                (race_corners['corner'] == 1) &
                (race_corners['horse_number'] == horse_number)
            ]
            c4_data = race_corners[
                (race_corners['corner'] == 4) &
                (race_corners['horse_number'] == horse_number)
            ]
            
            if len(c1_data) == 0 or len(c4_data) == 0:
                continue
            
            c1_pos = c1_data['position'].values[0]
            c4_pos = c4_data['position'].values[0]
            finish_pos = race_info['finish_position']
            
            # 脚質判定
            if c1_pos == 1:
                style = '逃げ'
            elif c1_pos <= 4:
                style = '先行'
            elif c1_pos <= 8 and c4_pos < c1_pos:  # 途中で上がってきた
                style = '差し'
            else:
                style = '追込'
            
            styles.append(style)
            style_results[style].append(finish_pos)
        
        if len(styles) < 3:
            return {'dominant_style': None, 'style_consistency': np.nan}
        
        # 最も多い脚質
        from collections import Counter
        style_counts = Counter(styles)
        dominant_style = style_counts.most_common(1)[0][0]
        
        # 脚質の一貫性（同じ脚質の割合）
        style_consistency = style_counts.most_common(1)[0][1] / len(styles)
        
        # 各脚質での平均着順
        style_avg_pos = {}
        for style, results in style_results.items():
            if results:
                style_avg_pos[f'{style}_avg_pos'] = np.mean(results)
            else:
                style_avg_pos[f'{style}_avg_pos'] = np.nan
        
        return {
            'dominant_style': dominant_style,
            'style_consistency': style_consistency,
            '逃げ_rate': styles.count('逃げ') / len(styles),
            '先行_rate': styles.count('先行') / len(styles),
            '差し_rate': styles.count('差し') / len(styles),
            '追込_rate': styles.count('追込') / len(styles),
            **style_avg_pos,
        }
    
    @staticmethod
    def position_relative_to_winner(
        horse_id: str,
        corners_df: pd.DataFrame,
        races_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        勝馬との相対位置分析
        
        新規特徴量:
        1. avg_gap_to_winner_c4: 4コーナーで勝馬との馬身差
        2. catch_winner_rate: 4コーナーで勝馬より後ろから追い抜いた率
        3. hold_position_rate: 4コーナーで勝馬より前から逃げ切った率
        
        市場が見落とす理由:
        - 着順だけでは「惜しい競馬」が見えない
        - 勝馬に肉薄した経験は次走で活きる
        """
        horse_races = races_df[races_df['horse_id'] == horse_id]
        
        gaps_to_winner_c4 = []
        catch_count = 0
        hold_count = 0
        total_count = 0
        
        for _, race in horse_races.iterrows():
            race_id = race['race_id']
            horse_number = race['horse_number']
            finish_pos = race['finish_position']
            
            # 勝馬を特定
            winner = races_df[
                (races_df['race_id'] == race_id) &
                (races_df['finish_position'] == 1)
            ]
            if len(winner) == 0:
                continue
            winner_number = winner.iloc[0]['horse_number']
            
            # 4コーナーでの位置
            c4_data = corners_df[
                (corners_df['race_id'] == race_id) &
                (corners_df['corner'] == 4)
            ]
            
            horse_c4 = c4_data[c4_data['horse_number'] == horse_number]
            winner_c4 = c4_data[c4_data['horse_number'] == winner_number]
            
            if len(horse_c4) == 0 or len(winner_c4) == 0:
                continue
            
            horse_c4_gap = horse_c4['gap_from_leader'].values[0]
            winner_c4_gap = winner_c4['gap_from_leader'].values[0]
            
            gap_to_winner = horse_c4_gap - winner_c4_gap
            gaps_to_winner_c4.append(gap_to_winner)
            
            total_count += 1
            
            # 4コーナーで後ろから追い抜いて勝利/好走
            if gap_to_winner > 0 and finish_pos <= 3:
                catch_count += 1
            
            # 4コーナーで前から逃げ切り
            if gap_to_winner <= 0 and finish_pos <= 3:
                hold_count += 1
        
        return {
            'avg_gap_to_winner_c4': np.mean(gaps_to_winner_c4) if gaps_to_winner_c4 else np.nan,
            'catch_winner_rate': catch_count / total_count if total_count > 0 else np.nan,
            'hold_position_rate': hold_count / total_count if total_count > 0 else np.nan,
        }


# ============================================================================
# ラップタイム活用（race_details.parquet）
# ============================================================================

class LapTimeAnalyzer:
    """
    race_details.parquetのラップタイム活用
    
    データ構造:
    - lap_times: ラップタイムリスト (list[float])
    - first_half: 前半3F秒数
    - second_half: 後半3F秒数
    """
    
    @staticmethod
    def course_pace_tendency(
        race_id: str,
        venue: str,
        distance_m: int,
        track_surface: str,
        historical_race_details: pd.DataFrame,
        historical_races: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        コース別ペース傾向（同条件過去レースのペース分析）
        
        リーク防止:
        - 当該レースのペースは使用不可
        - 同条件の過去レースのペースを統計
        
        市場が見落とす理由:
        - 「中山1200mはハイペースになりやすい」など
        - コース特性は専門家でも定量化が難しい
        """
        # 同条件の過去レースを抽出
        same_condition_races = historical_races[
            (historical_races['venue'] == venue) &
            (historical_races['distance_m'] == distance_m) &
            (historical_races['track_surface'] == track_surface) &
            (historical_races['race_id'] != race_id)  # 当該レース除外
        ]['race_id'].unique()
        
        past_race_details = historical_race_details[
            historical_race_details['race_id'].isin(same_condition_races)
        ]
        
        if len(past_race_details) < 10:
            return {'course_pace_tendency': np.nan}
        
        # ペース比率（前半/後半）の統計
        pace_ratios = past_race_details['first_half'] / past_race_details['second_half']
        
        return {
            'course_pace_tendency': pace_ratios.mean(),  # >1ならハイペース傾向
            'course_pace_std': pace_ratios.std(),  # 大きいとペースが安定しない
            'course_high_pace_rate': (pace_ratios > 1.05).mean(),  # ハイペース率
            'course_slow_pace_rate': (pace_ratios < 0.95).mean(),  # スローペース率
        }


# ============================================================================
# 最終的な特徴量追加推奨リスト
# ============================================================================

FINAL_RECOMMENDATIONS = """
================================================================================
【最終推奨: V16特徴量追加リスト】
================================================================================

■ 優先度A（即座に追加推奨、高インパクト・低リスク）

1. elo_rating [NEW]
   - 実装難易度: 中
   - 既存補完: horse_cum_win_rateの上位互換
   - 期待効果: 相手の強さを考慮した実力評価

2. recency_bias_indicator [NEW]  
   - 実装難易度: 低
   - 既存補完: prev_finish_categoryを強化
   - 期待効果: 市場の過剰反応を直接検出

3. gap_c1_avg, gap_c2_avg, gap_c3_avg [NEW]
   - 実装難易度: 低
   - 既存補完: 既存c4_gap_avgを全コーナーに拡張
   - 期待効果: より詳細な位置取り分析

4. gap_reduction_c1_c4, gap_reduction_c3_c4 [NEW]
   - 実装難易度: 低
   - 既存補完: なし（新規観点）
   - 期待効果: 追い込み力の定量化

■ 優先度B（検証後追加、中インパクト）

5. broodmare_sire_win_rate [NEW]
   - 実装難易度: 中
   - 既存補完: sire_win_rate_fixedを血統深層化
   - 期待効果: 母父の影響を捕捉（特に長距離）

6. expected_front_runners (from running styles) [NEW]
   - 実装難易度: 中
   - 既存補完: front_runner_competitionを定量化
   - 期待効果: ペース予測の精度向上

7. course_pace_tendency [NEW]
   - 実装難易度: 低
   - 既存補完: なし（コース特性）
   - 期待効果: コース別の展開傾向を捕捉

8. 差し_rate, 追込_rate (detailed running style) [NEW]
   - 実装難易度: 低
   - 既存補完: front_runner_rateを4分類に拡張
   - 期待効果: 脚質のより詳細な分類

■ 優先度C（慎重に検証、リスクあり）

9. odds_movement (morning → final) [CAUTION]
   - リスク: リーク可能性高、学習時注意
   - 用途: リアルタイム予測専用として分離

10. speed_figure_standardized [COMPLEX]
    - 実装難易度: 高
    - 既存補完: タイム系特徴量なし
    - 期待効果: 絶対的な速さの指標

■ 削除検討（冗長可能性）

- 新特徴量追加後、相関分析を実施
- 相関 > 0.8 の既存特徴量は統合検討
- 特徴量重要度 < 0.01 は削除検討

================================================================================
【実装時の注意事項】
================================================================================

1. Point-in-Time設計の徹底
   - すべての累積統計にshift(1)を適用
   - 当該レース情報は一切使用しない
   - 「予測時に利用可能な情報のみ」を厳守

2. サンプル数の確保
   - 新特徴量の計算に最低3-5レースのデータを要求
   - サンプル不足時はNaN（欠損値）として扱う
   - LightGBMは欠損値を自動処理

3. 段階的追加
   - 一度に全特徴量を追加しない
   - 優先度Aを追加 → 検証 → 優先度B追加 → 検証
   - 各段階でROI/精度を確認

4. 過学習モニタリング
   - 学習/検証/テストの3分割を維持
   - 検証セットでの改善を確認してから採用
   - Out-of-Time（異なる年）での一貫性確認
"""

if __name__ == "__main__":
    print(DIFFERENTIATION_MATRIX)
    print(FINAL_RECOMMENDATIONS)
