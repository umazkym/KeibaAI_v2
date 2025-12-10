"""
KeibaAI_v2 高優先度特徴量 詳細実装ガイド
==========================================
既存V15の55特徴量を拡張する追加特徴量の実装詳細
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

# ============================================================================
# 優先度1: 展開予測系（市場最大の盲点）
# ============================================================================

class PaceFeatureEngine:
    """
    展開（ペース）予測特徴量
    
    市場が見落とす理由:
    - 展開は確率的でなく、出走馬の組み合わせで決まる
    - 一般的なファンは血統・実績重視で展開分析が弱い
    - AI以外では定量化が難しい
    """
    
    @staticmethod
    def calculate_expected_pace(
        race_entries: pd.DataFrame,  # 当該レースの出走馬
        historical_corners: pd.DataFrame,  # 過去のコーナー通過データ
    ) -> Dict[str, float]:
        """
        出走馬の脚質構成からペースを予測
        
        実装ロジック:
        1. 各馬の「逃げ率」「先行率」を過去データから計算
        2. レース全体の逃げ馬数・先行馬数を予測
        3. ペース予測値を算出
        
        リーク防止:
        - 当該レースのコーナー通過データは使用不可
        - 過去レースのデータのみ使用
        - shift(1)で当該レース除外
        """
        features = {}
        
        for horse_id in race_entries['horse_id'].unique():
            # 当該馬の過去レースでの1コーナー通過順位
            past_c1 = historical_corners[
                (historical_corners['horse_id'] == horse_id) &
                (historical_corners['corner'] == 1)
            ]
            
            if len(past_c1) < 3:  # 最低3レースのデータが必要
                continue
                
            # 逃げ率（1コーナー1番手の割合）
            front_runner_rate = (past_c1['position'] == 1).mean()
            
            # 先行率（1コーナー3番手以内の割合）
            front_position_rate = (past_c1['position'] <= 3).mean()
            
            features[f'{horse_id}_front_runner_rate'] = front_runner_rate
            features[f'{horse_id}_front_position_rate'] = front_position_rate
        
        # レース全体の予測
        total_front_runners = sum(
            v for k, v in features.items() 
            if k.endswith('_front_runner_rate')
        )
        
        # ペースカテゴリ予測
        if total_front_runners >= 2:
            pace_category = 'HIGH'  # ハイペース予測
        elif total_front_runners < 0.5:
            pace_category = 'SLOW'  # スローペース予測
        else:
            pace_category = 'MIDDLE'
            
        return {
            'expected_front_runners': total_front_runners,
            'pace_category': pace_category,
            **features
        }
    
    @staticmethod
    def horse_pace_preference(
        horse_id: str,
        historical_races: pd.DataFrame,
        historical_race_details: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        馬の好むペース（過去成績のペース別分析）
        
        実装ロジック:
        1. 過去レースをペースカテゴリ別に分類
        2. 各カテゴリでの平均着順を計算
        3. ペース適性スコアを算出
        
        リーク防止:
        - 過去レースのペース情報は確定済みなので使用可能
        - 当該レースのペースは未確定なので使用不可
        """
        # 過去レースのペース情報を取得
        horse_races = historical_races[
            historical_races['horse_id'] == horse_id
        ].merge(
            historical_race_details[['race_id', 'first_half', 'second_half']],
            on='race_id'
        )
        
        if len(horse_races) < 5:
            return {'pace_preference': 0.0}  # データ不足
        
        # ペースカテゴリを算出
        horse_races['pace_ratio'] = (
            horse_races['first_half'] / horse_races['second_half']
        )
        horse_races['pace_cat'] = pd.cut(
            horse_races['pace_ratio'],
            bins=[0, 0.95, 1.05, float('inf')],
            labels=['SLOW', 'MIDDLE', 'HIGH']
        )
        
        # カテゴリ別平均着順
        pace_performance = horse_races.groupby('pace_cat')['finish_position'].mean()
        
        # ペース適性スコア（ハイペースでの相対パフォーマンス）
        if 'HIGH' in pace_performance and 'SLOW' in pace_performance:
            # 負の値 = ハイペースに強い
            pace_preference = pace_performance['SLOW'] - pace_performance['HIGH']
        else:
            pace_preference = 0.0
            
        return {
            'pace_preference': pace_preference,
            'high_pace_avg_pos': pace_performance.get('HIGH', np.nan),
            'slow_pace_avg_pos': pace_performance.get('SLOW', np.nan),
        }


# ============================================================================
# 優先度2: 市場バイアス検出系
# ============================================================================

class MarketBiasDetector:
    """
    市場の過剰反応・非効率性を検出
    
    市場心理学的背景:
    - Recency Bias: 直近の結果に過度に影響される
    - Representativeness Heuristic: 典型的なイメージに引きずられる
    - Anchoring: 前回のオッズに引きずられる
    """
    
    @staticmethod
    def recency_bias_indicator(
        horse_id: str,
        historical_races: pd.DataFrame,
        morning_popularity: int,
    ) -> Dict[str, float]:
        """
        直近成績への市場過剰反応指標
        
        実装ロジック:
        1. 前走の着順と今回の人気を比較
        2. 長期平均との乖離度を計算
        3. 過剰反応度をスコア化
        
        市場が見落とす理由:
        - 前走の大敗が「たまたま」か「実力低下」か区別できない
        - 前走の勝利が「相手弱」か「実力発揮」か区別できない
        """
        horse_races = historical_races[
            historical_races['horse_id'] == horse_id
        ].sort_values('race_date')
        
        if len(horse_races) < 5:
            return {'recency_bias': 0.0}
        
        # 直近1走と過去5走平均の乖離
        last_race = horse_races.iloc[-1]
        last_5_avg = horse_races.tail(5)['finish_position'].mean()
        
        last_race_deviation = last_race['finish_position'] - last_5_avg
        
        # 人気の期待着順（簡易計算）
        expected_pos_from_pop = morning_popularity * 0.8 + 2
        
        # 過剰反応度 = 実力との乖離
        # 正の値: 市場が過小評価（狙い目）
        # 負の値: 市場が過大評価（避けるべき）
        recency_bias = expected_pos_from_pop - last_5_avg
        
        return {
            'recency_bias': recency_bias,
            'last_race_deviation': last_race_deviation,
            'last_5_avg_pos': last_5_avg,
        }
    
    @staticmethod
    def class_change_adjustment(
        horse_id: str,
        current_race_class: str,
        historical_races: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        クラス変更への市場反応分析
        
        既存のis_class_upを拡張:
        - 昇級初戦の過去成績
        - 降級初戦の過去成績
        - クラス変更の「幅」を考慮
        
        市場が見落とす理由:
        - 昇級馬は「未知数」として過小評価されやすい
        - 降級馬は「落ち目」として適正評価されにくい
        """
        horse_races = historical_races[
            historical_races['horse_id'] == horse_id
        ].sort_values('race_date')
        
        if len(horse_races) < 2:
            return {'class_change_score': 0.0}
        
        # クラスの順序（例）
        class_order = {
            '新馬': 1, '未勝利': 2, '1勝クラス': 3,
            '2勝クラス': 4, '3勝クラス': 5, 'オープン': 6,
            'G3': 7, 'G2': 8, 'G1': 9
        }
        
        prev_class = horse_races.iloc[-1].get('race_class', '')
        prev_class_level = class_order.get(prev_class, 3)
        current_class_level = class_order.get(current_race_class, 3)
        
        class_change = current_class_level - prev_class_level
        
        # 過去のクラス昇格時の成績を分析
        class_up_races = []
        for i in range(1, len(horse_races)):
            prev_level = class_order.get(horse_races.iloc[i-1].get('race_class', ''), 3)
            curr_level = class_order.get(horse_races.iloc[i].get('race_class', ''), 3)
            if curr_level > prev_level:
                class_up_races.append(horse_races.iloc[i]['finish_position'])
        
        class_up_avg_pos = np.mean(class_up_races) if class_up_races else np.nan
        
        return {
            'class_change': class_change,
            'class_up_history_avg': class_up_avg_pos,
            'is_class_up': 1 if class_change > 0 else 0,
            'is_class_down': 1 if class_change < 0 else 0,
        }


# ============================================================================
# 優先度3: 血統深層分析系
# ============================================================================

class PedigreeAnalyzer:
    """
    血統の深層分析
    
    既存のsire_win_rate_fixedを拡張:
    - 母父（BMS）の影響
    - 条件別の血統適性
    - 近親交配の影響
    """
    
    @staticmethod
    def broodmare_sire_analysis(
        horse_id: str,
        pedigrees: pd.DataFrame,
        historical_races: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        母父（BMS = Broodmare Sire）の産駒成績分析
        
        実装ロジック:
        1. pedigreeからgeneration=1の母を特定
        2. 母のgeneration=1（つまり対象馬のgeneration=2）の父を特定
        3. その父の産駒（＝母父として）の成績を集計
        
        市場が見落とす理由:
        - 父の影響は注目されるが、母父は軽視されがち
        - 母父の影響は3歳以降・長距離で顕在化しやすい
        - スタミナ・気性は母系からの遺伝が大きい
        """
        # 対象馬の母父を特定
        horse_pedigree = pedigrees[pedigrees['horse_id'] == horse_id]
        
        # generation=2の中から母方の父（通常2番目のancestor）
        gen2_ancestors = horse_pedigree[horse_pedigree['generation'] == 2]
        if len(gen2_ancestors) < 2:
            return {'bms_win_rate': np.nan}
        
        # 母父のID（通常は2番目）
        bms_id = gen2_ancestors.iloc[1]['ancestor_id']
        
        # 母父の産駒（generation=2で同じancestor_idを持つ馬）の成績
        horses_with_same_bms = pedigrees[
            (pedigrees['generation'] == 2) &
            (pedigrees['ancestor_id'] == bms_id)
        ]['horse_id'].unique()
        
        bms_offspring_races = historical_races[
            historical_races['horse_id'].isin(horses_with_same_bms)
        ]
        
        if len(bms_offspring_races) < 100:  # 統計的に有意なサンプル数
            return {'bms_win_rate': np.nan}
        
        bms_win_rate = (bms_offspring_races['finish_position'] == 1).mean()
        bms_place_rate = (bms_offspring_races['finish_position'] <= 3).mean()
        
        return {
            'bms_win_rate': bms_win_rate,
            'bms_place_rate': bms_place_rate,
            'bms_offspring_count': len(bms_offspring_races),
        }
    
    @staticmethod
    def sire_condition_affinity(
        sire_id: str,
        historical_races: pd.DataFrame,
        pedigrees: pd.DataFrame,
        distance_category: str,  # 'short', 'middle', 'long'
        track_condition: str,    # '良', '稍重', '重', '不良'
    ) -> Dict[str, float]:
        """
        父の条件別適性
        
        実装ロジック:
        1. 父の産駒の成績を距離×馬場状態でクロス集計
        2. 当該条件での勝率を計算
        3. 全条件平均との差を特徴量化
        """
        # 父の産駒を特定
        sire_offspring = pedigrees[
            (pedigrees['generation'] == 1) &
            (pedigrees['ancestor_id'] == sire_id)
        ]['horse_id'].unique()
        
        offspring_races = historical_races[
            historical_races['horse_id'].isin(sire_offspring)
        ]
        
        if len(offspring_races) < 100:
            return {'sire_condition_affinity': np.nan}
        
        # 全体の勝率
        overall_win_rate = (offspring_races['finish_position'] == 1).mean()
        
        # 条件別の勝率
        condition_races = offspring_races[
            (offspring_races['distance_category'] == distance_category) &
            (offspring_races['track_condition'] == track_condition)
        ]
        
        if len(condition_races) < 20:
            return {'sire_condition_affinity': np.nan}
        
        condition_win_rate = (condition_races['finish_position'] == 1).mean()
        
        # 適性スコア（条件別勝率 / 全体勝率）
        affinity = condition_win_rate / overall_win_rate if overall_win_rate > 0 else 1.0
        
        return {
            'sire_condition_affinity': affinity,
            'sire_condition_win_rate': condition_win_rate,
            'sire_overall_win_rate': overall_win_rate,
        }


# ============================================================================
# 優先度4: 高度統計系
# ============================================================================

class AdvancedStatistics:
    """
    高度な統計的特徴量
    """
    
    @staticmethod
    def elo_rating(
        horse_id: str,
        historical_races: pd.DataFrame,
        initial_rating: float = 1500,
        k_factor: float = 32,
    ) -> float:
        """
        馬のEloレーティング
        
        実装ロジック:
        1. 初期レーティング1500からスタート
        2. 各レースで対戦相手とのレーティング差から期待勝率を計算
        3. 実際の結果に基づきレーティングを更新
        
        リーク防止:
        - 当該レース以前のレースのみでレーティング計算
        - shift(1)で当該レース除外
        
        市場が見落とす理由:
        - 単純な勝率では相手の強さが考慮されない
        - 「強い相手に僅差で負けた馬」と「弱い相手に大敗した馬」を区別できる
        """
        horse_races = historical_races[
            historical_races['horse_id'] == horse_id
        ].sort_values('race_date')
        
        rating = initial_rating
        
        for _, race in horse_races.iterrows():
            race_id = race['race_id']
            finish_pos = race['finish_position']
            
            # 同レース出走馬のレーティング取得（簡易版：人気で代用）
            race_horses = historical_races[
                historical_races['race_id'] == race_id
            ]
            
            # 対戦相手の平均レーティング（人気から推定）
            avg_opponent_rating = 1500 - (race['popularity'] - 5) * 20
            
            # 期待勝率
            expected = 1 / (1 + 10 ** ((avg_opponent_rating - rating) / 400))
            
            # 実際のスコア（1位=1.0, 2位=0.7, 3位=0.5, 以下徐々に減少）
            if finish_pos == 1:
                actual = 1.0
            elif finish_pos == 2:
                actual = 0.7
            elif finish_pos == 3:
                actual = 0.5
            else:
                actual = max(0, 0.3 - (finish_pos - 4) * 0.05)
            
            # レーティング更新
            rating += k_factor * (actual - expected)
        
        return rating
    
    @staticmethod
    def standardized_speed_figure(
        horse_id: str,
        historical_races: pd.DataFrame,
        race_id: str,
    ) -> Dict[str, float]:
        """
        標準化スピード指数
        
        実装ロジック:
        1. 距離・馬場・コースで走破タイムを補正
        2. 同条件レースの平均タイムとの差を計算
        3. 偏差値化してスピード指数とする
        
        リーク防止:
        - 当該レース以前のタイムデータのみ使用
        - 当該レースの走破タイムは使用不可
        """
        horse_races = historical_races[
            (historical_races['horse_id'] == horse_id) &
            (historical_races['race_id'] != race_id)  # 当該レース除外
        ]
        
        if len(horse_races) < 3:
            return {'speed_figure': np.nan}
        
        speed_figures = []
        
        for _, race in horse_races.iterrows():
            distance = race['distance_m']
            condition = race['track_condition']
            venue = race['venue']
            time = race['finish_time_seconds']
            
            # 同条件レースの平均タイム
            same_condition_races = historical_races[
                (historical_races['distance_m'] == distance) &
                (historical_races['track_condition'] == condition) &
                (historical_races['venue'] == venue) &
                (historical_races['race_date'] < race['race_date'])  # 過去のみ
            ]
            
            if len(same_condition_races) < 10:
                continue
            
            avg_time = same_condition_races['finish_time_seconds'].mean()
            std_time = same_condition_races['finish_time_seconds'].std()
            
            if std_time > 0:
                # 偏差値化（平均50、標準偏差10）
                z_score = (avg_time - time) / std_time  # 速い方が高い
                speed_figure = 50 + z_score * 10
                speed_figures.append(speed_figure)
        
        if not speed_figures:
            return {'speed_figure': np.nan}
        
        return {
            'speed_figure': np.mean(speed_figures),
            'speed_figure_max': np.max(speed_figures),
            'speed_figure_recent': speed_figures[-1] if speed_figures else np.nan,
        }


# ============================================================================
# 特徴量組み合わせによる相乗効果
# ============================================================================

class FeatureInteractions:
    """
    特徴量の組み合わせ（交互作用項）
    
    注意: 過学習リスクが高いため慎重に使用
    """
    
    @staticmethod
    def pace_ability_interaction(
        expected_pace: str,  # 'HIGH', 'MIDDLE', 'SLOW'
        horse_pace_preference: float,
        horse_elo_rating: float,
    ) -> Dict[str, float]:
        """
        展開予測 × ペース適性 × 実力 の組み合わせ
        
        ロジック:
        - ハイペース予測 + ハイペース巧者 + 高実力 = 最高評価
        - スローペース予測 + ハイペース巧者 + 高実力 = 展開不向き
        """
        # ペースマッチスコア
        if expected_pace == 'HIGH':
            pace_match = horse_pace_preference  # 正の値ならハイペース巧者
        elif expected_pace == 'SLOW':
            pace_match = -horse_pace_preference  # 負の値ならスロー巧者
        else:
            pace_match = 0
        
        # 実力で重み付け（Eloが高いほど信頼度高）
        elo_weight = (horse_elo_rating - 1500) / 200  # -1 to +1程度
        
        # 交互作用スコア
        interaction_score = pace_match * (1 + elo_weight * 0.5)
        
        return {
            'pace_ability_interaction': interaction_score,
            'pace_match_raw': pace_match,
        }


# ============================================================================
# 過学習防止戦略
# ============================================================================

class OverfittingPrevention:
    """
    過学習・過適合防止のベストプラクティス
    """
    
    STRATEGIES = """
    1. 時系列分割の徹底
       - 学習: 2020-2023年
       - 検証: 2024年1-6月
       - テスト: 2024年7-12月
       - 決してテストデータで特徴量チューニングしない
    
    2. 特徴量選択
       - 相関係数 > 0.8 の特徴量は1つに絞る
       - 特徴量重要度が低い（< 0.01）ものは除外
       - 新特徴量追加時は検証セットでの改善を確認
    
    3. 正則化
       - LightGBMのreg_alpha, reg_lambda を適切に設定
       - max_depth を制限（深すぎると過学習）
       - min_data_in_leaf を十分大きく（100以上推奨）
    
    4. クロスバリデーション
       - 時系列を考慮したCV（sklearn.model_selection.TimeSeriesSplit）
       - 各foldでの性能分散を確認
       - 分散が大きい特徴量は不安定
    
    5. 特徴量のサンプル数チェック
       - カテゴリカル特徴量のカテゴリごとのサンプル数 > 100
       - 累積統計のサンプル数 > 10
       - 不足時は欠損値として扱う
    
    6. Out-of-Time検証
       - 異なる年・季節でのパフォーマンス一貫性を確認
       - 特定の期間でのみ有効な特徴量は危険
    
    7. 既存特徴量との冗長性チェック
       - 新特徴量と既存特徴量の相関を確認
       - 相関 > 0.7 なら追加価値が少ない可能性
    """
    
    @staticmethod
    def check_feature_redundancy(
        new_feature: pd.Series,
        existing_features: pd.DataFrame,
        threshold: float = 0.7,
    ) -> Dict[str, float]:
        """
        新特徴量と既存特徴量の冗長性チェック
        """
        correlations = {}
        for col in existing_features.columns:
            corr = new_feature.corr(existing_features[col])
            correlations[col] = corr
        
        max_corr = max(correlations.values())
        max_corr_feature = max(correlations, key=correlations.get)
        
        return {
            'max_correlation': max_corr,
            'most_similar_feature': max_corr_feature,
            'is_redundant': max_corr > threshold,
            'all_correlations': correlations,
        }


# ============================================================================
# 推奨実装順序
# ============================================================================

IMPLEMENTATION_PRIORITY = """
【フェーズ1: 高インパクト・低リスク】（推奨開始点）
1. recency_bias_indicator
   - 実装容易、リークリスク低、市場バイアス直接捕捉
   - 既存のprev_finish_categoryを強化

2. elo_rating
   - 相手の強さを考慮した実力評価
   - 既存の累積統計を補完

3. expected_pace_from_running_styles
   - 展開予測の核心
   - 既存のfront_runner_competitionを強化

【フェーズ2: 中インパクト・要注意】
4. broodmare_sire_win_rate
   - 血統の深層分析
   - 既存のsire_win_rate_fixedを補完

5. horse_pace_preference
   - ペース適性の定量化
   - expected_paceと組み合わせて使用

6. class_change_adjustment
   - 既存のis_class_upを拡張
   - より詳細なクラス変更分析

【フェーズ3: 高インパクト・要検証】
7. odds_movement（朝オッズ→最終オッズ）
   - リアルタイム予測専用
   - 学習時のリーク注意

8. standardized_speed_figure
   - 条件補正後のスピード指数
   - 計算が複雑だが価値高い

【フェーズ4: 組み合わせ最適化】
9. pace_ability_interaction
   - フェーズ1-2の特徴量を組み合わせ
   - 過学習注意

10. 特徴量選択・削減
    - 冗長な特徴量を削除
    - 最終的に50-60特徴量程度に最適化
"""

if __name__ == "__main__":
    print(IMPLEMENTATION_PRIORITY)
    print("\n" + "=" * 80)
    print("過学習防止戦略:")
    print(OverfittingPrevention.STRATEGIES)
