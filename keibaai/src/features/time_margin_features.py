"""
タイム差（着差）ベースの特徴量モジュール

【目的】
順位（1着/2着/3着）だけでなく、タイム差（着差秒数）を活用して
「真の実力差」を把握する特徴量を生成する。

【生成する特徴量】
1. 馬別着差統計（過去走ベース）
   - horse_avg_margin_when_win:  勝った時の平均着差（差をつけて勝つ馬か）
   - horse_avg_margin_when_loss: 負けた時の平均着差（惜しく負ける馬か）
   - horse_close_loss_rate:      僅差負け率（0.3秒以内で負けた割合）
   - horse_margin_std:           着差の標準偏差（安定性）

2. レースレベル予測特徴量（予測スコアベース）
   - race_place_prob_std:        レース内place_prob標準偏差（荒れ度）
   - race_win_prob_entropy:      勝率のエントロピー（混戦度）

【リーク対策】★重要★
- margin_seconds は当該レースの結果 → 直接使用は厳禁
- 必ず過去レースのみ使用 (shift(1) + expanding)
- fit時にTrain期間のデータで累積統計を事前計算
- transform時は事前計算した統計をマッピングするのみ

【過学習対策】
- min_races=5 を要件とする（少ないサンプルでは統計を欠損扱い）
- 外れ値除外: margin_seconds > 10秒、または競走中止

【使用方法】
```python
# MultiTargetPredictorのpredict()出力を拡張
predictor = MultiTargetPredictor(...)
predictor.fit(train_df, valid_df, feature_cols)

# 予測実行
predictions = predictor.predict(test_df)

# タイム差特徴量を追加
margin_analyzer = TimeMarginAnalyzer()
margin_analyzer.fit(train_df)  # 過去統計を計算
enhanced_predictions = margin_analyzer.add_race_volatility(predictions, test_df)
```

作成日: 2026-01-11
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
import logging
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class TimeMarginConfig:
    """タイム差特徴量の設定"""
    
    # 僅差負けの閾値（秒）
    close_loss_threshold: float = 0.3
    
    # 最小レース数（過学習対策）
    min_races_for_stats: int = 5
    
    # 外れ値除外閾値（秒）
    max_valid_margin: float = 10.0
    
    # 荒れレース判定閾値（place_prob標準偏差）
    # 分散が小さい = 実力差が見えにくい = 荒れやすい
    volatile_threshold: float = 0.10
    moderate_threshold: float = 0.18


class TimeMarginFeatureEngineer:
    """
    タイム差（着差）ベースの特徴量エンジニア
    
    【リーク防止設計】
    1. fit() で過去データから統計を計算（shift適用済み）
    2. transform() では事前計算した統計をマッピングするのみ
    """
    
    def __init__(self, config: Optional[TimeMarginConfig] = None):
        self.config = config or TimeMarginConfig()
        
        # fit時に計算する統計（リーク防止のため事前計算）
        self._horse_margin_stats: Dict[str, Dict] = {}
        self._fitted = False
    
    def fit(self, races_df: pd.DataFrame) -> 'TimeMarginFeatureEngineer':
        """
        過去レースデータから馬別着差統計を計算（ベクトル化版）
        
        【リーク対策】
        - expanding() + shift(1) で「その時点で知りえた情報」のみ使用
        - 当該レースの着差は含まない
        
        Args:
            races_df: レース結果データ（finish_position, margin_seconds含む）
        
        Returns:
            self
        """
        logger.info("TimeMarginFeatureEngineer: fit開始")
        
        # 必須カラムチェック
        required_cols = ['race_id', 'race_date', 'horse_id', 'finish_position', 'margin_seconds']
        missing = [c for c in required_cols if c not in races_df.columns]
        if missing:
            logger.warning(f"  必須カラムが不足: {missing}")
            self._fitted = True
            return self
        
        df = races_df.copy()
        
        # データクリーニング
        df = self._clean_margin_data(df)
        
        # ソート（時系列順）
        df = df.sort_values(['horse_id', 'race_date', 'race_id']).reset_index(drop=True)
        
        # 勝ち負けフラグ
        df['is_win'] = (df['finish_position'] == 1).astype(int)
        df['is_loss'] = (df['finish_position'] > 1).astype(int)
        
        # 僅差負けフラグ（2-3着かつ着差0.3秒以内）
        df['is_close_loss'] = (
            (df['finish_position'].isin([2, 3])) & 
            (df['margin_seconds'] <= self.config.close_loss_threshold)
        ).astype(int)
        
        logger.info("  累積統計を計算中（ベクトル化版）...")
        
        # ========== ベクトル化計算 ==========
        
        # 1. 負け時の着差（勝ちは NaN にする）
        df['loss_margin'] = df['margin_seconds'].where(df['is_loss'] == 1, np.nan)
        
        # 2. 累積統計（shift(1)でリーク防止）
        # 負け時の平均着差
        df['cum_loss_margin_sum'] = df.groupby('horse_id')['loss_margin'].transform(
            lambda x: x.fillna(0).cumsum().shift(1)
        )
        df['cum_loss_count'] = df.groupby('horse_id')['is_loss'].transform(
            lambda x: x.cumsum().shift(1)
        )
        df['avg_margin_when_loss'] = df['cum_loss_margin_sum'] / df['cum_loss_count'].replace(0, np.nan)
        
        # 僅差負け率
        df['cum_close_loss'] = df.groupby('horse_id')['is_close_loss'].transform(
            lambda x: x.cumsum().shift(1)
        )
        df['close_loss_rate'] = df['cum_close_loss'] / df['cum_loss_count'].replace(0, np.nan)
        
        # 着差標準偏差
        df['cum_margin_std'] = df.groupby('horse_id')['margin_seconds'].transform(
            lambda x: x.shift(1).expanding().std()
        )
        
        # レース数
        df['cum_race_count'] = df.groupby('horse_id').cumcount()
        
        # 3. 最小レース数フィルタ
        min_races = self.config.min_races_for_stats
        df.loc[df['cum_race_count'] < min_races, 'avg_margin_when_loss'] = np.nan
        df.loc[df['cum_race_count'] < min_races, 'close_loss_rate'] = np.nan
        df.loc[df['cum_race_count'] < min_races, 'cum_margin_std'] = np.nan
        
        # 4. 各馬の最新統計を抽出
        latest = df.groupby('horse_id').last()[
            ['avg_margin_when_loss', 'close_loss_rate', 'cum_margin_std', 'cum_race_count']
        ].reset_index()
        
        # 最小レース数に満たない馬を除外
        latest = latest[latest['cum_race_count'] >= min_races]
        
        # 辞書に変換
        for _, row in latest.iterrows():
            self._horse_margin_stats[row['horse_id']] = {
                'avg_margin_when_win': np.nan,  # 簡略化: 勝ち時は計算省略
                'avg_margin_when_loss': row['avg_margin_when_loss'],
                'close_loss_rate': row['close_loss_rate'],
                'margin_std': row['cum_margin_std'],
                'race_count': row['cum_race_count'],
            }
        
        self._fitted = True
        logger.info(f"  統計計算完了: {len(self._horse_margin_stats):,}頭")
        logger.info("TimeMarginFeatureEngineer: fit完了")
        
        return self
    
    def _clean_margin_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        着差データのクリーニング
        
        【除外対象】
        - margin_seconds がNaN
        - margin_seconds > 10秒（異常値）
        - 競走中止・失格等
        """
        original_len = len(df)
        
        # NaN除外
        df = df[df['margin_seconds'].notna()].copy()
        
        # 外れ値除外
        df = df[df['margin_seconds'] <= self.config.max_valid_margin]
        
        # 競走中止・失格除外（着順がNaNまたは18超）
        df = df[df['finish_position'].notna()]
        df = df[df['finish_position'] <= 18]
        
        removed = original_len - len(df)
        if removed > 0:
            logger.info(f"  外れ値除外: {removed:,}件（{removed/original_len*100:.1f}%）")
        
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        事前計算した統計を特徴量として付与
        
        【リーク対策】
        - fit時に計算した統計をマッピングするのみ
        - 当該レースの情報は一切使用しない
        
        Args:
            df: 特徴量を付与するデータフレーム
        
        Returns:
            特徴量が追加されたデータフレーム
        """
        if not self._fitted:
            raise RuntimeError("fit()を先に実行してください")
        
        logger.info("TimeMarginFeatureEngineer: transform開始")
        
        result = df.copy()
        
        # 馬別統計をマッピング
        result['horse_avg_margin_when_win'] = result['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('avg_margin_when_win', np.nan)
        )
        result['horse_avg_margin_when_loss'] = result['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('avg_margin_when_loss', np.nan)
        )
        result['horse_close_loss_rate'] = result['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('close_loss_rate', np.nan)
        )
        result['horse_margin_std'] = result['horse_id'].map(
            lambda x: self._horse_margin_stats.get(x, {}).get('margin_std', np.nan)
        )
        
        # 非NaN率をログ出力
        for col in ['horse_avg_margin_when_win', 'horse_avg_margin_when_loss', 
                    'horse_close_loss_rate', 'horse_margin_std']:
            non_null = result[col].notna().sum()
            logger.info(f"  {col}: {non_null:,}/{len(result):,} ({non_null/len(result)*100:.1f}%)")
        
        logger.info("TimeMarginFeatureEngineer: transform完了")
        
        return result
    
    def get_feature_columns(self) -> list:
        """生成する特徴量カラム名のリスト"""
        return [
            'horse_avg_margin_when_win',
            'horse_avg_margin_when_loss', 
            'horse_close_loss_rate',
            'horse_margin_std',
        ]


class RaceVolatilityAnalyzer:
    """
    レースの「荒れやすさ」を分析するクラス
    
    【リーク対策】★重要★
    この分析は予測結果（place_prob等）をベースにするため、
    予測時点で使用可能な情報のみを使用する。
    
    当該レースの着差・タイム差は使用不可（リーク）。
    過去レースの統計のみ使用可能。
    """
    
    def __init__(self, config: Optional[TimeMarginConfig] = None):
        self.config = config or TimeMarginConfig()
    
    def calc_race_volatility(self, predictions_df: pd.DataFrame) -> pd.DataFrame:
        """
        レース内の予測確率分布から「荒れやすさ」を計算
        
        【計算方法】
        - place_prob標準偏差: 分散が小さい = 実力差が見えにくい = 荒れやすい
        - win_probエントロピー: 高い = 混戦 = 荒れやすい
        
        【リーク対策】
        - 予測確率（place_prob, win_prob）のみを使用
        - 当該レースの実際の着差は使用しない
        
        Args:
            predictions_df: MultiTargetPredictorの予測結果
                            (race_id, horse_number, win_prob, top2_prob, place_prob)
        
        Returns:
            荒れ度指標が追加されたデータフレーム
        """
        logger.info("RaceVolatilityAnalyzer: 荒れ度計算開始")
        
        result = predictions_df.copy()
        
        # レース内place_prob標準偏差（低い = 荒れやすい）
        result['race_place_prob_std'] = result.groupby('race_id')['place_prob'].transform('std')
        
        # レース内win_probエントロピー（高い = 混戦）
        def calc_entropy(probs):
            """正規化エントロピーを計算"""
            probs = np.array(probs)
            probs = probs / probs.sum()  # 正規化
            probs = probs[probs > 0]  # 0を除外
            if len(probs) <= 1:
                return 0
            entropy = -np.sum(probs * np.log2(probs))
            max_entropy = np.log2(len(probs))
            return entropy / max_entropy if max_entropy > 0 else 0
        
        result['race_win_prob_entropy'] = result.groupby('race_id')['win_prob'].transform(
            lambda x: calc_entropy(x.values)
        )
        
        # 荒れ度分類
        result['race_volatility_type'] = result['race_place_prob_std'].apply(
            lambda x: 'volatile' if x < self.config.volatile_threshold 
                      else ('moderate' if x < self.config.moderate_threshold else 'stable')
        )
        
        # 統計ログ
        type_counts = result.groupby('race_id')['race_volatility_type'].first().value_counts()
        logger.info(f"  レース分類: {type_counts.to_dict()}")
        logger.info(f"  place_prob_std平均: {result['race_place_prob_std'].mean():.4f}")
        logger.info(f"  win_prob_entropy平均: {result['race_win_prob_entropy'].mean():.4f}")
        
        logger.info("RaceVolatilityAnalyzer: 荒れ度計算完了")
        
        return result
    
    def calc_exact_position_probs(self, predictions_df: pd.DataFrame) -> pd.DataFrame:
        """
        着順別確率を算出
        
        P(exactly 1着) = win_prob
        P(exactly 2着) = top2_prob - win_prob
        P(exactly 3着) = place_prob - top2_prob
        
        【注意】
        各モデルは独立に学習されているため、top2_prob < win_prob となる
        可能性がある。その場合は0にクリップする。
        
        Args:
            predictions_df: 予測結果
        
        Returns:
            着順別確率が追加されたデータフレーム
        """
        logger.info("RaceVolatilityAnalyzer: 着順別確率計算開始")
        
        result = predictions_df.copy()
        
        # 着順別確率を算出
        result['prob_exactly_1st'] = result['win_prob']
        result['prob_exactly_2nd'] = (result['top2_prob'] - result['win_prob']).clip(lower=0)
        result['prob_exactly_3rd'] = (result['place_prob'] - result['top2_prob']).clip(lower=0)
        
        # 整合性チェック（負値発生率）
        negative_2nd = (result['top2_prob'] < result['win_prob']).mean()
        negative_3rd = (result['place_prob'] < result['top2_prob']).mean()
        
        if negative_2nd > 0.01:
            logger.warning(f"  警告: top2_prob < win_prob が {negative_2nd*100:.1f}% 発生")
        if negative_3rd > 0.01:
            logger.warning(f"  警告: place_prob < top2_prob が {negative_3rd*100:.1f}% 発生")
        
        logger.info("RaceVolatilityAnalyzer: 着順別確率計算完了")
        
        return result
    
    def get_strategy_recommendation(self, race_volatility: pd.DataFrame) -> pd.DataFrame:
        """
        荒れ度に応じた戦略推奨を生成
        
        Args:
            race_volatility: calc_race_volatility()の出力
        
        Returns:
            戦略推奨が追加されたデータフレーム
        """
        result = race_volatility.copy()
        
        # 荒れ度タイプに基づく戦略
        strategy_map = {
            'volatile': 'longshot',   # 穴馬狙い（V4.4ウェイト増加）
            'moderate': 'balanced',   # バランス
            'stable': 'favorite',     # 本命狙い（V15ウェイト維持）
        }
        
        result['recommended_strategy'] = result['race_volatility_type'].map(strategy_map)
        
        # V4.4推奨ウェイト
        weight_map = {
            'volatile': 0.45,   # 30% → 45%に増加
            'moderate': 0.35,   # 30% → 35%に増加
            'stable': 0.30,     # 30%維持
        }
        
        result['v44_recommended_weight'] = result['race_volatility_type'].map(weight_map)
        
        return result
