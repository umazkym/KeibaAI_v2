"""
Condition Change Model (v5.1)

馬の状態変化を評価するモデル。

【エッジの理由】
- 市場は「直近の着順」に引きずられる
- モデルは「休養明け」「体重変動」「クラス変動」を分析
- 状態の上がり/下がりを市場より早く検知

【入力】
- races.parquet: 過去レース結果
- 当日の体重・クラス・間隔情報

【出力】
- condition_score: 状態変化スコア（-0.1〜+0.1）
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ConditionChangeModel:
    """
    状態変化モデル
    
    休養明け、体重変動、クラス変動から馬の状態を評価。
    """
    
    # クラスのランク定義
    CLASS_RANK = {
        'G1': 10, 'G2': 9, 'G3': 8,
        'オープン': 7, 'OP': 7,
        '3勝クラス': 6, '1600万下': 6,
        '2勝クラス': 5, '1000万下': 5,
        '1勝クラス': 4, '500万下': 4,
        '未勝利': 3,
        '新馬': 2,
    }
    
    # 休養効果の設定
    REST_EFFECT = {
        (0, 30): 0.0,      # 通常間隔
        (30, 60): 0.02,    # やや休養明け（フレッシュ効果）
        (60, 120): 0.0,    # 中程度休養
        (120, 180): -0.02, # 長期休養（調整懸念）
        (180, 365): -0.05, # 超長期休養
        (365, 9999): -0.08 # 1年以上
    }
    
    def __init__(self, train_cutoff: str = '2023-01-01'):
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.horse_history = {}  # 馬ごとの履歴
        
    def fit(self, races_df: pd.DataFrame):
        """馬ごとの履歴を構築"""
        logging.info("ConditionChangeModel: fit開始")
        
        races_df = races_df.copy()
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # Train期間のみ
        train_races = races_df[races_df['race_date'] < self.train_cutoff].copy()
        
        # 馬ごとに履歴を構築
        for horse_id, group in train_races.groupby('horse_id'):
            history = group.sort_values('race_date')[
                ['race_date', 'finish_position', 'horse_weight', 'race_class']
            ].to_dict('records')
            self.horse_history[horse_id] = history
        
        logging.info(f"  馬履歴: {len(self.horse_history):,}頭")
    
    def calculate_rest_days(self, horse_id: str, current_race_date: pd.Timestamp) -> int:
        """前走からの日数を計算"""
        if horse_id not in self.horse_history:
            return 0
        
        history = self.horse_history[horse_id]
        past_dates = [h['race_date'] for h in history if h['race_date'] < current_race_date]
        
        if not past_dates:
            return 365  # 履歴なし = 長期休養扱い
        
        last_race = max(past_dates)
        return (current_race_date - last_race).days
    
    def calculate_weight_change(self, horse_id: str, current_weight: float,
                                current_race_date: pd.Timestamp) -> float:
        """体重変動を計算"""
        if horse_id not in self.horse_history or current_weight <= 0:
            return 0.0
        
        history = self.horse_history[horse_id]
        past_weights = [
            h['horse_weight'] for h in history 
            if h['race_date'] < current_race_date and h.get('horse_weight', 0) > 0
        ]
        
        if not past_weights:
            return 0.0
        
        avg_weight = np.mean(past_weights)
        change_rate = (current_weight - avg_weight) / avg_weight
        
        return change_rate
    
    def calculate_class_change(self, horse_id: str, current_class: str,
                              current_race_date: pd.Timestamp) -> int:
        """クラス変動を計算（上がり=正、下がり=負）"""
        if horse_id not in self.horse_history:
            return 0
        
        history = self.horse_history[horse_id]
        past_classes = [
            h['race_class'] for h in history 
            if h['race_date'] < current_race_date and h.get('race_class')
        ]
        
        if not past_classes:
            return 0
        
        last_class = past_classes[-1]
        current_rank = self.CLASS_RANK.get(current_class, 5)
        last_rank = self.CLASS_RANK.get(last_class, 5)
        
        return current_rank - last_rank
    
    def get_rest_effect(self, rest_days: int) -> float:
        """休養日数から効果を取得"""
        for (min_days, max_days), effect in self.REST_EFFECT.items():
            if min_days <= rest_days < max_days:
                return effect
        return 0.0
    
    def predict(self, horse_id: str, current_race_date: pd.Timestamp,
                current_weight: float = 0, current_class: str = '') -> float:
        """
        状態変化スコアを予測
        
        Returns:
        --------
        float : 状態変化スコア（-0.1〜+0.1）
        """
        horse_id = str(horse_id)
        score = 0.0
        
        # 1. 休養効果
        rest_days = self.calculate_rest_days(horse_id, current_race_date)
        rest_score = self.get_rest_effect(rest_days)
        score += rest_score
        
        # 2. 体重変動
        if current_weight > 0:
            weight_change = self.calculate_weight_change(horse_id, current_weight, current_race_date)
            # 大幅な増減はマイナス
            if abs(weight_change) > 0.03:  # 3%以上の変動
                weight_score = -0.02
            elif abs(weight_change) > 0.05:  # 5%以上
                weight_score = -0.05
            else:
                weight_score = 0.0
            score += weight_score
        
        # 3. クラス変動
        if current_class:
            class_change = self.calculate_class_change(horse_id, current_class, current_race_date)
            if class_change > 0:
                # 昇級 → やや不利
                score -= 0.02 * min(class_change, 2)
            elif class_change < 0:
                # 降級 → やや有利
                score += 0.02 * min(abs(class_change), 2)
        
        return np.clip(score, -0.1, 0.1)
    
    def predict_race(self, race_df: pd.DataFrame, current_race_date: pd.Timestamp,
                    current_class: str = '') -> np.ndarray:
        """レース全体の状態変化スコアを予測"""
        scores = []
        for _, horse in race_df.iterrows():
            horse_id = str(horse.get('horse_id', ''))
            weight = horse.get('horse_weight', 0)
            if pd.isna(weight):
                weight = 0
            
            score = self.predict(horse_id, current_race_date, 
                               float(weight), current_class)
            scores.append(score)
        
        return np.array(scores)


if __name__ == "__main__":
    from pathlib import Path
    
    print("=" * 60)
    print("ConditionChangeModel テスト")
    print("=" * 60)
    
    model = ConditionChangeModel()
    
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    
    if races_path.exists():
        races_df = pd.read_parquet(races_path)
        print(f"レースデータ: {len(races_df):,}件")
        
        model.fit(races_df)
        
        # テスト予測
        test_date = pd.Timestamp('2024-06-01')
        sample_horse_ids = list(model.horse_history.keys())[:5]
        
        print(f"\nテスト日付: {test_date.strftime('%Y-%m-%d')}")
        print("\n馬ID | 休養日数 | スコア")
        print("-" * 40)
        for hid in sample_horse_ids:
            rest_days = model.calculate_rest_days(hid, test_date)
            score = model.predict(hid, test_date, current_class='2勝クラス')
            print(f"{hid} | {rest_days:3d}日 | {score:+.3f}")
    else:
        print(f"データファイルが見つかりません: {races_path}")
