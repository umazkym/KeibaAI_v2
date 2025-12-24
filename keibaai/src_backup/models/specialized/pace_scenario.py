"""
Pace Scenario Model (v5.1)

展開予測モデル - corner_positions.parquetを活用

【エッジの理由】
- 市場は「過去の着順」を見る
- モデルは「当日の展開予測」を加味
- 展開次第で着順が大きく変わることを市場は見落とす

【入力】
- corner_positions.parquet: 過去のコーナー通過履歴（予測対象より前のみ）

【出力】
- pace_type: 'slow' / 'medium' / 'fast'
- advantage_scores: 各馬の展開有利度（-0.2〜+0.2）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, List, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PaceScenarioModel:
    """
    展開予測モデル
    
    出走馬の脚質構成からペースを予測し、
    脚質×ペースの相性から各馬の有利度を計算する。
    """
    
    # 脚質×ペースの相性マトリクス
    ADVANTAGE_MATRIX = {
        ('senkou', 'slow'): 0.15,    # 先行×スロー = 有利
        ('senkou', 'medium'): 0.05,  # 先行×平均 = やや有利
        ('senkou', 'fast'): -0.10,   # 先行×ハイ = 不利
        ('sashi', 'slow'): -0.05,    # 差し×スロー = やや不利
        ('sashi', 'medium'): 0.05,   # 差し×平均 = やや有利
        ('sashi', 'fast'): 0.10,     # 差し×ハイ = 有利
        ('oikomi', 'slow'): -0.15,   # 追込×スロー = 不利
        ('oikomi', 'medium'): 0.00,  # 追込×平均 = 平均
        ('oikomi', 'fast'): 0.15,    # 追込×ハイ = 有利
    }
    
    def __init__(self, train_cutoff: str = '2023-01-01'):
        """
        Parameters:
        -----------
        train_cutoff : str
            Train期間終了日（この日より前のデータのみ使用）
        """
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.running_style_cache = {}  # 馬ごとの脚質キャッシュ
        
    def load_corner_history(self) -> pd.DataFrame:
        """コーナー通過履歴を読み込み"""
        path = Path('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
        if not path.exists():
            logging.warning(f"corner_positions.parquet not found: {path}")
            return pd.DataFrame()
        
        df = pd.read_parquet(path)
        return df
    
    def load_races(self) -> pd.DataFrame:
        """レースデータを読み込み"""
        path = Path('keibaai/data/parsed/parquet/races/races.parquet')
        if not path.exists():
            logging.warning(f"races.parquet not found: {path}")
            return pd.DataFrame()
        
        df = pd.read_parquet(path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        return df
    
    def classify_running_style(
        self,
        horse_id: str,
        corner_history: pd.DataFrame,
        race_dates: pd.DataFrame,
        current_race_date: pd.Timestamp
    ) -> str:
        """
        馬の脚質を過去データから判定
        
        Parameters:
        -----------
        horse_id : str
            馬ID
        corner_history : pd.DataFrame
            コーナー通過履歴
        race_dates : pd.DataFrame
            race_id -> race_date のマッピング
        current_race_date : pd.Timestamp
            予測対象レースの日付
            
        Returns:
        --------
        str : 'senkou' (逃げ・先行) / 'sashi' (差し) / 'oikomi' (追込)
        """
        # キャッシュチェック
        cache_key = f"{horse_id}_{current_race_date.strftime('%Y%m%d')}"
        if cache_key in self.running_style_cache:
            return self.running_style_cache[cache_key]
        
        # 過去のコーナー通過データを取得
        # 4コーナー（最終コーナー）の位置取りを使用
        past_corners = corner_history[
            (corner_history['horse_number'].notna()) &
            (corner_history['corner'] == 4)
        ].copy()
        
        # race_idからrace_dateを取得してフィルタ
        if isinstance(race_dates, pd.DataFrame) and 'race_id' in race_dates.columns:
            race_date_map = race_dates.set_index('race_id')['race_date'].to_dict()
            past_corners['race_date'] = past_corners['race_id'].map(race_date_map)
            past_corners = past_corners[past_corners['race_date'] < current_race_date]
        
        if len(past_corners) == 0:
            # データなし → 中間脚質と仮定
            style = 'sashi'
        else:
            avg_position = past_corners['position'].mean()
            
            if avg_position <= 3:
                style = 'senkou'  # 逃げ・先行
            elif avg_position <= 6:
                style = 'sashi'   # 差し
            else:
                style = 'oikomi'  # 追込
        
        self.running_style_cache[cache_key] = style
        return style
    
    def predict_pace(self, running_styles: List[str]) -> str:
        """
        出走馬の脚質構成からペースを予測
        
        Parameters:
        -----------
        running_styles : List[str]
            各馬の脚質リスト
            
        Returns:
        --------
        str : 'slow' / 'medium' / 'fast'
        """
        n_senkou = sum(1 for s in running_styles if s == 'senkou')
        n_horses = len(running_styles)
        
        senkou_ratio = n_senkou / n_horses if n_horses > 0 else 0
        
        if senkou_ratio >= 0.35:  # 35%以上が先行馬
            return 'fast'   # ハイペース（先行馬潰れる）
        elif senkou_ratio <= 0.15:  # 15%以下が先行馬
            return 'slow'   # スローペース（逃げ有利）
        else:
            return 'medium'
    
    def calculate_advantage(self, running_style: str, pace_type: str) -> float:
        """
        脚質×ペースの相性から有利度を計算
        """
        return self.ADVANTAGE_MATRIX.get((running_style, pace_type), 0.0)
    
    def predict(
        self,
        race_df: pd.DataFrame,
        current_race_date: pd.Timestamp,
        corner_history: Optional[pd.DataFrame] = None,
        races_df: Optional[pd.DataFrame] = None
    ) -> Tuple[str, np.ndarray]:
        """
        展開予測を実行
        
        Parameters:
        -----------
        race_df : pd.DataFrame
            当日の出馬表（horse_id必須）
        current_race_date : pd.Timestamp
            予測対象レースの日付
        corner_history : pd.DataFrame, optional
            コーナー通過履歴
        races_df : pd.DataFrame, optional
            レースデータ（race_date取得用）
            
        Returns:
        --------
        pace_type : str
            予測ペース
        advantage_scores : np.ndarray
            各馬の展開有利度（-0.2〜+0.2）
        """
        if corner_history is None:
            corner_history = self.load_corner_history()
        if races_df is None:
            races_df = self.load_races()
        
        # 各馬の脚質を判定
        running_styles = []
        for _, horse in race_df.iterrows():
            horse_id = str(horse.get('horse_id', ''))
            if horse_id:
                style = self.classify_running_style(
                    horse_id, corner_history, races_df, current_race_date
                )
            else:
                style = 'sashi'  # デフォルト
            running_styles.append(style)
        
        # ペースを予測
        pace_type = self.predict_pace(running_styles)
        
        # 各馬の有利度を計算
        advantage_scores = np.array([
            self.calculate_advantage(style, pace_type)
            for style in running_styles
        ])
        
        return pace_type, advantage_scores
    
    def get_running_style_distribution(self, running_styles: List[str]) -> Dict[str, float]:
        """脚質分布を返す"""
        n = len(running_styles)
        if n == 0:
            return {'senkou': 0, 'sashi': 0, 'oikomi': 0}
        
        return {
            'senkou': sum(1 for s in running_styles if s == 'senkou') / n,
            'sashi': sum(1 for s in running_styles if s == 'sashi') / n,
            'oikomi': sum(1 for s in running_styles if s == 'oikomi') / n
        }


if __name__ == "__main__":
    # テスト
    print("=" * 60)
    print("PaceScenarioModel テスト")
    print("=" * 60)
    
    model = PaceScenarioModel()
    
    # サンプルデータ（8頭立て）
    test_race_df = pd.DataFrame({
        'horse_id': [f'H{i:03d}' for i in range(1, 9)],
        'horse_number': list(range(1, 9))
    })
    
    # 脚質パターンをテスト
    test_styles = ['senkou', 'senkou', 'senkou', 'sashi', 'sashi', 'sashi', 'oikomi', 'oikomi']
    
    print(f"\n脚質構成: {test_styles}")
    print(f"脚質分布: {model.get_running_style_distribution(test_styles)}")
    
    pace = model.predict_pace(test_styles)
    print(f"予測ペース: {pace}")
    
    advantages = [model.calculate_advantage(s, pace) for s in test_styles]
    print(f"有利度: {advantages}")
