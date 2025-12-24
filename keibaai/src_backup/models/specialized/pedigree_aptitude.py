"""
Pedigree Aptitude Model (v5.1)

血統×条件の交互作用から隠れた適性を発見するモデル。

【エッジの理由】
- 市場は「この馬の過去成績」を見る
- モデルは「この血統の条件別傾向」を見る
- 初めての条件でも血統から予測できる

【入力】
- pedigrees.parquet: 血統データ（5世代）
- races.parquet: 過去レース結果（Train期間のみ）

【出力】
- aptitude_score: 血統適性スコア（-0.15〜+0.15）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PedigreeAptitudeModel:
    """
    血統適性モデル
    
    Train期間のみで種牡馬別・条件別の成績を集計し、
    当日の条件に対する適性スコアを予測。
    """
    
    # 距離カテゴリ定義
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 2800),
        'marathon': (2800, 5000)
    }
    
    def __init__(self, train_cutoff: str = '2023-01-01', min_samples: int = 30):
        """
        Parameters:
        -----------
        train_cutoff : str
            Train期間終了日（この日より前のデータのみ使用）
        min_samples : int
            統計計算に必要な最小サンプル数
        """
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.min_samples = min_samples
        self.sire_stats = {}  # 種牡馬別統計
        self.bms_stats = {}   # 母父別統計
        self.base_win_rate = 0.08  # 全体平均勝率（約8%）
        self.is_fitted = False
        self.sire_map = None
        self.bms_map = None
        
    def fit(self, races_df: pd.DataFrame, pedigrees_df: pd.DataFrame):
        """
        Train期間のデータで血統別の条件別成績を集計
        
        ⚠️ Train期間のみで統計を計算（リーク防止）
        """
        logging.info("PedigreeAptitudeModel: fit開始")
        
        # Train期間のみフィルタ
        races_df = races_df.copy()
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        train_races = races_df[races_df['race_date'] < self.train_cutoff].copy()
        
        logging.info(f"  Train期間レース数: {len(train_races):,}")
        
        # 血統データの準備
        pedigrees_df = pedigrees_df.copy()
        pedigrees_df['horse_id'] = pedigrees_df['horse_id'].astype(str)
        
        # 父（generation=1の最初の祖先）
        sire_df = pedigrees_df[pedigrees_df['generation'] == 1].copy()
        sire_df = sire_df.drop_duplicates(subset=['horse_id'], keep='first')
        self.sire_map = sire_df.set_index('horse_id')['ancestor_id'].to_dict()
        
        # 母父は別途計算が必要（generation=2の3番目）
        # 簡易版ではgeneration=2の最初の祖先を使用
        bms_df = pedigrees_df[pedigrees_df['generation'] == 2].copy()
        bms_df = bms_df.drop_duplicates(subset=['horse_id'], keep='first')
        self.bms_map = bms_df.set_index('horse_id')['ancestor_id'].to_dict()
        
        # 馬IDをstring型に
        train_races['horse_id'] = train_races['horse_id'].astype(str)
        
        # 父IDを追加
        train_races['sire_id'] = train_races['horse_id'].map(self.sire_map)
        train_races['bms_id'] = train_races['horse_id'].map(self.bms_map)
        
        # 共通処理
        train_races['finish_position'] = pd.to_numeric(train_races['finish_position'], errors='coerce')
        train_races['is_win'] = (train_races['finish_position'] == 1).fillna(False).astype(int)
        train_races['is_place'] = (train_races['finish_position'] <= 3).fillna(False).astype(int)
        
        # 距離カテゴリを追加
        train_races['distance_m'] = pd.to_numeric(train_races['distance_m'], errors='coerce')
        train_races['distance_category'] = pd.cut(
            train_races['distance_m'],
            bins=[0, 1400, 1800, 2200, 2800, 5000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'marathon']
        )
        
        # 馬場状態の正規化
        if 'track_condition' in train_races.columns:
            train_races['is_heavy'] = train_races['track_condition'].isin(['重', '不良'])
        else:
            train_races['is_heavy'] = False
        
        # 全体平均勝率
        self.base_win_rate = train_races['is_win'].mean()
        logging.info(f"  全体平均勝率: {self.base_win_rate:.2%}")
        
        # 種牡馬別統計を集計
        self._fit_sire_stats(train_races)
        
        # 母父別統計を集計
        self._fit_bms_stats(train_races)
        
        self.is_fitted = True
        logging.info(f"  種牡馬統計: {len(self.sire_stats)}種")
        logging.info(f"  母父統計: {len(self.bms_stats)}種")
    
    def _fit_sire_stats(self, df: pd.DataFrame):
        """種牡馬別の条件別成績を集計"""
        for sire_id in df['sire_id'].dropna().unique():
            sire_races = df[df['sire_id'] == sire_id]
            
            if len(sire_races) < self.min_samples:
                continue
            
            stats = {
                'total_races': len(sire_races),
                'overall_win_rate': sire_races['is_win'].mean(),
                'overall_place_rate': sire_races['is_place'].mean(),
            }
            
            # 馬場別
            for surface in ['芝', 'ダート']:
                surface_races = sire_races[sire_races['track_surface'] == surface]
                if len(surface_races) >= 10:
                    stats[f'{surface}_win_rate'] = surface_races['is_win'].mean()
                    stats[f'{surface}_place_rate'] = surface_races['is_place'].mean()
            
            # 距離別
            for dist_cat in ['sprint', 'mile', 'intermediate', 'long', 'marathon']:
                dist_races = sire_races[sire_races['distance_category'] == dist_cat]
                if len(dist_races) >= 10:
                    stats[f'{dist_cat}_win_rate'] = dist_races['is_win'].mean()
            
            # 馬場状態別
            heavy_races = sire_races[sire_races['is_heavy'] == True]
            if len(heavy_races) >= 10:
                stats['heavy_win_rate'] = heavy_races['is_win'].mean()
            
            self.sire_stats[sire_id] = stats
    
    def _fit_bms_stats(self, df: pd.DataFrame):
        """母父別の条件別成績を集計"""
        for bms_id in df['bms_id'].dropna().unique():
            bms_races = df[df['bms_id'] == bms_id]
            
            if len(bms_races) < self.min_samples:
                continue
            
            stats = {
                'total_races': len(bms_races),
                'overall_win_rate': bms_races['is_win'].mean(),
            }
            
            # 馬場別
            for surface in ['芝', 'ダート']:
                surface_races = bms_races[bms_races['track_surface'] == surface]
                if len(surface_races) >= 10:
                    stats[f'{surface}_win_rate'] = surface_races['is_win'].mean()
            
            # 距離別
            for dist_cat in ['sprint', 'mile', 'intermediate', 'long']:
                dist_races = bms_races[bms_races['distance_category'] == dist_cat]
                if len(dist_races) >= 10:
                    stats[f'{dist_cat}_win_rate'] = dist_races['is_win'].mean()
            
            self.bms_stats[bms_id] = stats
    
    def predict(self, horse_id: str, race_conditions: Dict) -> float:
        """
        血統から条件適性スコアを予測
        
        Parameters:
        -----------
        horse_id : str
            馬ID
        race_conditions : Dict
            当日のレース条件
            - surface: '芝' or 'ダート'
            - distance_category: 'sprint', 'mile', 'intermediate', 'long', 'marathon'
            - is_heavy: True/False（重・不良馬場かどうか）
            
        Returns:
        --------
        float : 適性スコア（-0.15〜+0.15）
        """
        if not self.is_fitted:
            raise RuntimeError("モデルがfitされていません")
        
        horse_id = str(horse_id)
        score = 0.0
        
        # 種牡馬からの適性
        sire_id = self.sire_map.get(horse_id)
        if sire_id and sire_id in self.sire_stats:
            sire_score = self._calculate_sire_score(sire_id, race_conditions)
            score += sire_score
        
        # 母父からの適性（重みを半分に）
        bms_id = self.bms_map.get(horse_id)
        if bms_id and bms_id in self.bms_stats:
            bms_score = self._calculate_bms_score(bms_id, race_conditions)
            score += bms_score * 0.5
        
        # クリップ
        return np.clip(score, -0.15, 0.15)
    
    def _calculate_sire_score(self, sire_id: str, conditions: Dict) -> float:
        """種牡馬の条件適性スコアを計算"""
        stats = self.sire_stats.get(sire_id, {})
        score = 0.0
        
        # 馬場別
        surface = conditions.get('surface')
        if surface and f'{surface}_win_rate' in stats:
            diff = stats[f'{surface}_win_rate'] - self.base_win_rate
            score += np.clip(diff, -0.05, 0.05)
        
        # 距離別
        dist_cat = conditions.get('distance_category')
        if dist_cat and f'{dist_cat}_win_rate' in stats:
            diff = stats[f'{dist_cat}_win_rate'] - self.base_win_rate
            score += np.clip(diff, -0.05, 0.05)
        
        # 馬場状態
        if conditions.get('is_heavy') and 'heavy_win_rate' in stats:
            diff = stats['heavy_win_rate'] - self.base_win_rate
            score += np.clip(diff, -0.03, 0.03)
        
        return score
    
    def _calculate_bms_score(self, bms_id: str, conditions: Dict) -> float:
        """母父の条件適性スコアを計算"""
        stats = self.bms_stats.get(bms_id, {})
        score = 0.0
        
        surface = conditions.get('surface')
        if surface and f'{surface}_win_rate' in stats:
            diff = stats[f'{surface}_win_rate'] - self.base_win_rate
            score += np.clip(diff, -0.03, 0.03)
        
        return score
    
    def predict_race(self, race_df: pd.DataFrame, race_conditions: Dict) -> np.ndarray:
        """
        レース全体の血統適性スコアを予測
        
        Returns:
        --------
        np.ndarray : 各馬の適性スコア
        """
        scores = []
        for _, horse in race_df.iterrows():
            horse_id = str(horse.get('horse_id', ''))
            score = self.predict(horse_id, race_conditions)
            scores.append(score)
        
        return np.array(scores)


if __name__ == "__main__":
    print("=" * 60)
    print("PedigreeAptitudeModel テスト")
    print("=" * 60)
    
    # データ読み込みテスト
    model = PedigreeAptitudeModel(train_cutoff='2023-01-01')
    
    # ファイル存在確認
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    
    if races_path.exists() and pedigrees_path.exists():
        races_df = pd.read_parquet(races_path)
        pedigrees_df = pd.read_parquet(pedigrees_path)
        
        print(f"レースデータ: {len(races_df):,}件")
        print(f"血統データ: {len(pedigrees_df):,}件")
        
        # フィット
        model.fit(races_df, pedigrees_df)
        
        # テスト予測
        test_conditions = {
            'surface': '芝',
            'distance_category': 'long',
            'is_heavy': False
        }
        
        # サンプル馬ID（実際にデータに存在するものを使用）
        sample_horse_ids = races_df['horse_id'].dropna().unique()[:5]
        
        print(f"\nテスト条件: {test_conditions}")
        print("\n馬ID | 適性スコア")
        print("-" * 30)
        for hid in sample_horse_ids:
            score = model.predict(str(hid), test_conditions)
            print(f"{hid} | {score:+.3f}")
    else:
        print("データファイルが見つかりません")
        print(f"  races: {races_path.exists()}")
        print(f"  pedigrees: {pedigrees_path.exists()}")
