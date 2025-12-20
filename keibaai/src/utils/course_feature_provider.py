"""
コース特徴量プロバイダ

HTMLからパースされたコース情報（venue, surface, distance, is_outer_course）と
マスターデータを突き合わせ、正確なコース特徴量を付与するユーティリティ。
"""

from pathlib import Path
from typing import Optional, Dict, Any
import yaml
import pandas as pd


class CourseFeatureProvider:
    """
    コースマスターデータを用いて特徴量を付与するクラス。
    
    使用例:
        provider = CourseFeatureProvider()
        df = provider.add_features(races_df)
    """
    
    _instance = None
    _master_data = None
    
    # マスターデータのパス
    MASTER_PATH = Path(__file__).parent.parent.parent / 'configs' / 'course_master.yaml'
    
    def __new__(cls):
        """シングルトンパターン"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._master_data is None:
            self._load_master()
    
    def _load_master(self):
        """マスターデータをロード"""
        with open(self.MASTER_PATH, 'r', encoding='utf-8') as f:
            self._master_data = yaml.safe_load(f)
        
        # venue_metaを分離
        self._venue_meta = self._master_data.pop('venue_meta', {})
    
    def get_course_features(
        self, 
        venue: str, 
        surface: str, 
        distance: int, 
        is_outer: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        指定コースの特徴量を取得する。
        
        Args:
            venue: 競馬場名（例: '新潟', '京都'）
            surface: 馬場（'芝' or 'ダート'）
            distance: 距離（m）
            is_outer: 外回りかどうか（None=不明/デフォルト使用）
        
        Returns:
            コース特徴量の辞書
        """
        # マスターからコースデータを取得
        try:
            course_data = self._master_data[venue][surface][distance]
        except KeyError:
            # マスターにない場合はデフォルト値を返す
            return self._get_default_features(venue)
        
        # inner/outer が両方ある場合（混在コース）
        if 'inner' in course_data and 'outer' in course_data:
            if is_outer is True:
                features = course_data['outer'].copy()
            elif is_outer is False:
                features = course_data['inner'].copy()
            else:
                # 不明な場合はouterをデフォルトとする（上位クラスで多い）
                features = course_data['outer'].copy()
        else:
            # defaultのみの場合
            features = course_data.get('default', {}).copy()
        
        # venue_metaから追加情報を付与
        venue_info = self._venue_meta.get(venue, {})
        features['turn_direction'] = venue_info.get('turn_direction', 'right')
        
        return features
    
    def _get_default_features(self, venue: str) -> Dict[str, Any]:
        """デフォルト特徴量を返す"""
        venue_info = self._venue_meta.get(venue, {})
        return {
            'course_type': 'none',
            'corner_count': 4,
            'start_to_corner_m': 300,
            'final_straight_m': 300,
            'slope_percent': 0.0,
            'turn_direction': venue_info.get('turn_direction', 'right'),
        }
    
    def add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        DataFrameにコース特徴量を追加する。
        
        必要なカラム:
            - venue: 競馬場名
            - track_surface: 馬場（芝/ダート）
            - distance_m: 距離
            - is_outer_course: 外回りフラグ（オプション、なければNone扱い）
        
        追加されるカラム:
            - course_type: inner/outer/none/straight
            - course_corner_count: コーナー数
            - course_start_to_corner_m: スタートから最初のコーナーまでの距離
            - course_final_straight_m: 最終直線距離
            - course_slope_percent: 坂の勾配率
            - course_turn_direction: 回り方向
            - course_is_outer: 外回りかどうか（bool）
        """
        result = df.copy()
        
        # カラム名の正規化
        surface_col = 'track_surface' if 'track_surface' in df.columns else 'surface'
        distance_col = 'distance_m' if 'distance_m' in df.columns else 'distance'
        outer_col = 'is_outer_course' if 'is_outer_course' in df.columns else None
        
        # 特徴量を格納するリスト
        features_list = []
        
        for idx, row in df.iterrows():
            venue = row.get('venue', '')
            surface = row.get(surface_col, '')
            distance = int(row.get(distance_col, 0))
            is_outer = row.get(outer_col) if outer_col else None
            
            # 特徴量を取得
            features = self.get_course_features(venue, surface, distance, is_outer)
            features_list.append(features)
        
        # DataFrameに変換して結合
        features_df = pd.DataFrame(features_list, index=df.index)
        
        # カラム名のリネーム
        rename_map = {
            'course_type': 'course_type',
            'corner_count': 'course_corner_count',
            'start_to_corner_m': 'course_start_to_corner_m',
            'final_straight_m': 'course_final_straight_m',
            'slope_percent': 'course_slope_percent',
            'turn_direction': 'course_turn_direction',
        }
        features_df = features_df.rename(columns=rename_map)
        
        # course_is_outer を追加
        features_df['course_is_outer'] = features_df['course_type'] == 'outer'
        
        # 元のDataFrameに結合
        for col in features_df.columns:
            result[col] = features_df[col]
        
        return result


# シングルトンインスタンスを取得する関数
def get_course_feature_provider() -> CourseFeatureProvider:
    """CourseFeatureProviderのインスタンスを取得"""
    return CourseFeatureProvider()


if __name__ == '__main__':
    # テスト
    provider = CourseFeatureProvider()
    
    # 新潟2000m外
    print("新潟2000m外:", provider.get_course_features('新潟', '芝', 2000, is_outer=True))
    
    # 新潟2000m内
    print("新潟2000m内:", provider.get_course_features('新潟', '芝', 2000, is_outer=False))
    
    # 京都1600m外
    print("京都1600m外:", provider.get_course_features('京都', '芝', 1600, is_outer=True))
    
    # 東京1600m（内外区分なし）
    print("東京1600m:", provider.get_course_features('東京', '芝', 1600))
    
    # 中山2000m（内のみ）
    print("中山2000m:", provider.get_course_features('中山', '芝', 2000))
