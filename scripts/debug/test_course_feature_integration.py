"""
パーサーとCourseFeatureProviderの統合テスト
"""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from keibaai.src.modules.parsers.results_parser import extract_race_metadata_enhanced
from keibaai.src.utils.course_feature_provider import CourseFeatureProvider
from bs4 import BeautifulSoup

BIN_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\raw\html\race")

def test_parse_and_feature(race_id: str, expected_outer: bool = None):
    """特定のレースをパースして特徴量を確認"""
    bin_path = BIN_DIR / f"{race_id}.bin"
    
    if not bin_path.exists():
        print(f"ファイルが存在しません: {bin_path}")
        return
    
    with open(bin_path, 'rb') as f:
        content = f.read().decode('euc-jp', errors='ignore')
    
    soup = BeautifulSoup(content, 'html.parser')
    metadata = extract_race_metadata_enhanced(soup)
    
    print(f"\n{'='*60}")
    print(f"race_id: {race_id}")
    print(f"venue: {metadata.get('venue')}")
    print(f"surface: {metadata.get('track_surface')}")
    print(f"distance: {metadata.get('distance_m')}m")
    print(f"course_direction: {metadata.get('course_direction')}")
    print(f"is_outer_course: {metadata.get('is_outer_course')}")
    
    # CourseFeatureProviderで特徴量を取得
    provider = CourseFeatureProvider()
    features = provider.get_course_features(
        venue=metadata.get('venue') or '',
        surface=metadata.get('track_surface') or '',
        distance=metadata.get('distance_m') or 0,
        is_outer=metadata.get('is_outer_course')
    )
    
    print(f"\n[特徴量]")
    print(f"course_type: {features.get('course_type')}")
    print(f"final_straight_m: {features.get('final_straight_m')}")
    print(f"corner_count: {features.get('corner_count')}")
    print(f"slope_percent: {features.get('slope_percent')}")
    
    if expected_outer is not None:
        status = "✓" if metadata.get('is_outer_course') == expected_outer else "✗"
        print(f"\n期待値チェック: is_outer={expected_outer} → {status}")

def main():
    print("パーサー + CourseFeatureProvider 統合テスト")
    
    # テストケース
    # 1. 新潟記念(外回り) - 202504030411
    test_parse_and_feature('202504030411', expected_outer=True)
    
    # 2. 京都2000m(内回り) - 202408070905
    test_parse_and_feature('202408070905', expected_outer=None)  # 無印→内
    
    # 3. 東京1600m（区分なし）
    test_parse_and_feature('202405010101', expected_outer=None)
    
    # 4. 中山芝1200m（外のみ）
    test_parse_and_feature('202401010101', expected_outer=None)

if __name__ == '__main__':
    main()
