"""新潟2000mの内外確認"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai")))
from src.utils.course_feature_provider import CourseFeatureProvider

provider = CourseFeatureProvider()

inner = provider.get_course_features("新潟", "芝", 2000, is_outer=False)
outer = provider.get_course_features("新潟", "芝", 2000, is_outer=True)
unknown = provider.get_course_features("新潟", "芝", 2000, is_outer=None)

print("新潟2000m マスターデータ確認:")
print(f"  内回り(is_outer=False): 直線{inner['final_straight_m']}m, コーナー{inner['corner_count']}回")
print(f"  外回り(is_outer=True):  直線{outer['final_straight_m']}m, コーナー{outer['corner_count']}回")
print(f"  不明(is_outer=None):    直線{unknown['final_straight_m']}m, コーナー{unknown['corner_count']}回")
