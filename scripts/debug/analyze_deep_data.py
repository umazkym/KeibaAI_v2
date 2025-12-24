#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生データの多角的深層分析

course_master.yamlと生データを詳細に分析し、
ROI向上につながる新しい特徴量の候補を発見する。

【分析項目】
1. course_master.yamlの詳細分析
2. 競馬場×距離×馬場別のROI分析
3. 馬体重変化とROIの関係
4. 騎手乗り替わりとROI
5. 逃げ馬数とROI
6. 休み明け（日数）とROI
"""

import pandas as pd
import numpy as np
import yaml
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent


def load_data():
    """データ読み込み"""
    races = pd.read_parquet(project_root / 'keibaai/data/parsed/parquet/races/races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['finish_position'].notna()]
    
    with open(project_root / 'keibaai/configs/course_master.yaml', 'r', encoding='utf-8') as f:
        course_master = yaml.safe_load(f)
    
    return races, course_master


def calc_roi_for_group(df):
    """人気1位のみに賭けた場合のROIを計算"""
    pop1 = df[df['popularity'] == 1]
    wins = pop1[pop1['finish_position'] == 1]
    if len(pop1) == 0:
        return 0, 0, 0
    roi = wins['win_odds'].sum() / len(pop1) * 100
    hit = len(wins) / len(pop1) * 100
    return roi, hit, len(pop1)


def analyze_course_master(course_master):
    """course_master.yaml詳細分析"""
    print("=" * 60)
    print("1. course_master.yaml 詳細分析")
    print("=" * 60)
    
    # 全コース情報を抽出
    courses = []
    for venue, venue_data in course_master.items():
        if venue == 'venue_meta':
            continue
        for surface, surface_data in venue_data.items():
            if not isinstance(surface_data, dict):
                continue
            for distance, dist_data in surface_data.items():
                if not isinstance(dist_data, dict):
                    continue
                if 'default' in dist_data:
                    info = dist_data['default']
                else:
                    # inner/outer等がある場合は最初のキーを使用
                    first_key = list(dist_data.keys())[0]
                    if isinstance(dist_data[first_key], dict):
                        info = dist_data[first_key]
                    else:
                        info = dist_data
                
                courses.append({
                    'venue': venue,
                    'surface': surface,
                    'distance_m': distance,
                    'slope_percent': info.get('slope_percent', 0),
                    'final_straight_m': info.get('final_straight_m', 0),
                    'corner_count': info.get('corner_count', 0),
                    'start_to_corner_m': info.get('start_to_corner_m', 0)
                })
    
    courses_df = pd.DataFrame(courses)
    
    print(f"\n総コース数: {len(courses_df)}")
    print("\n【坂勾配 Top 10】")
    print(courses_df.nlargest(10, 'slope_percent')[['venue', 'surface', 'distance_m', 'slope_percent']])
    
    print("\n【直線距離 Top 10】")
    print(courses_df.nlargest(10, 'final_straight_m')[['venue', 'surface', 'distance_m', 'final_straight_m']])
    
    print("\n【スタート〜最初のコーナー Top 10】")
    print(courses_df.nlargest(10, 'start_to_corner_m')[['venue', 'surface', 'distance_m', 'start_to_corner_m']])
    
    return courses_df


def analyze_venue_distance_roi(races, test_year=2025):
    """競馬場×距離別ROI分析（2025年）"""
    print("\n" + "=" * 60)
    print("2. 競馬場×距離別 ROI分析 (2025年)")
    print("=" * 60)
    
    test = races[races['race_date'].dt.year == test_year]
    
    results = []
    for venue in test['venue'].unique():
        for surface in ['芝', 'ダート']:
            for dist in test[(test['venue'] == venue) & (test['track_surface'] == surface)]['distance_m'].unique():
                sub = test[(test['venue'] == venue) & (test['track_surface'] == surface) & (test['distance_m'] == dist)]
                if len(sub) < 50:
                    continue
                roi, hit, n = calc_roi_for_group(sub)
                results.append({
                    'venue': venue,
                    'surface': surface,
                    'distance_m': dist,
                    'roi': roi,
                    'hit_rate': hit,
                    'count': n
                })
    
    results_df = pd.DataFrame(results)
    
    print("\n【高ROI Top 15】")
    print(results_df.nlargest(15, 'roi').to_string(index=False))
    
    print("\n【低ROI Top 10】")
    print(results_df.nsmallest(10, 'roi').to_string(index=False))
    
    return results_df


def analyze_horse_weight_change(races, test_year=2025):
    """馬体重変化とROIの関係"""
    print("\n" + "=" * 60)
    print("3. 馬体重変化とROI (2025年)")
    print("=" * 60)
    
    test = races[races['race_date'].dt.year == test_year].copy()
    
    if 'horse_weight_change' not in test.columns:
        print("horse_weight_change列がありません")
        return None
    
    test['weight_change_cat'] = pd.cut(
        test['horse_weight_change'].fillna(0),
        bins=[-100, -20, -10, -4, 4, 10, 20, 100],
        labels=['-20以下', '-20~-10', '-10~-4', '-4~+4', '+4~+10', '+10~+20', '+20以上']
    )
    
    results = []
    for cat in test['weight_change_cat'].dropna().unique():
        sub = test[test['weight_change_cat'] == cat]
        roi, hit, n = calc_roi_for_group(sub)
        results.append({'category': cat, 'roi': roi, 'hit_rate': hit, 'count': n})
    
    results_df = pd.DataFrame(results).sort_values('category')
    print(results_df.to_string(index=False))
    
    return results_df


def analyze_days_since_last_race(races, test_year=2025):
    """休み明け日数とROI"""
    print("\n" + "=" * 60)
    print("4. 休み明け日数とROI (2025年)")
    print("=" * 60)
    
    test = races[races['race_date'].dt.year == test_year].copy()
    
    if 'days_since_last_race' not in test.columns:
        # 計算してみる
        test = test.sort_values(['horse_id', 'race_date'])
        test['prev_race_date'] = test.groupby('horse_id')['race_date'].shift(1)
        test['days_since_last_race'] = (test['race_date'] - test['prev_race_date']).dt.days
    
    test['rest_category'] = pd.cut(
        test['days_since_last_race'].fillna(365),
        bins=[0, 14, 28, 60, 120, 365, 1000],
        labels=['2週以内', '2-4週', '1-2ヶ月', '2-4ヶ月', '4-12ヶ月', '1年以上']
    )
    
    results = []
    for cat in test['rest_category'].dropna().unique():
        sub = test[test['rest_category'] == cat]
        roi, hit, n = calc_roi_for_group(sub)
        results.append({'category': cat, 'roi': roi, 'hit_rate': hit, 'count': n})
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    return results_df


def analyze_field_size_roi(races, test_year=2025):
    """頭数別ROI"""
    print("\n" + "=" * 60)
    print("5. 頭数別ROI (2025年)")
    print("=" * 60)
    
    test = races[races['race_date'].dt.year == test_year].copy()
    test['field_size'] = test.groupby('race_id')['horse_id'].transform('count')
    
    test['field_cat'] = pd.cut(
        test['field_size'],
        bins=[0, 8, 12, 14, 16, 20],
        labels=['8頭以下', '9-12頭', '13-14頭', '15-16頭', '17頭以上']
    )
    
    results = []
    for cat in test['field_cat'].dropna().unique():
        sub = test[test['field_cat'] == cat]
        roi, hit, n = calc_roi_for_group(sub)
        results.append({'category': cat, 'roi': roi, 'hit_rate': hit, 'count': n})
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    return results_df


def analyze_track_condition_roi(races, test_year=2025):
    """馬場状態別ROI"""
    print("\n" + "=" * 60)
    print("6. 馬場状態別ROI (2025年)")
    print("=" * 60)
    
    test = races[races['race_date'].dt.year == test_year].copy()
    
    results = []
    for cond in test['track_condition'].dropna().unique():
        sub = test[test['track_condition'] == cond]
        roi, hit, n = calc_roi_for_group(sub)
        results.append({'condition': cond, 'roi': roi, 'hit_rate': hit, 'count': n})
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    return results_df


def main():
    print("=" * 60)
    print("生データ多角的深層分析")
    print("=" * 60)
    
    races, course_master = load_data()
    print(f"データ期間: {races['race_date'].min().date()} ~ {races['race_date'].max().date()}")
    print(f"総件数: {len(races):,}")
    
    # 分析実行
    courses_df = analyze_course_master(course_master)
    venue_dist_roi = analyze_venue_distance_roi(races)
    weight_roi = analyze_horse_weight_change(races)
    rest_roi = analyze_days_since_last_race(races)
    field_roi = analyze_field_size_roi(races)
    track_roi = analyze_track_condition_roi(races)
    
    print("\n" + "=" * 60)
    print("分析完了")
    print("=" * 60)
    
    # 発見まとめ
    print("\n【ROI向上につながる可能性のある発見】")
    if venue_dist_roi is not None:
        high_roi = venue_dist_roi[venue_dist_roi['roi'] >= 90]
        print(f"- 高ROI(90%+)の会場×距離パターン: {len(high_roi)}件")
    
    print("- 詳細はログを確認してください")


if __name__ == "__main__":
    main()
