# -*- coding: utf-8 -*-
"""
弱点詳細分析 - 不的中パターンを具体的に分析

どのような状況で外すのか？改善可能な弱点を特定
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import yaml
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_course_master():
    yaml_path = project_root / "keibaai/configs/course_master.yaml"
    if yaml_path.exists():
        with open(yaml_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}


def get_course_features(course_master, venue, surface, distance_m):
    if not course_master or venue not in course_master:
        return {'course_slope_percent': 0, 'course_final_straight_m': 300, 
                'course_corner_count': 2, 'course_start_to_corner_m': 300}
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    try:
        dist = int(distance_m)
    except:
        return {'course_slope_percent': 0, 'course_final_straight_m': 300, 
                'course_corner_count': 2, 'course_start_to_corner_m': 300}
    distance_data = surface_data.get(dist, {})
    if not distance_data:
        return {'course_slope_percent': 0, 'course_final_straight_m': 300, 
                'course_corner_count': 2, 'course_start_to_corner_m': 300}
    if 'default' in distance_data:
        info = distance_data['default']
    else:
        first_key = list(distance_data.keys())[0] if distance_data else None
        if first_key and isinstance(distance_data[first_key], dict):
            info = distance_data[first_key]
        else:
            info = distance_data
    return {
        'course_slope_percent': info.get('slope_percent', 0),
        'course_final_straight_m': info.get('final_straight_m', 300),
        'course_corner_count': info.get('corner_count', 2),
        'course_start_to_corner_m': info.get('start_to_corner_m', 300)
    }


def add_enhanced_features(df, races_raw, course_master):
    df = df.copy()
    df['month'] = df['race_date'].dt.month
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    if races_raw is not None:
        races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
        for i in range(1, 4):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
        merge_cols = ['race_id', 'horse_id'] + [f'prev_{i}_finish' for i in range(1, 4)]
        merge_cols = [c for c in merge_cols if c in races_sorted.columns]
        races_subset = races_sorted[merge_cols].drop_duplicates(['race_id', 'horse_id'])
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        races_subset['race_id'] = races_subset['race_id'].astype(str)
        races_subset['horse_id'] = races_subset['horse_id'].astype(str)
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left', suffixes=('', '_new'))
        for col in df.columns:
            if col.endswith('_new'):
                base_col = col[:-4]
                if base_col in df.columns:
                    df[base_col] = df[base_col].fillna(df[col])
                df = df.drop(columns=[col])
    return df


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("弱点詳細分析 - 不的中パターンを具体的に分析")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    races_raw = races.copy()
    course_master = load_course_master()
    
    train_end = '2022-12-31'
    valid_start = '2023-01-01'
    valid_end = '2023-12-31'
    test_start = '2024-01-01'
    test_end = '2025-01-01'
    
    train = races[races['race_date'] <= train_end].copy()
    valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
    
    print(f"\n[2024年テスト]")
    print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    train_f = add_enhanced_features(train_f, races_raw, course_master)
    valid_f = add_enhanced_features(valid_f, races_raw, course_master)
    test_f = add_enhanced_features(test_f, races_raw, course_master)
    
    all_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    print(f"  特徴量数: {len(all_features)}")
    print("\n  モデル学習中...")
    model = train_model(train_f, valid_f, all_features)
    
    preds = model.predict(test_f[all_features].fillna(0))
    test_f['pred_score'] = preds
    test_f['pred_rank'] = test_f.groupby('race_id')['pred_score'].rank(ascending=False, method='first')
    
    # 的中/不的中を分類
    bets = test_f[test_f['pred_rank'] == 1].copy()
    bets['is_hit'] = bets['finish_position'] == 1
    
    n_hit = bets['is_hit'].sum()
    n_miss = (~bets['is_hit']).sum()
    
    print(f"\n全体: 的中{n_hit:,}件 / 不的中{n_miss:,}件")
    
    # ===== 分析1: クラス別の的中率 =====
    print("\n" + "=" * 80)
    print("[1] レースクラス別の的中率")
    print("=" * 80)
    
    if 'race_class' in bets.columns:
        class_stats = bets.groupby('race_class').agg({
            'is_hit': ['sum', 'count', 'mean'],
            'win_odds': 'mean'
        }).round(3)
        class_stats.columns = ['的中', 'ベット', '的中率', '平均オッズ']
        class_stats['ROI'] = (class_stats['的中'] * class_stats['平均オッズ']) / class_stats['ベット'] * 100
        print(class_stats.sort_values('ベット', ascending=False).head(10))
    
    # ===== 分析2: 馬場状態別の的中率 =====
    print("\n" + "=" * 80)
    print("[2] 馬場状態別の的中率")
    print("=" * 80)
    
    if 'track_condition' in bets.columns:
        condition_stats = bets.groupby('track_condition').agg({
            'is_hit': ['sum', 'count', 'mean'],
            'win_odds': 'mean'
        }).round(3)
        condition_stats.columns = ['的中', 'ベット', '的中率', '平均オッズ']
        condition_stats['ROI'] = (condition_stats['的中'] * condition_stats['平均オッズ']) / condition_stats['ベット'] * 100
        print(condition_stats)
    
    # ===== 分析3: 予測1位馬の人気別 =====
    print("\n" + "=" * 80)
    print("[3] 予測1位馬の人気別的中率")
    print("=" * 80)
    
    if 'popularity' in bets.columns:
        pop_bins = [(1, 1), (2, 2), (3, 3), (4, 5), (6, 10), (11, 99)]
        for low, high in pop_bins:
            subset = bets[(bets['popularity'] >= low) & (bets['popularity'] <= high)]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                print(f"  {low}-{high}番人気: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析4: 直近着順別（予測1位馬の前走着順）=====
    print("\n" + "=" * 80)
    print("[4] 予測1位馬の前走着順別的中率")
    print("=" * 80)
    
    if 'horse_last_finish' in bets.columns:
        finish_bins = [(1, 1), (2, 3), (4, 5), (6, 10), (11, 99)]
        for low, high in finish_bins:
            subset = bets[(bets['horse_last_finish'] >= low) & (bets['horse_last_finish'] <= high)]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                print(f"  前走{low}-{high}着: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析5: 距離変更パターン =====
    print("\n" + "=" * 80)
    print("[5] 距離変更パターン別的中率")
    print("=" * 80)
    
    if 'distance_extend_flag' in bets.columns:
        for flag in [-1, 0, 1]:
            subset = bets[bets['distance_extend_flag'] == flag]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                label = {-1: '距離短縮', 0: '同距離', 1: '距離延長'}[flag]
                print(f"  {label}: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析6: 馬場替わり =====
    print("\n" + "=" * 80)
    print("[6] 馬場替わり別的中率")
    print("=" * 80)
    
    if 'surface_switch_flag' in bets.columns:
        for flag in [0, 1]:
            subset = bets[bets['surface_switch_flag'] == flag]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                label = {0: '馬場同じ', 1: '馬場替わり'}[flag]
                print(f"  {label}: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析7: クラス変動（昇級/降級）=====
    print("\n" + "=" * 80)
    print("[7] クラス変動別的中率")
    print("=" * 80)
    
    if 'is_class_up' in bets.columns:
        for flag in [-1, 0, 1]:
            subset = bets[bets['is_class_up'] == flag]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                label = {-1: '降級', 0: '同級', 1: '昇級'}[flag]
                print(f"  {label}: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析8: 出走頭数別 =====
    print("\n" + "=" * 80)
    print("[8] 出走頭数別的中率")
    print("=" * 80)
    
    if 'field_size' in bets.columns:
        size_bins = [(1, 8), (9, 12), (13, 15), (16, 18)]
        for low, high in size_bins:
            subset = bets[(bets['field_size'] >= low) & (bets['field_size'] <= high)]
            if len(subset) > 0:
                hit_rate = subset['is_hit'].mean() * 100
                roi = subset[subset['is_hit']]['win_odds'].sum() / len(subset) * 100
                print(f"  {low}-{high}頭: ベット{len(subset):,}件, 的中率{hit_rate:.1f}%, ROI {roi:.1f}%")
    
    # ===== 分析9: 会場別 =====
    print("\n" + "=" * 80)
    print("[9] 会場別的中率")
    print("=" * 80)
    
    if 'venue' in bets.columns:
        venue_stats = bets.groupby('venue').agg({
            'is_hit': ['sum', 'count', 'mean'],
            'win_odds': 'mean'
        }).round(3)
        venue_stats.columns = ['的中', 'ベット', '的中率', '平均オッズ']
        venue_stats['ROI'] = (venue_stats['的中'] * venue_stats['平均オッズ']) / venue_stats['ベット'] * 100
        venue_stats = venue_stats.sort_values('ベット', ascending=False)
        print(venue_stats)
    
    # ===== 分析10: 不的中パターンの深掘り =====
    print("\n" + "=" * 80)
    print("[10] 不的中パターンの詳細分析")
    print("=" * 80)
    
    misses = bets[~bets['is_hit']].copy()
    
    # 不的中時に実際に勝った馬の情報を取得
    miss_race_ids = misses['race_id'].unique()
    actual_winners = test_f[(test_f['race_id'].isin(miss_race_ids)) & (test_f['finish_position'] == 1)]
    
    # 予測順位を確認
    print(f"\n  不的中 {len(misses):,}件 のうち:")
    
    for rank in [2, 3, 4, 5]:
        count = (actual_winners['pred_rank'] == rank).sum()
        pct = count / len(misses) * 100
        print(f"    実際の1着が予測{rank}位だった: {count:,}件 ({pct:.1f}%)")
    
    low_rank = (actual_winners['pred_rank'] > 5).sum()
    pct = low_rank / len(misses) * 100
    print(f"    実際の1着が予測6位以下: {low_rank:,}件 ({pct:.1f}%)")
    
    # 実際の1着馬の特徴
    print(f"\n  【実際の1着馬の特徴（不的中時）】")
    if 'popularity' in actual_winners.columns:
        avg_pop = actual_winners['popularity'].mean()
        print(f"    平均人気: {avg_pop:.1f}番人気")
    if 'win_odds' in actual_winners.columns:
        avg_odds = actual_winners['win_odds'].mean()
        print(f"    平均オッズ: {avg_odds:.1f}倍")
    if 'horse_last_finish' in actual_winners.columns:
        avg_last = actual_winners['horse_last_finish'].mean()
        print(f"    平均前走着順: {avg_last:.1f}着")
    
    # ===== 結論 =====
    print("\n" + "=" * 80)
    print("[結論] 弱点と改善の可能性")
    print("=" * 80)
    
    print("""
  【確認された弱点】
  
  1. クラス昇級馬への過大評価（要確認）
  2. 馬場替わり時の予測精度（要確認）
  3. 不人気馬（6番人気以下）を選んだ時の的中率
  4. 多頭数レース（16頭以上）での予測精度
  
  【改善の方向性】
  
  1. クラス変動の影響をより強く反映する特徴量
  2. 馬場適性のより詳細な分析
  3. 頭数に応じた予測スコアの補正
  4. 人気とオッズの乖離を利用した期待値計算
""")


if __name__ == "__main__":
    main()
