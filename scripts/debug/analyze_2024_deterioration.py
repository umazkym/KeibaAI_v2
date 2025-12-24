# -*- coding: utf-8 -*-
"""
2024年の大幅悪化の原因調査

2024年でアンサンブルが-9.5%悪化した原因を詳細に調査
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
    df['week_of_year'] = df['race_date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['race_date'].dt.dayofweek
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    df['is_steep_slope'] = (df['course_slope_percent'] >= 1.5).astype(int)
    df['is_many_corners'] = (df['course_corner_count'] >= 4).astype(int)
    
    if races_raw is not None:
        races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
        for i in range(1, 6):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
            if 'finish_time_seconds' in races_sorted.columns:
                races_sorted[f'prev_{i}_time'] = races_sorted.groupby('horse_id')['finish_time_seconds'].shift(i)
            if 'last_3f_time' in races_sorted.columns:
                races_sorted[f'prev_{i}_last3f'] = races_sorted.groupby('horse_id')['last_3f_time'].shift(i)
        merge_cols = ['race_id', 'horse_id']
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f']:
                col = f'prev_{i}_{suffix}'
                if col in races_sorted.columns:
                    merge_cols.append(col)
        merge_cols = list(set(merge_cols))
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
                else:
                    df[base_col] = df[col]
                df = df.drop(columns=[col])
    
    df['jockey_horse_pair'] = df['jockey_id'].astype(str) + '_' + df['horse_id'].astype(str)
    df_sorted = df.sort_values(['jockey_horse_pair', 'race_date']).copy()
    df_sorted['is_win'] = (df_sorted['finish_position'] == 1).astype(int)
    df_sorted['pair_cum_wins'] = df_sorted.groupby('jockey_horse_pair')['is_win'].cumsum().shift(1).fillna(0)
    df_sorted['pair_cum_races'] = df_sorted.groupby('jockey_horse_pair').cumcount()
    df_sorted['jockey_horse_pair_win_rate'] = np.where(
        df_sorted['pair_cum_races'] > 0,
        df_sorted['pair_cum_wins'] / df_sorted['pair_cum_races'], 0)
    df_sorted['jockey_horse_pair_count'] = df_sorted['pair_cum_races']
    pair_features = df_sorted[['race_id', 'horse_id', 'jockey_horse_pair_win_rate', 'jockey_horse_pair_count']].copy()
    df = df.merge(pair_features, on=['race_id', 'horse_id'], how='left', suffixes=('', '_pair'))
    return df


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def train_v44(train_df, valid_df, feature_cols):
    """V4.4 LambdaRank Model"""
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    for d in [train_df, valid_df]:
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train, 
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("2024年の大幅悪化の原因調査")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    course_master = load_course_master()
    
    # 2024年のテストデータ準備
    train_end = '2022-12-31'
    valid_start = '2023-01-01'
    valid_end = '2023-12-31'
    test_start = '2024-01-01'
    test_end = '2025-01-01'
    
    print("\n" + "=" * 80)
    print("[1] 障害込み vs 障害除外でのV4.4を2024年で比較")
    print("=" * 80)
    
    # ===== 障害込み =====
    print("\n[障害込み]")
    races_with = races.copy()
    races_raw_with = races_with.copy()
    
    train_with = races_with[races_with['race_date'] <= train_end].copy()
    valid_with = races_with[(races_with['race_date'] >= valid_start) & (races_with['race_date'] < valid_end)].copy()
    test_with = races_with[(races_with['race_date'] >= test_start) & (races_with['race_date'] < test_end)].copy()
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train_with, pedigrees, corners, race_details, horses_df=horses)
    
    train_with_f = engine.transform(train_with)
    valid_with_f = engine.transform(valid_with)
    test_with_f = engine.transform(test_with)
    
    train_with_f = add_enhanced_features(train_with_f, races_raw_with, course_master)
    valid_with_f = add_enhanced_features(valid_with_f, races_raw_with, course_master)
    test_with_f = add_enhanced_features(test_with_f, races_raw_with, course_master)
    
    base_features = [c for c in engine.get_feature_columns() if c in train_with_f.columns]
    new_features = [
        'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
        'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
        'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
        'jockey_horse_pair_win_rate', 'jockey_horse_pair_count',
    ]
    for i in range(1, 6):
        for suffix in ['finish', 'time', 'last3f']:
            new_features.append(f'prev_{i}_{suffix}')
    new_features = [f for f in new_features if f in train_with_f.columns]
    all_features = list(dict.fromkeys(base_features + new_features))
    
    print(f"  Train: {len(train_with_f):,}, Valid: {len(valid_with_f):,}, Test: {len(test_with_f):,}")
    
    model_with = train_v44(train_with_f.copy(), valid_with_f.copy(), all_features)
    preds_with = model_with.predict(test_with_f[all_features].fillna(0))
    roi_with, hr_with = calc_roi(test_with_f, preds_with)
    print(f"  V4.4 ROI: {roi_with:.1f}%, Hit Rate: {hr_with:.1f}%")
    
    # 平地/障害別のROI
    test_with_f['preds'] = preds_with
    test_flat = test_with_f[test_with_f['track_surface'] != '障害']
    test_obstacle = test_with_f[test_with_f['track_surface'] == '障害']
    
    if len(test_flat) > 0:
        roi_flat_with, _ = calc_roi(test_flat, test_flat['preds'])
        print(f"    - 平地のみ ROI: {roi_flat_with:.1f}% (n={len(test_flat):,})")
    
    if len(test_obstacle) > 0:
        roi_obs_with, _ = calc_roi(test_obstacle, test_obstacle['preds'])
        print(f"    - 障害のみ ROI: {roi_obs_with:.1f}% (n={len(test_obstacle):,})")
    
    # ===== 障害除外 =====
    print("\n[障害除外]")
    races_without = races[races['track_surface'] != '障害'].copy()
    races_raw_without = races_without.copy()
    
    train_without = races_without[races_without['race_date'] <= train_end].copy()
    valid_without = races_without[(races_without['race_date'] >= valid_start) & (races_without['race_date'] < valid_end)].copy()
    test_without = races_without[(races_without['race_date'] >= test_start) & (races_without['race_date'] < test_end)].copy()
    
    engine2 = LeakFreeFeatureEngineerV15()
    engine2.fit(train_without, pedigrees, corners, race_details, horses_df=horses)
    
    train_without_f = engine2.transform(train_without)
    valid_without_f = engine2.transform(valid_without)
    test_without_f = engine2.transform(test_without)
    
    train_without_f = add_enhanced_features(train_without_f, races_raw_without, course_master)
    valid_without_f = add_enhanced_features(valid_without_f, races_raw_without, course_master)
    test_without_f = add_enhanced_features(test_without_f, races_raw_without, course_master)
    
    all_features2 = list(dict.fromkeys([c for c in engine2.get_feature_columns() if c in train_without_f.columns] + new_features))
    
    print(f"  Train: {len(train_without_f):,}, Valid: {len(valid_without_f):,}, Test: {len(test_without_f):,}")
    
    model_without = train_v44(train_without_f.copy(), valid_without_f.copy(), all_features2)
    preds_without = model_without.predict(test_without_f[all_features2].fillna(0))
    roi_without, hr_without = calc_roi(test_without_f, preds_without)
    print(f"  V4.4 ROI: {roi_without:.1f}%, Hit Rate: {hr_without:.1f}%")
    
    # ===== 比較 =====
    print("\n" + "=" * 80)
    print("[2] 詳細比較")
    print("=" * 80)
    
    print(f"""
  【V4.4 2024年テストのROI比較】
  - 障害込み (テスト全体): {roi_with:.1f}%
  - 障害込み (平地のみ):   {roi_flat_with:.1f}%
  - 障害除外:             {roi_without:.1f}%
  
  【差分】
  - 障害込み全体 vs 障害除外: {roi_with - roi_without:.1f}%
  - 平地のみ vs 障害除外:    {roi_flat_with - roi_without:.1f}%
""")
    
    if roi_flat_with > roi_without:
        print("  【発見】")
        print("  障害込みで学習したモデルの方が、平地レースでも性能が良い！")
        print("  → 障害レースのデータが正則化効果として機能している")
    else:
        print("  【発見】")
        print("  障害レース自体が高ROIで、全体を押し上げていた")
    
    # ===== Feature Importance比較 =====
    print("\n" + "=" * 80)
    print("[3] Feature Importance比較（Top 20）")
    print("=" * 80)
    
    importance_with = pd.DataFrame({
        'feature': all_features,
        'importance': model_with.feature_importances_
    }).sort_values('importance', ascending=False).head(20)
    
    importance_without = pd.DataFrame({
        'feature': all_features2,
        'importance': model_without.feature_importances_
    }).sort_values('importance', ascending=False).head(20)
    
    print("\n[障害込みモデル Top 20]")
    for i, row in importance_with.iterrows():
        print(f"  {row['feature']}: {row['importance']:.0f}")
    
    print("\n[障害除外モデル Top 20]")
    for i, row in importance_without.iterrows():
        print(f"  {row['feature']}: {row['importance']:.0f}")


if __name__ == "__main__":
    main()
