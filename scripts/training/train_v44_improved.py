# -*- coding: utf-8 -*-
"""
V4.4改善検証 - V17特徴量をV4.4に適用

V4.4（LambdaRank）にV17の新特徴量を適用して検証
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


def add_v17_features(df, races_raw, course_master):
    """V17改善特徴量を追加"""
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
        for i in range(1, 6):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
        merge_cols = ['race_id', 'horse_id'] + [f'prev_{i}_finish' for i in range(1, 6)]
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
    
    # 多頭数補正
    if 'field_size' in df.columns:
        df['is_large_field'] = (df['field_size'] >= 16).astype(int)
        df['field_difficulty'] = np.log1p(df['field_size'])
    
    # 復帰馬評価
    if 'prev_1_finish' in df.columns:
        prev_1 = df['prev_1_finish'].fillna(99)
        df['is_recovery_candidate'] = ((prev_1 >= 4) & (prev_1 <= 5)).astype(int)
        if 'prev_2_finish' in df.columns:
            prev_2 = df['prev_2_finish'].fillna(99)
            df['finish_trend'] = prev_2 - prev_1
            df['is_improving'] = (df['finish_trend'] > 0).astype(int)
        finish_cols = [c for c in ['prev_1_finish', 'prev_2_finish', 'prev_3_finish'] if c in df.columns]
        if len(finish_cols) > 0:
            df['best_recent_finish'] = df[finish_cols].min(axis=1)
            df['finish_variance'] = df[finish_cols].std(axis=1).fillna(0)
    
    # 展開特徴量
    if 'horse_number' in df.columns and 'field_size' in df.columns:
        df['relative_post'] = df['horse_number'] / df['field_size']
        df['is_inner'] = (df['relative_post'] <= 0.33).astype(int)
        df['is_outer'] = (df['relative_post'] >= 0.67).astype(int)
    
    # 穴馬評価
    if 'popularity' in df.columns:
        pop = df['popularity'].fillna(99)
        df['is_mid_popularity'] = ((pop >= 6) & (pop <= 10)).astype(int)
    
    return df


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


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bets = d[d['rank_pred'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("V4.4改善検証 - V17特徴量をV4.4に適用")
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
    
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    results_v44 = []
    results_v44_improved = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        v44_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # V17特徴量を追加
        train_imp = add_v17_features(train_f, races_raw, course_master)
        valid_imp = add_v17_features(valid_f, races_raw, course_master)
        test_imp = add_v17_features(test_f, races_raw, course_master)
        
        new_features = [
            'is_large_field', 'field_difficulty', 'is_recovery_candidate',
            'finish_trend', 'is_improving', 'best_recent_finish', 'finish_variance',
            'relative_post', 'is_inner', 'is_outer', 'is_mid_popularity',
            'month_sin', 'month_cos', 'course_slope_percent', 'course_final_straight_m',
        ]
        for i in range(1, 6):
            new_features.append(f'prev_{i}_finish')
        
        v44_imp_features = list(dict.fromkeys(v44_features + [f for f in new_features if f in train_imp.columns]))
        
        print(f"  V4.4特徴量: {len(v44_features)}, V4.4改善: {len(v44_imp_features)}")
        
        # V4.4オリジナル
        print("  V4.4学習中...")
        model_v44 = train_v44(train_f.copy(), valid_f.copy(), v44_features)
        preds_v44 = model_v44.predict(test_f[v44_features].fillna(0))
        roi_v44, hr_v44 = calc_roi(test_f, preds_v44)
        
        # V4.4改善版
        print("  V4.4改善版学習中...")
        model_v44_imp = train_v44(train_imp.copy(), valid_imp.copy(), v44_imp_features)
        preds_v44_imp = model_v44_imp.predict(test_imp[v44_imp_features].fillna(0))
        roi_v44_imp, hr_v44_imp = calc_roi(test_imp, preds_v44_imp)
        
        print(f"\n  【V4.4】 ROI: {roi_v44:.1f}%, 的中率: {hr_v44:.1f}%")
        print(f"  【V4.4改善】 ROI: {roi_v44_imp:.1f}%, 的中率: {hr_v44_imp:.1f}%")
        
        diff = roi_v44_imp - roi_v44
        print(f"  → 差分: {diff:+.1f}%")
        
        results_v44.append({'year': year, 'roi': roi_v44, 'hit_rate': hr_v44})
        results_v44_improved.append({'year': year, 'roi': roi_v44_imp, 'hit_rate': hr_v44_imp})
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間検証サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V4.4 ROI':>10} {'V4.4改善':>10} {'差分':>8}")
    print("-" * 40)
    
    for v44, v44_imp in zip(results_v44, results_v44_improved):
        diff = v44_imp['roi'] - v44['roi']
        print(f"{v44['year']:>6} {v44['roi']:>9.1f}% {v44_imp['roi']:>9.1f}% {diff:>+7.1f}%")
    
    avg_v44 = np.mean([r['roi'] for r in results_v44])
    avg_v44_imp = np.mean([r['roi'] for r in results_v44_improved])
    avg_diff = avg_v44_imp - avg_v44
    
    print("-" * 40)
    print(f"{'平均':>6} {avg_v44:>9.1f}% {avg_v44_imp:>9.1f}% {avg_diff:>+7.1f}%")
    
    if avg_diff > 0:
        print(f"\n  ✅ V4.4改善効果: +{avg_diff:.1f}%")
    else:
        print(f"\n  ❌ V4.4改善効果なし: {avg_diff:.1f}%")


if __name__ == "__main__":
    main()
