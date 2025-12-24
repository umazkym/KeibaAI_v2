# -*- coding: utf-8 -*-
"""
V16モデル - 前処理強化版

【追加前処理】
1. 障害レース除外（track_surface == '障害'を除外）
2. 同着処理（同タイム馬は同じ着順に補正）

【リーク・過学習対策】
- 時系列分割を維持
- 正則化強化済みパラメータ
- Gap監視
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


def preprocess_v16(df):
    """
    V16用前処理
    
    【処理内容】
    1. 障害レース除外
    2. 同着処理（同タイム馬は同じ着順に補正）
    
    【リーク対策】
    - 同着処理はfinish_time_secondsを使用（レース結果の正確な反映）
    - 未来情報は使用しない
    """
    df = df.copy()
    
    # 1. 障害レース除外
    original_len = len(df)
    df = df[df['track_surface'] != '障害'].copy()
    excluded = original_len - len(df)
    print(f"  障害レース除外: {excluded:,}件 ({excluded/original_len*100:.1f}%)")
    
    # 2. 同着処理（同タイム馬は同じ着順に補正）
    # 注意: これはラベル（finish_position）の補正であり、リークにはならない
    # 理由: finish_time_secondsとfinish_positionは同時点で確定するため
    if 'finish_time_seconds' in df.columns:
        # レースごとにタイムでグループ化し、同タイムなら最小着順を使用
        def adjust_position(group):
            if group['finish_time_seconds'].isna().all():
                return group
            # 同タイムの馬は同じ着順（最小値）にする
            time_to_min_pos = group.groupby('finish_time_seconds')['finish_position'].transform('min')
            group = group.copy()
            group.loc[group['finish_time_seconds'].notna(), 'finish_position_adjusted'] = time_to_min_pos[group['finish_time_seconds'].notna()]
            group.loc[group['finish_time_seconds'].isna(), 'finish_position_adjusted'] = group.loc[group['finish_time_seconds'].isna(), 'finish_position']
            return group
        
        df = df.groupby('race_id', group_keys=False).apply(adjust_position)
        
        # 同着補正の効果を確認
        same_pos_cases = (df['finish_position'] != df['finish_position_adjusted']).sum()
        print(f"  同着補正: {same_pos_cases:,}件 ({same_pos_cases/len(df)*100:.2f}%)")
        
        # 補正後の着順を使用
        df['finish_position_original'] = df['finish_position']
        df['finish_position'] = df['finish_position_adjusted']
    
    return df


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


def train_v16(train_df, valid_df, feature_cols, params):
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
    print("V16モデル - 前処理強化版（障害レース除外 + 同着処理）")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n元データ: {len(races):,}件")
    
    # V16前処理
    print("\n[V16前処理]")
    races = preprocess_v16(races)
    
    print(f"\n前処理後: {len(races):,}件")
    
    races_raw = races.copy()
    course_master = load_course_master()
    
    # 強正則化パラメータ
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
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
        
        train_f = add_enhanced_features(train_f, races_raw, course_master)
        valid_f = add_enhanced_features(valid_f, races_raw, course_master)
        test_f = add_enhanced_features(test_f, races_raw, course_master)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        new_features = [
            'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
            'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
            'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
            'jockey_horse_pair_win_rate', 'jockey_horse_pair_count',
        ]
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f']:
                new_features.append(f'prev_{i}_{suffix}')
        new_features = [f for f in new_features if f in train_f.columns]
        all_features = list(dict.fromkeys(base_features + new_features))
        
        # V16モデル学習
        model = train_v16(train_f, valid_f, all_features, params)
        
        # 予測
        X_train = train_f[all_features].fillna(0)
        X_valid = valid_f[all_features].fillna(0)
        X_test = test_f[all_features].fillna(0)
        
        preds_train = model.predict(X_train)
        preds_valid = model.predict(X_valid)
        preds_test = model.predict(X_test)
        
        train_roi, _ = calc_roi(train_f, preds_train)
        valid_roi, _ = calc_roi(valid_f, preds_valid)
        test_roi, _ = calc_roi(test_f, preds_test)
        
        gap = valid_roi - test_roi
        
        print(f"  Train ROI: {train_roi:.1f}%")
        print(f"  Valid ROI: {valid_roi:.1f}%")
        print(f"  Test ROI:  {test_roi:.1f}%")
        print(f"  Gap:       {gap:.1f}%")
        
        all_results.append({
            'year': year,
            'train_roi': train_roi,
            'valid_roi': valid_roi,
            'test_roi': test_roi,
            'gap': gap
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("V16 検証結果サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'Train ROI':>12} {'Valid ROI':>12} {'Test ROI':>12} {'Gap':>10}")
    print("-" * 60)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['train_roi']:>11.1f}% {r['valid_roi']:>11.1f}% {r['test_roi']:>11.1f}% {r['gap']:>9.1f}%")
    
    avg_test = np.mean([r['test_roi'] for r in all_results])
    avg_gap = np.mean([r['gap'] for r in all_results])
    
    print("-" * 60)
    print(f"{'平均':>6} {'-':>12} {'-':>12} {avg_test:>11.1f}% {avg_gap:>9.1f}%")
    
    print("\n" + "=" * 80)
    print("V15 vs V16 比較")
    print("=" * 80)
    print(f"\n  V15 (従来): 76.1%")
    print(f"  V16 (前処理強化): {avg_test:.1f}%")
    print(f"  差分: {avg_test - 76.1:.1f}%")
    
    if avg_test > 76.1:
        print(f"\n  → V16が改善！")
    else:
        print(f"\n  → V15が良い（前処理強化は効果なし）")


if __name__ == "__main__":
    main()
