# -*- coding: utf-8 -*-
"""
V16独立検証 - リークチェック

【検証項目】
1. 同着補正後の1位が複数いるレースの数を確認
2. 同着補正なしでの障害レース除外のみの結果（V16a）
3. 同着補正ありでの結果（V16b）を比較
4. ROI計算方法のリークチェック
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


def analyze_same_position(df):
    """同着補正後の1位が複数いるレースを分析"""
    print("\n[同着補正の分析]")
    
    # レースごとの1位の数を計算
    first_place_count = df[df['finish_position'] == 1].groupby('race_id').size()
    
    total_races = df['race_id'].nunique()
    races_with_multi_first = (first_place_count > 1).sum()
    
    print(f"  総レース数: {total_races:,}")
    print(f"  1位が1頭のレース: {total_races - races_with_multi_first:,} ({(total_races - races_with_multi_first)/total_races*100:.1f}%)")
    print(f"  1位が複数のレース: {races_with_multi_first:,} ({races_with_multi_first/total_races*100:.1f}%)")
    
    # 1位の数の分布
    print(f"\n  [1位の数の分布]")
    for cnt in sorted(first_place_count.unique()):
        n = (first_place_count == cnt).sum()
        print(f"    {cnt}頭: {n:,}レース ({n/total_races*100:.1f}%)")
    
    return races_with_multi_first, total_races


def preprocess_v16a(df):
    """障害レース除外のみ（同着補正なし）"""
    df = df.copy()
    original_len = len(df)
    df = df[df['track_surface'] != '障害'].copy()
    excluded = original_len - len(df)
    print(f"  障害レース除外: {excluded:,}件")
    return df


def preprocess_v16b(df):
    """障害レース除外 + 同着補正"""
    df = df.copy()
    
    # 障害レース除外
    original_len = len(df)
    df = df[df['track_surface'] != '障害'].copy()
    excluded = original_len - len(df)
    print(f"  障害レース除外: {excluded:,}件")
    
    # 同着補正
    if 'finish_time_seconds' in df.columns:
        def adjust_position(group):
            if group['finish_time_seconds'].isna().all():
                return group
            time_to_min_pos = group.groupby('finish_time_seconds')['finish_position'].transform('min')
            group = group.copy()
            group.loc[group['finish_time_seconds'].notna(), 'finish_position_adjusted'] = time_to_min_pos[group['finish_time_seconds'].notna()]
            group.loc[group['finish_time_seconds'].isna(), 'finish_position_adjusted'] = group.loc[group['finish_time_seconds'].isna(), 'finish_position']
            return group
        
        df = df.groupby('race_id', group_keys=False).apply(adjust_position)
        same_pos_cases = (df['finish_position'] != df['finish_position_adjusted']).sum()
        print(f"  同着補正: {same_pos_cases:,}件")
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


def calc_roi_strict(df, preds):
    """
    厳密なROI計算（1位が複数いる場合でも1レース1ベット）
    予測1位の馬が実際の1位（複数いる場合はいずれか）であればヒット
    """
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 予測1位のみベット
    bet_df = d[d['rank_pred'] == 1]
    
    # ヒット判定: 予測1位の馬が実際に1位か
    hits = bet_df[bet_df['finish_position'] == 1]
    
    if len(bet_df) == 0:
        return 0, 0, 0
    
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    
    return roi, hit_rate, len(bet_df)


def calc_roi_fair(df, preds, original_finish_col='finish_position_original'):
    """
    公正なROI計算（元の着順でヒット判定）
    同着補正後の着順ではなく、元の着順でヒット判定
    """
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1]
    
    # 元の着順でヒット判定
    if original_finish_col in d.columns:
        hits = bet_df[bet_df[original_finish_col] == 1]
    else:
        hits = bet_df[bet_df['finish_position'] == 1]
    
    if len(bet_df) == 0:
        return 0, 0, 0
    
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    
    return roi, hit_rate, len(bet_df)


def train_model(train_df, valid_df, feature_cols, params):
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
    print("V16独立検証 - リークチェック")
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
    
    # パラメータ
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    course_master = load_course_master()
    
    # ===== 検証1: 同着補正の影響分析 =====
    print("\n" + "=" * 80)
    print("[検証1] 同着補正後の1位が複数いるレースの分析")
    print("=" * 80)
    
    races_v16b = preprocess_v16b(races.copy())
    multi_first, total_races = analyze_same_position(races_v16b)
    
    # ===== 検証2: 同着補正なし vs あり の比較 =====
    print("\n" + "=" * 80)
    print("[検証2] 同着補正なし(V16a) vs あり(V16b) の比較")
    print("=" * 80)
    
    # 2025年のみで検証
    year = '2025'
    train_end = '2023-12-31'
    valid_start = '2024-01-01'
    valid_end = '2024-12-31'
    test_start = '2025-01-01'
    test_end = '2025-11-01'
    
    # V16a: 同着補正なし
    print("\n[V16a: 障害除外のみ（同着補正なし）]")
    races_a = preprocess_v16a(races.copy())
    races_raw_a = races_a.copy()
    
    train_a = races_a[races_a['race_date'] <= train_end].copy()
    valid_a = races_a[(races_a['race_date'] >= valid_start) & (races_a['race_date'] < valid_end)].copy()
    test_a = races_a[(races_a['race_date'] >= test_start) & (races_a['race_date'] < test_end)].copy()
    
    print(f"  Train: {len(train_a):,}, Valid: {len(valid_a):,}, Test: {len(test_a):,}")
    
    engine_a = LeakFreeFeatureEngineerV15()
    engine_a.fit(train_a, pedigrees, corners, race_details, horses_df=horses)
    
    train_af = engine_a.transform(train_a)
    valid_af = engine_a.transform(valid_a)
    test_af = engine_a.transform(test_a)
    
    train_af = add_enhanced_features(train_af, races_raw_a, course_master)
    valid_af = add_enhanced_features(valid_af, races_raw_a, course_master)
    test_af = add_enhanced_features(test_af, races_raw_a, course_master)
    
    base_features = [c for c in engine_a.get_feature_columns() if c in train_af.columns]
    new_features = [
        'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
        'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
        'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
        'jockey_horse_pair_win_rate', 'jockey_horse_pair_count',
    ]
    for i in range(1, 6):
        for suffix in ['finish', 'time', 'last3f']:
            new_features.append(f'prev_{i}_{suffix}')
    new_features = [f for f in new_features if f in train_af.columns]
    all_features = list(dict.fromkeys(base_features + new_features))
    
    model_a = train_model(train_af, valid_af, all_features, params)
    preds_a = model_a.predict(test_af[all_features].fillna(0))
    roi_a, hr_a, bets_a = calc_roi_strict(test_af, preds_a)
    print(f"  Test ROI: {roi_a:.1f}% (的中率: {hr_a:.1f}%, ベット数: {bets_a:,})")
    
    # V16b: 同着補正あり
    print("\n[V16b: 障害除外 + 同着補正]")
    races_b = preprocess_v16b(races.copy())
    races_raw_b = races_b.copy()
    
    train_b = races_b[races_b['race_date'] <= train_end].copy()
    valid_b = races_b[(races_b['race_date'] >= valid_start) & (races_b['race_date'] < valid_end)].copy()
    test_b = races_b[(races_b['race_date'] >= test_start) & (races_b['race_date'] < test_end)].copy()
    
    print(f"  Train: {len(train_b):,}, Valid: {len(valid_b):,}, Test: {len(test_b):,}")
    
    engine_b = LeakFreeFeatureEngineerV15()
    engine_b.fit(train_b, pedigrees, corners, race_details, horses_df=horses)
    
    train_bf = engine_b.transform(train_b)
    valid_bf = engine_b.transform(valid_b)
    test_bf = engine_b.transform(test_b)
    
    train_bf = add_enhanced_features(train_bf, races_raw_b, course_master)
    valid_bf = add_enhanced_features(valid_bf, races_raw_b, course_master)
    test_bf = add_enhanced_features(test_bf, races_raw_b, course_master)
    
    all_features_b = list(dict.fromkeys([c for c in engine_b.get_feature_columns() if c in train_bf.columns] + new_features))
    
    model_b = train_model(train_bf, valid_bf, all_features_b, params)
    preds_b = model_b.predict(test_bf[all_features_b].fillna(0))
    
    # 同着補正後の着順でROI計算
    roi_b, hr_b, bets_b = calc_roi_strict(test_bf, preds_b)
    print(f"  Test ROI (補正後着順): {roi_b:.1f}% (的中率: {hr_b:.1f}%, ベット数: {bets_b:,})")
    
    # 元の着順でROI計算（公正な評価）
    roi_b_fair, hr_b_fair, bets_b_fair = calc_roi_fair(test_bf, preds_b)
    print(f"  Test ROI (元の着順):   {roi_b_fair:.1f}% (的中率: {hr_b_fair:.1f}%, ベット数: {bets_b_fair:,})")
    
    # ===== 検証3: リークの可能性チェック =====
    print("\n" + "=" * 80)
    print("[検証3] リークの可能性チェック")
    print("=" * 80)
    
    # 1位複数レースでの的中率
    first_place_count = test_bf[test_bf['finish_position'] == 1].groupby('race_id').size()
    multi_first_races = first_place_count[first_place_count > 1].index
    single_first_races = first_place_count[first_place_count == 1].index
    
    test_bf_multi = test_bf[test_bf['race_id'].isin(multi_first_races)]
    test_bf_single = test_bf[test_bf['race_id'].isin(single_first_races)]
    
    if len(test_bf_multi) > 0:
        preds_multi = model_b.predict(test_bf_multi[all_features_b].fillna(0))
        roi_multi, hr_multi, _ = calc_roi_strict(test_bf_multi, preds_multi)
        print(f"  1位複数レースでのROI: {roi_multi:.1f}% (的中率: {hr_multi:.1f}%, レース数: {len(multi_first_races):,})")
    
    if len(test_bf_single) > 0:
        preds_single = model_b.predict(test_bf_single[all_features_b].fillna(0))
        roi_single, hr_single, _ = calc_roi_strict(test_bf_single, preds_single)
        print(f"  1位単独レースでのROI: {roi_single:.1f}% (的中率: {hr_single:.1f}%, レース数: {len(single_first_races):,})")
    
    # ===== 結論 =====
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    print(f"""
  【同着補正の影響】
  - 1位複数レース: {multi_first:,}件 ({multi_first/total_races*100:.1f}%)
  
  【V16a vs V16b（2025年）】
  - V16a (障害除外のみ):       Test ROI {roi_a:.1f}%
  - V16b (障害除外+同着補正):  Test ROI {roi_b:.1f}% (補正後着順)
  - V16b (障害除外+同着補正):  Test ROI {roi_b_fair:.1f}% (元の着順)
  
  【リーク判定】
""")
    
    if roi_b > roi_b_fair + 5:
        print("  ⚠️ リークの可能性あり: 同着補正がROIを人為的に押し上げている")
        print(f"     補正後着順と元着順でROI差: {roi_b - roi_b_fair:.1f}%")
    else:
        print("  ✅ リークなし: 同着補正の影響は限定的")
    
    print(f"\n  【推奨】")
    print(f"  - V16a (障害除外のみ) を採用: {roi_a:.1f}%")
    print(f"  - 同着補正は使用しない（リーク/過適合リスク）")


if __name__ == "__main__":
    main()
