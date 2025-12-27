# -*- coding: utf-8 -*-
"""
アンサンブルモデル検証 - μモデル特徴量統合版

V15 Binary と V4.4 Ultra Safe に、μモデル開発で得た特徴量を追加

【追加特徴量（μモデル由来）】
- horse_normalized_time_avg: 正規化タイムの過去平均
- horse_normalized_time_last: 直近の正規化タイム
- horse_avg_passing_order_1: 脚質（1コーナー通過順位の平均）

【アンサンブル手法】
1. 平均アンサンブル: (V15 + V4.4) / 2
2. 加重平均: V15 * w1 + V4.4 * w2
3. 最小ランク: V15とV4.4の両方でTop3に入った馬
4. 積スコア: V15_score * V4.4_score
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
from pathlib import Path
import sys
import yaml
import warnings
warnings.filterwarnings('ignore')

optuna.logging.set_verbosity(optuna.logging.WARNING)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

MIN_BASE_TIME_SAMPLES = 30


def add_mu_features(df, train_df):
    """
    μモデル由来の特徴量を追加
    
    【追加特徴量】
    - horse_normalized_time_avg: 正規化タイムの過去平均
    - horse_normalized_time_last: 直近の正規化タイム
    - horse_avg_passing_order_1: 脚質（1コーナー通過順位の平均）
    
    【リーク対策】
    - 基準タイムはtrain_dfのみで計算
    - 馬の統計は shift(1) + expanding() で過去データのみ使用
    """
    df = df.copy()
    
    # 基準タイム統計を計算（train_dfのみ）
    train_valid = train_df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface', 'track_condition', 'venue'])
    
    # Level1: venue × distance × surface × condition
    level1 = train_valid.groupby(
        ['venue', 'distance_m', 'track_surface', 'track_condition']
    )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
    level1.columns = ['venue', 'distance_m', 'track_surface', 'track_condition', 'bt_l1_mean', 'bt_l1_std', 'bt_l1_count']
    level1['bt_l1_std'] = level1['bt_l1_std'].fillna(2.0).clip(lower=0.5)
    
    # Level2: venue × distance × surface
    level2 = train_valid.groupby(
        ['venue', 'distance_m', 'track_surface']
    )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
    level2.columns = ['venue', 'distance_m', 'track_surface', 'bt_l2_mean', 'bt_l2_std', 'bt_l2_count']
    level2['bt_l2_std'] = level2['bt_l2_std'].fillna(2.0).clip(lower=0.5)
    
    # Level3: distance × surface × condition
    level3 = train_valid.groupby(
        ['distance_m', 'track_surface', 'track_condition']
    )['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
    level3.columns = ['distance_m', 'track_surface', 'track_condition', 'bt_l3_mean', 'bt_l3_std', 'bt_l3_count']
    level3['bt_l3_std'] = level3['bt_l3_std'].fillna(2.0).clip(lower=0.5)
    
    # マージ
    df = df.merge(level1, on=['venue', 'distance_m', 'track_surface', 'track_condition'], how='left')
    df = df.merge(level2, on=['venue', 'distance_m', 'track_surface'], how='left')
    df = df.merge(level3, on=['distance_m', 'track_surface', 'track_condition'], how='left')
    
    # 階層的フォールバック
    def select_base_time(row):
        if pd.notna(row.get('bt_l1_count')) and row['bt_l1_count'] >= MIN_BASE_TIME_SAMPLES:
            return row['bt_l1_mean'], row['bt_l1_std']
        if pd.notna(row.get('bt_l2_count')) and row['bt_l2_count'] >= MIN_BASE_TIME_SAMPLES:
            return row['bt_l2_mean'], row['bt_l2_std']
        if pd.notna(row.get('bt_l3_count')):
            return row['bt_l3_mean'], row['bt_l3_std']
        return np.nan, 2.0
    
    result = df.apply(select_base_time, axis=1, result_type='expand')
    df['_base_mean'] = result[0]
    df['_base_std'] = result[1]
    
    # 正規化タイム
    df['_normalized_time'] = (df['finish_time_seconds'] - df['_base_mean']) / df['_base_std']
    
    # 馬ごとの過去統計（リーク対策: shift + expanding）
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    df['horse_normalized_time_avg'] = (
        df.groupby('horse_id')['_normalized_time']
        .transform(lambda x: x.shift(1).expanding().mean())
    )
    df['horse_normalized_time_last'] = df.groupby('horse_id')['_normalized_time'].shift(1)
    
    # 脚質特徴量
    if 'passing_order_1' in df.columns:
        df['horse_avg_passing_order_1'] = (
            df.groupby('horse_id')['passing_order_1']
            .transform(lambda x: x.shift(1).expanding().mean())
        )
    
    # 作業用カラムを削除
    drop_cols = [
        '_base_mean', '_base_std', '_normalized_time',
        'bt_l1_mean', 'bt_l1_std', 'bt_l1_count',
        'bt_l2_mean', 'bt_l2_std', 'bt_l2_count',
        'bt_l3_mean', 'bt_l3_std', 'bt_l3_count',
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
    
    return df


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
    """拡張特徴量を追加（v1と同じ）"""
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


def train_v15(train_df, valid_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def train_v44(train_df, valid_df, feature_cols, params):
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
    print("アンサンブルモデル検証 - V15 × V4.4")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    races_raw = races.copy()
    course_master = load_course_master()
    
    print(f"\n総データ: {len(races):,}件")
    
    # V15用パラメータ（v1で最適化されたもの）
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    # V4.4用パラメータ
    v44_params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
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
        
        # μモデル由来特徴量を追加
        print("  μ特徴量を追加中...")
        train_f = add_mu_features(train_f, train)
        valid_f = add_mu_features(valid_f, train)  # train期間の基準タイムを使用
        test_f = add_mu_features(test_f, train)    # train期間の基準タイムを使用
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        new_features = [
            'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
            'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
            'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
            'jockey_horse_pair_win_rate', 'jockey_horse_pair_count',
        ]
        
        # μモデル由来特徴量
        mu_features = [
            'horse_normalized_time_avg',
            'horse_normalized_time_last',
            'horse_avg_passing_order_1',
        ]
        new_features.extend(mu_features)
        
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f']:

                new_features.append(f'prev_{i}_{suffix}')
        
        new_features = [f for f in new_features if f in train_f.columns]
        all_features = list(dict.fromkeys(base_features + new_features))
        
        # 両モデルを学習
        print("  V15学習中...")
        model_v15 = train_v15(train_f, valid_f, all_features, v15_params)
        
        print("  V4.4学習中...")
        model_v44 = train_v44(train_f.copy(), valid_f.copy(), all_features, v44_params)
        
        # テストデータで予測
        X_test = test_f[all_features].fillna(0)
        preds_v15 = model_v15.predict(X_test)
        preds_v44 = model_v44.predict(X_test)
        
        # 正規化（0-1スケール）
        preds_v15_norm = (preds_v15 - preds_v15.min()) / (preds_v15.max() - preds_v15.min() + 1e-8)
        preds_v44_norm = (preds_v44 - preds_v44.min()) / (preds_v44.max() - preds_v44.min() + 1e-8)
        
        # 各モデル単独のROI
        roi_v15, _ = calc_roi(test_f, preds_v15)
        roi_v44, _ = calc_roi(test_f, preds_v44)
        
        # アンサンブル手法
        # 1. 平均アンサンブル
        preds_avg = (preds_v15_norm + preds_v44_norm) / 2
        roi_avg, _ = calc_roi(test_f, preds_avg)
        
        # 2. 加重平均（V15:V4.4 = 0.4:0.6, V4.4の方がROI高いから重みを増やす）
        preds_weighted = preds_v15_norm * 0.4 + preds_v44_norm * 0.6
        roi_weighted, _ = calc_roi(test_f, preds_weighted)
        
        # 3. 積スコア（両方で高評価な馬を優先）
        preds_product = preds_v15_norm * preds_v44_norm
        roi_product, _ = calc_roi(test_f, preds_product)
        
        # 4. 最小ランク（両方でTop3に入った馬のみ）
        test_f['rank_v15'] = test_f.groupby('race_id').apply(
            lambda x: pd.Series(preds_v15[x.index]).rank(ascending=False).values
        ).explode().values.astype(float)
        test_f['rank_v44'] = test_f.groupby('race_id').apply(
            lambda x: pd.Series(preds_v44[x.index]).rank(ascending=False).values
        ).explode().values.astype(float)
        test_f['min_rank'] = test_f[['rank_v15', 'rank_v44']].max(axis=1)
        preds_minrank = -test_f['min_rank']  # 小さいほど良いので反転
        roi_minrank, _ = calc_roi(test_f, preds_minrank)
        
        print(f"\n  結果:")
        print(f"    V15単独:    {roi_v15:.1f}%")
        print(f"    V4.4単独:   {roi_v44:.1f}%")
        print(f"    平均:       {roi_avg:.1f}%")
        print(f"    加重(4:6):  {roi_weighted:.1f}%")
        print(f"    積スコア:   {roi_product:.1f}%")
        print(f"    最小ランク: {roi_minrank:.1f}%")
        
        best_ensemble = max(roi_avg, roi_weighted, roi_product, roi_minrank)
        best_single = max(roi_v15, roi_v44)
        
        all_results.append({
            'year': year,
            'v15': roi_v15, 'v44': roi_v44,
            'avg': roi_avg, 'weighted': roi_weighted,
            'product': roi_product, 'minrank': roi_minrank,
            'best_single': best_single, 'best_ensemble': best_ensemble
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("検証結果サマリー（Test ROI）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15':>8} {'V4.4':>8} {'平均':>8} {'加重':>8} {'積':>8} {'最小ランク':>10}")
    print("-" * 70)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v15']:>7.1f}% {r['v44']:>7.1f}% {r['avg']:>7.1f}% {r['weighted']:>7.1f}% {r['product']:>7.1f}% {r['minrank']:>9.1f}%")
    
    avg_v15 = np.mean([r['v15'] for r in all_results])
    avg_v44 = np.mean([r['v44'] for r in all_results])
    avg_avg = np.mean([r['avg'] for r in all_results])
    avg_weighted = np.mean([r['weighted'] for r in all_results])
    avg_product = np.mean([r['product'] for r in all_results])
    avg_minrank = np.mean([r['minrank'] for r in all_results])
    
    print("-" * 70)
    print(f"{'平均':>6} {avg_v15:>7.1f}% {avg_v44:>7.1f}% {avg_avg:>7.1f}% {avg_weighted:>7.1f}% {avg_product:>7.1f}% {avg_minrank:>9.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best_method = max([
        ('V15単独', avg_v15),
        ('V4.4単独', avg_v44),
        ('平均', avg_avg),
        ('加重平均', avg_weighted),
        ('積スコア', avg_product),
        ('最小ランク', avg_minrank),
    ], key=lambda x: x[1])
    
    print(f"\n  最高ROI: {best_method[0]} ({best_method[1]:.1f}%)")
    
    if best_method[1] > max(avg_v15, avg_v44):
        print(f"  → アンサンブルが有効！")
    else:
        print(f"  → 単一モデルの方が良い")


if __name__ == "__main__":
    main()
