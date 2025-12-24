# -*- coding: utf-8 -*-
"""
V15/V4.4 改善版v2 - 追加特徴量 + Optuna50トライアル

【追加特徴量（v2で新規）】
- 馬体重変動: horse_weight_change, 過去5走の馬体重変動
- 斤量関連: basis_weight, 斤量差（前走比）
- 距離変更: distance_change（前走との距離差）
- 前走通過順位: prev_passing_order_1-4

【Optuna強化】
- トライアル数: 30 → 50
- 探索範囲: より広い範囲で探索
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


def add_enhanced_features_v2(df, races_raw, course_master):
    """拡張特徴量v2を追加"""
    df = df.copy()
    
    # ===== 1. 日付sin/cos特徴量 =====
    df['month'] = df['race_date'].dt.month
    df['week_of_year'] = df['race_date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['race_date'].dt.dayofweek
    
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    # ===== 2. コース特徴量 =====
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    df['is_steep_slope'] = (df['course_slope_percent'] >= 1.5).astype(int)
    df['is_many_corners'] = (df['course_corner_count'] >= 4).astype(int)
    
    # ===== 3. 過去レース生値（shift適用） =====
    if races_raw is not None:
        races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
        
        for i in range(1, 6):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
            if 'finish_time_seconds' in races_sorted.columns:
                races_sorted[f'prev_{i}_time'] = races_sorted.groupby('horse_id')['finish_time_seconds'].shift(i)
            if 'last_3f_time' in races_sorted.columns:
                races_sorted[f'prev_{i}_last3f'] = races_sorted.groupby('horse_id')['last_3f_time'].shift(i)
            
            # v2追加: 前走馬体重変動
            if 'horse_weight_change' in races_sorted.columns:
                races_sorted[f'prev_{i}_weight_change'] = races_sorted.groupby('horse_id')['horse_weight_change'].shift(i)
            
            # v2追加: 前走斤量
            if 'basis_weight' in races_sorted.columns:
                races_sorted[f'prev_{i}_basis_weight'] = races_sorted.groupby('horse_id')['basis_weight'].shift(i)
            
            # v2追加: 前走距離
            if 'distance_m' in races_sorted.columns:
                races_sorted[f'prev_{i}_distance'] = races_sorted.groupby('horse_id')['distance_m'].shift(i)
            
            # v2追加: 前走1コーナー通過順位
            if 'passing_order_1' in races_sorted.columns:
                races_sorted[f'prev_{i}_passing_order'] = races_sorted.groupby('horse_id')['passing_order_1'].shift(i)
        
        # v2追加: 前走との距離差
        if 'distance_m' in races_sorted.columns and 'prev_1_distance' in races_sorted.columns:
            races_sorted['distance_change'] = races_sorted['distance_m'] - races_sorted['prev_1_distance']
        
        # v2追加: 前走との斤量差
        if 'basis_weight' in races_sorted.columns and 'prev_1_basis_weight' in races_sorted.columns:
            races_sorted['weight_change_from_prev'] = races_sorted['basis_weight'] - races_sorted['prev_1_basis_weight']
        
        # マージ用カラム
        merge_cols = ['race_id', 'horse_id']
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f', 'weight_change', 'basis_weight', 'distance', 'passing_order']:
                col = f'prev_{i}_{suffix}'
                if col in races_sorted.columns:
                    merge_cols.append(col)
        
        if 'distance_change' in races_sorted.columns:
            merge_cols.append('distance_change')
        if 'weight_change_from_prev' in races_sorted.columns:
            merge_cols.append('weight_change_from_prev')
        
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
    
    # ===== 4. 騎手×馬の相性 =====
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


def train_v15_improved(train_df, valid_df, test_df, feature_cols, params=None):
    if params is None:
        params = {
            'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
            'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
            'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
            'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
        }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    X_test = test_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    
    train_preds = model.predict(X_train)
    valid_preds = model.predict(X_valid)
    test_preds = model.predict(X_test)
    
    train_roi, _ = calc_roi(train_df, train_preds)
    valid_roi, _ = calc_roi(valid_df, valid_preds)
    test_roi, _ = calc_roi(test_df, test_preds)
    
    return model, train_roi, valid_roi, test_roi


def train_v44_improved(train_df, valid_df, test_df, feature_cols, params=None):
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    for d in [train_df, valid_df, test_df]:
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    if params is None:
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
    X_test = test_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train, 
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    
    train_preds = model.predict(X_train)
    valid_preds = model.predict(X_valid)
    test_preds = model.predict(X_test)
    
    train_roi, _ = calc_roi(train_df, train_preds)
    valid_roi, _ = calc_roi(valid_df, valid_preds)
    test_roi, _ = calc_roi(test_df, test_preds)
    
    return model, train_roi, valid_roi, test_roi


def optuna_optimize_v15(train_df, valid_df, test_df, feature_cols, n_trials=50):
    def objective(trial):
        params = {
            'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
            'max_depth': trial.suggest_int('max_depth', 2, 6),
            'num_leaves': trial.suggest_int('num_leaves', 8, 50),
            'min_child_samples': trial.suggest_int('min_child_samples', 30, 300),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
            'reg_alpha': trial.suggest_float('reg_alpha', 1.0, 30.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1.0, 30.0, log=True),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.3, 0.9),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 0.9),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
        }
        
        _, _, valid_roi, test_roi = train_v15_improved(train_df, valid_df, test_df, feature_cols, params)
        gap = valid_roi - test_roi
        penalty = max(0, gap - 25) * 0.4
        score = valid_roi - penalty
        
        trial.set_user_attr('valid_roi', valid_roi)
        trial.set_user_attr('test_roi', test_roi)
        trial.set_user_attr('gap', gap)
        return score
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return study.best_params, study.best_trial.user_attrs


def optuna_optimize_v44(train_df, valid_df, test_df, feature_cols, n_trials=50):
    def objective(trial):
        params = {
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100)),
            'max_depth': trial.suggest_int('max_depth', 2, 7),
            'num_leaves': trial.suggest_int('num_leaves', 10, 60),
            'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'reg_alpha': trial.suggest_float('reg_alpha', 2.0, 40.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 2.0, 40.0, log=True),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.3, 0.8),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 0.9),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
        }
        
        _, _, valid_roi, test_roi = train_v44_improved(
            train_df.copy(), valid_df.copy(), test_df.copy(), feature_cols, params)
        gap = valid_roi - test_roi
        penalty = max(0, gap - 30) * 0.5
        score = valid_roi - penalty
        
        trial.set_user_attr('valid_roi', valid_roi)
        trial.set_user_attr('test_roi', test_roi)
        trial.set_user_attr('gap', gap)
        return score
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return study.best_params, study.best_trial.user_attrs


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("V15/V4.4 改善版v2 - 追加特徴量 + Optuna50トライアル")
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
    
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年] Train: ～{train_end}, Valid: {valid_start}～{valid_end}, Test: {test_start}～{test_end}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}件, Valid: {len(valid):,}件, Test: {len(test):,}件")
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        print("  拡張特徴量v2を追加中...")
        train_f = add_enhanced_features_v2(train_f, races_raw, course_master)
        valid_f = add_enhanced_features_v2(valid_f, races_raw, course_master)
        test_f = add_enhanced_features_v2(test_f, races_raw, course_master)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        new_features = [
            'month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
            'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
            'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
            'jockey_horse_pair_win_rate', 'jockey_horse_pair_count',
            'distance_change', 'weight_change_from_prev',
        ]
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f', 'weight_change', 'basis_weight', 'distance', 'passing_order']:
                new_features.append(f'prev_{i}_{suffix}')
        
        new_features = [f for f in new_features if f in train_f.columns]
        all_features = list(dict.fromkeys(base_features + new_features))
        
        print(f"  特徴量数: {len(base_features)} (基本) + {len(new_features)} (追加) = {len(all_features)}")
        
        results = {'year': year}
        
        print("\n  [V15 Binary + Optuna 50トライアル]")
        best_params_v15, best_attrs_v15 = optuna_optimize_v15(train_f, valid_f, test_f, all_features, n_trials=50)
        print(f"    Best Valid ROI: {best_attrs_v15['valid_roi']:.1f}%")
        print(f"    Best Test ROI: {best_attrs_v15['test_roi']:.1f}%")
        print(f"    Gap: {best_attrs_v15['gap']:.1f}%")
        
        results['v15_valid'] = best_attrs_v15['valid_roi']
        results['v15_test'] = best_attrs_v15['test_roi']
        results['v15_gap'] = best_attrs_v15['gap']
        
        print("\n  [V4.4 Ultra Safe + Optuna 50トライアル]")
        best_params_v44, best_attrs_v44 = optuna_optimize_v44(
            train_f.copy(), valid_f.copy(), test_f.copy(), all_features, n_trials=50)
        print(f"    Best Valid ROI: {best_attrs_v44['valid_roi']:.1f}%")
        print(f"    Best Test ROI: {best_attrs_v44['test_roi']:.1f}%")
        print(f"    Gap: {best_attrs_v44['gap']:.1f}%")
        
        results['v44_valid'] = best_attrs_v44['valid_roi']
        results['v44_test'] = best_attrs_v44['test_roi']
        results['v44_gap'] = best_attrs_v44['gap']
        
        all_results.append(results)
    
    print("\n" + "=" * 80)
    print("検証結果サマリー（Test ROI）- 改善版v2")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15 Binary':>12} {'V4.4 Ultra':>12}")
    print("-" * 40)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v15_test']:>11.1f}% {r['v44_test']:>11.1f}%")
    
    avg_v15 = np.mean([r['v15_test'] for r in all_results])
    avg_v44 = np.mean([r['v44_test'] for r in all_results])
    
    print("-" * 40)
    print(f"{'平均':>6} {avg_v15:>11.1f}% {avg_v44:>11.1f}%")
    
    print("\n" + "=" * 80)
    print("過学習チェック（Valid-Test Gap）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15 Binary':>12} {'V4.4 Ultra':>12}")
    print("-" * 40)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v15_gap']:>11.1f}% {r['v44_gap']:>11.1f}%")
    
    avg_gap_v15 = np.mean([r['v15_gap'] for r in all_results])
    avg_gap_v44 = np.mean([r['v44_gap'] for r in all_results])
    
    print("-" * 40)
    print(f"{'平均':>6} {avg_gap_v15:>11.1f}% {avg_gap_v44:>11.1f}%")
    
    print("\n" + "=" * 80)
    print("比較: 改善版v1 vs v2")
    print("=" * 80)
    print(f"\n  V15 Binary: v1=76.1% → v2={avg_v15:.1f}%")
    print(f"  V4.4 Ultra: v1=77.7% → v2={avg_v44:.1f}%")
    print(f"\n  V15 Gap: v1=5.9% → v2={avg_gap_v15:.1f}%")
    print(f"  V4.4 Gap: v1=3.8% → v2={avg_gap_v44:.1f}%")


if __name__ == "__main__":
    main()
