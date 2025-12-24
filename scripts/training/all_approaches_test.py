# -*- coding: utf-8 -*-
"""
全アプローチ検証 - XGBoost/CatBoost、スタッキング、クラス別モデル

【検証項目】
1. XGBoost/CatBoost追加アンサンブル
2. スタッキング（2段階学習）
3. クラス別モデル（芝/ダート、距離帯別）
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

# XGBoost/CatBoostのインポート
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("Warning: XGBoost not installed")

try:
    import catboost as cb
    HAS_CB = True
except ImportError:
    HAS_CB = False
    print("Warning: CatBoost not installed")


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
        return 0
    return hits['win_odds'].sum() / len(bet_df) * 100


def train_lgb(train_df, valid_df, feature_cols):
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


def train_xgb(train_df, valid_df, feature_cols):
    if not HAS_XGB:
        return None
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dvalid = xgb.DMatrix(X_valid, label=y_valid)
    
    params = {
        'objective': 'binary:logistic', 'eval_metric': 'auc',
        'max_depth': 3, 'learning_rate': 0.03, 'subsample': 0.6,
        'colsample_bytree': 0.6, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'verbosity': 0
    }
    model = xgb.train(params, dtrain, num_boost_round=500,
                      evals=[(dvalid, 'valid')],
                      early_stopping_rounds=20, verbose_eval=False)
    return model


def train_cb(train_df, valid_df, feature_cols):
    if not HAS_CB:
        return None
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    model = cb.CatBoostClassifier(
        iterations=500, learning_rate=0.03, depth=3,
        l2_leaf_reg=8.0, subsample=0.6, early_stopping_rounds=20,
        verbose=False, random_state=42
    )
    model.fit(X_train, y_train, eval_set=(X_valid, y_valid), verbose=False)
    return model


def predict(model, df, feature_cols, model_type='lgb'):
    X = df[feature_cols].fillna(0)
    if model_type == 'lgb':
        return model.predict(X)
    elif model_type == 'xgb':
        dtest = xgb.DMatrix(X)
        return model.predict(dtest)
    elif model_type == 'cb':
        return model.predict_proba(X)[:, 1]
    return np.zeros(len(df))


def normalize(preds):
    return (preds - preds.min()) / (preds.max() - preds.min() + 1e-8)


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("全アプローチ検証 - XGBoost/CatBoost、スタッキング、クラス別モデル")
    print("=" * 80)
    print(f"XGBoost: {'✅' if HAS_XGB else '❌'}, CatBoost: {'✅' if HAS_CB else '❌'}")
    
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
    ]
    
    results = {}
    
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
        
        print(f"  特徴量数: {len(all_features)}")
        
        # === 1. 各モデル学習 ===
        print("\n  [1] 各モデル学習...")
        model_lgb = train_lgb(train_f, valid_f, all_features)
        preds_lgb = predict(model_lgb, test_f, all_features, 'lgb')
        roi_lgb = calc_roi(test_f, preds_lgb)
        print(f"    LightGBM: {roi_lgb:.1f}%")
        
        preds_xgb = preds_lgb  # fallback
        if HAS_XGB:
            model_xgb = train_xgb(train_f, valid_f, all_features)
            preds_xgb = predict(model_xgb, test_f, all_features, 'xgb')
            roi_xgb = calc_roi(test_f, preds_xgb)
            print(f"    XGBoost:  {roi_xgb:.1f}%")
        
        preds_cb = preds_lgb  # fallback
        if HAS_CB:
            model_cb = train_cb(train_f, valid_f, all_features)
            preds_cb = predict(model_cb, test_f, all_features, 'cb')
            roi_cb = calc_roi(test_f, preds_cb)
            print(f"    CatBoost: {roi_cb:.1f}%")
        
        # === 2. アンサンブル ===
        print("\n  [2] アンサンブル...")
        
        # LGB + XGB + CB 平均
        preds_norm_lgb = normalize(preds_lgb)
        preds_norm_xgb = normalize(preds_xgb)
        preds_norm_cb = normalize(preds_cb)
        
        preds_all_avg = (preds_norm_lgb + preds_norm_xgb + preds_norm_cb) / 3
        roi_all_avg = calc_roi(test_f, preds_all_avg)
        print(f"    3モデル平均: {roi_all_avg:.1f}%")
        
        # === 3. スタッキング ===
        print("\n  [3] スタッキング...")
        
        # 1段目: Valid予測を取得
        preds_lgb_valid = predict(model_lgb, valid_f, all_features, 'lgb')
        valid_f['stack_lgb'] = preds_lgb_valid
        
        if HAS_XGB:
            preds_xgb_valid = predict(model_xgb, valid_f, all_features, 'xgb')
            valid_f['stack_xgb'] = preds_xgb_valid
        else:
            valid_f['stack_xgb'] = preds_lgb_valid
        
        if HAS_CB:
            preds_cb_valid = predict(model_cb, valid_f, all_features, 'cb')
            valid_f['stack_cb'] = preds_cb_valid
        else:
            valid_f['stack_cb'] = preds_lgb_valid
        
        # 2段目: スタッキングモデル学習
        stack_features = ['stack_lgb', 'stack_xgb', 'stack_cb']
        X_stack = valid_f[stack_features].fillna(0)
        y_stack = (valid_f['finish_position'] == 1).astype(int)
        
        stack_model = lgb.LGBMClassifier(n_estimators=100, max_depth=2, learning_rate=0.1, verbose=-1)
        stack_model.fit(X_stack, y_stack)
        
        # テストデータでスタッキング予測
        test_f['stack_lgb'] = preds_lgb
        test_f['stack_xgb'] = preds_xgb
        test_f['stack_cb'] = preds_cb
        preds_stack = stack_model.predict_proba(test_f[stack_features])[:, 1]
        roi_stack = calc_roi(test_f, preds_stack)
        print(f"    スタッキング: {roi_stack:.1f}%")
        
        # === 4. クラス別モデル ===
        print("\n  [4] クラス別モデル（芝/ダート）...")
        
        # 芝用モデル
        train_turf = train_f[train_f['track_surface'] == '芝'].copy()
        valid_turf = valid_f[valid_f['track_surface'] == '芝'].copy()
        test_turf = test_f[test_f['track_surface'] == '芝'].copy()
        
        # ダート用モデル
        train_dirt = train_f[train_f['track_surface'] == 'ダート'].copy()
        valid_dirt = valid_f[valid_f['track_surface'] == 'ダート'].copy()
        test_dirt = test_f[test_f['track_surface'] == 'ダート'].copy()
        
        if len(train_turf) > 1000 and len(test_turf) > 100:
            model_turf = train_lgb(train_turf, valid_turf, all_features)
            preds_turf = predict(model_turf, test_turf, all_features, 'lgb')
            roi_turf = calc_roi(test_turf, preds_turf)
            print(f"    芝モデル:   {roi_turf:.1f}% (n={len(test_turf):,})")
        else:
            roi_turf = 0
        
        if len(train_dirt) > 1000 and len(test_dirt) > 100:
            model_dirt = train_lgb(train_dirt, valid_dirt, all_features)
            preds_dirt = predict(model_dirt, test_dirt, all_features, 'lgb')
            roi_dirt = calc_roi(test_dirt, preds_dirt)
            print(f"    ダートモデル: {roi_dirt:.1f}% (n={len(test_dirt):,})")
        else:
            roi_dirt = 0
        
        # クラス別予測を結合
        test_f['class_pred'] = 0.0
        if 'preds_turf' in dir() and len(test_turf) > 0:
            test_f.loc[test_f['track_surface'] == '芝', 'class_pred'] = preds_turf
        if 'preds_dirt' in dir() and len(test_dirt) > 0:
            test_f.loc[test_f['track_surface'] == 'ダート', 'class_pred'] = preds_dirt
        
        roi_class = calc_roi(test_f, test_f['class_pred'])
        print(f"    クラス別結合: {roi_class:.1f}%")
        
        results[year] = {
            'lgb': roi_lgb,
            'xgb': calc_roi(test_f, preds_xgb) if HAS_XGB else roi_lgb,
            'cb': calc_roi(test_f, preds_cb) if HAS_CB else roi_lgb,
            'all_avg': roi_all_avg,
            'stack': roi_stack,
            'class': roi_class,
        }
    
    # サマリー
    print("\n" + "=" * 80)
    print("検証結果サマリー（Test ROI）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'LGB':>8} {'XGB':>8} {'CB':>8} {'3モデル平均':>10} {'スタッキング':>10} {'クラス別':>8}")
    print("-" * 70)
    
    for year, r in results.items():
        print(f"{year:>6} {r['lgb']:>7.1f}% {r['xgb']:>7.1f}% {r['cb']:>7.1f}% {r['all_avg']:>9.1f}% {r['stack']:>9.1f}% {r['class']:>7.1f}%")
    
    # 平均
    avg_lgb = np.mean([r['lgb'] for r in results.values()])
    avg_xgb = np.mean([r['xgb'] for r in results.values()])
    avg_cb = np.mean([r['cb'] for r in results.values()])
    avg_all = np.mean([r['all_avg'] for r in results.values()])
    avg_stack = np.mean([r['stack'] for r in results.values()])
    avg_class = np.mean([r['class'] for r in results.values()])
    
    print("-" * 70)
    print(f"{'平均':>6} {avg_lgb:>7.1f}% {avg_xgb:>7.1f}% {avg_cb:>7.1f}% {avg_all:>9.1f}% {avg_stack:>9.1f}% {avg_class:>7.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best = max([
        ('LightGBM', avg_lgb),
        ('XGBoost', avg_xgb),
        ('CatBoost', avg_cb),
        ('3モデル平均', avg_all),
        ('スタッキング', avg_stack),
        ('クラス別', avg_class),
    ], key=lambda x: x[1])
    
    print(f"\n  最高ROI: {best[0]} ({best[1]:.1f}%)")
    print(f"  ベースライン（平均アンサンブル）: 79.3%")
    
    if best[1] > 79.3:
        print(f"  → {best[0]}が改善！ (+{best[1] - 79.3:.1f}%)")
    else:
        print(f"  → 平均アンサンブルが最適")


if __name__ == "__main__":
    main()
