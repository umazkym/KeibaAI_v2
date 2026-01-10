#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V15 + V4.4 + V16 統合アンサンブルモデル

目標: ROI 81.9%維持 + 安定性向上（Gap削減）

【統合方法】
1. V15 Binary (安定ベース)
2. V4.4 LambdaRank (大穴枠)
3. V16新特徴量 (安定性向上用) ← 追加

【V16新特徴量】(6年Walk-forward検証済み)
- jockey_prev_winrate: 6/6年プラス, +11.3pt
- prev_3f_avg: 6/6年プラス, +7.6pt
- horse_prev_winrate: 5/6年プラス, +7.9pt
- trainer_prev_winrate: 補完
- is_late_race: 5/6年プラス, +4.1pt
- prev_finish: 5/6年プラス, +3.8pt
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


def add_v16_features(df):
    """V16新特徴量を追加（shift(1)でリーク防止）"""
    df = df.copy()
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # is_win作成
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # 1. 馬累積勝率
    df['_horse_cumwins'] = df.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_horse_cumraces'] = df.groupby('horse_id').cumcount()
    df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
    df.drop(columns=['_horse_cumwins', '_horse_cumraces'], inplace=True)
    
    # 2. 騎手累積勝率
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['_jockey_cumwins'] = df.groupby('jockey_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_jockey_cumraces'] = df.groupby('jockey_id').cumcount()
    df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
    df.drop(columns=['_jockey_cumwins', '_jockey_cumraces'], inplace=True)
    
    # 3. 調教師累積勝率
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['_trainer_cumwins'] = df.groupby('trainer_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_trainer_cumraces'] = df.groupby('trainer_id').cumcount()
    df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
    df.drop(columns=['_trainer_cumwins', '_trainer_cumraces'], inplace=True)
    
    # 4. 過去上がり3F累積平均
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    if 'last_3f_time' in df.columns:
        def calc_cum_avg_shifted(series):
            cumsum = series.fillna(0).cumsum()
            cumcount = series.notna().cumsum()
            avg = cumsum / cumcount.replace(0, np.nan)
            return avg.shift(1)
        df['prev_3f_avg'] = df.groupby('horse_id')['last_3f_time'].transform(calc_cum_avg_shifted)
    else:
        df['prev_3f_avg'] = np.nan
    
    # 5. R8-12レースフラグ
    df['_race_number'] = df['race_id'].astype(str).str[-2:].astype(int)
    df['is_late_race'] = ((df['_race_number'] >= 8) & (df['_race_number'] <= 12)).astype(int)
    df.drop(columns=['_race_number'], inplace=True)
    
    # 6. 前走着順
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    return df


def add_enhanced_features(df, races_raw, course_master):
    """既存の拡張特徴量"""
    df = df.copy()
    
    df['month'] = df['race_date'].dt.month
    df['week_of_year'] = df['race_date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['race_date'].dt.dayofweek
    
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    df['is_steep_slope'] = (df['course_slope_percent'] >= 1.5).astype(int)
    df['is_many_corners'] = (df['course_corner_count'] >= 4).astype(int)
    
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
    print("V15 + V4.4 + V16 統合アンサンブルモデル")
    print("目標: ROI 81.9%維持 + 安定性向上 (Gap削減)")
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
    
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    v44_params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    # V16新特徴量
    v16_features = [
        'horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate',
        'prev_3f_avg', 'is_late_race', 'prev_finish'
    ]
    
    test_periods = [
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2022-01-01'),
    ]
    
    results_baseline = []
    results_with_v16 = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # V15特徴量
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        # 拡張特徴量
        train_f = add_enhanced_features(train_f, races_raw, course_master)
        valid_f = add_enhanced_features(valid_f, races_raw, course_master)
        test_f = add_enhanced_features(test_f, races_raw, course_master)
        
        # V16新特徴量追加
        train_f = add_v16_features(train_f)
        valid_f = add_v16_features(valid_f)
        test_f = add_v16_features(test_f)
        
        # 特徴量リスト
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        enhanced_features = ['month_sin', 'month_cos', 'week_sin', 'week_cos',
                            'course_slope_percent', 'course_final_straight_m', 
                            'is_long_straight', 'is_steep_slope']
        enhanced_features = [f for f in enhanced_features if f in train_f.columns]
        
        # ベースライン特徴量（V15+V4.4オリジナル）
        baseline_features = list(dict.fromkeys(base_features + enhanced_features))
        
        # V16追加特徴量
        v16_available = [f for f in v16_features if f in train_f.columns]
        all_features = list(dict.fromkeys(baseline_features + v16_available))
        
        print(f"  ベースライン特徴量: {len(baseline_features)}個")
        print(f"  V16追加後: {len(all_features)}個 (+{len(v16_available)})")
        
        # ========== ベースライン（V15+V4.4） ==========
        print("  ベースライン学習中...")
        model_v15_base = train_v15(train_f, valid_f, baseline_features, v15_params)
        model_v44_base = train_v44(train_f.copy(), valid_f.copy(), baseline_features, v44_params)
        
        X_test_base = test_f[baseline_features].fillna(0)
        preds_v15_base = model_v15_base.predict(X_test_base)
        preds_v44_base = model_v44_base.predict(X_test_base)
        
        preds_v15_norm = (preds_v15_base - preds_v15_base.min()) / (preds_v15_base.max() - preds_v15_base.min() + 1e-8)
        preds_v44_norm = (preds_v44_base - preds_v44_base.min()) / (preds_v44_base.max() - preds_v44_base.min() + 1e-8)
        preds_ensemble_base = preds_v15_norm * 0.5 + preds_v44_norm * 0.5
        
        roi_base, hit_base = calc_roi(test_f, preds_ensemble_base)
        
        # Valid ROI（Gap計算用）
        X_valid_base = valid_f[baseline_features].fillna(0)
        preds_v15_valid = model_v15_base.predict(X_valid_base)
        preds_v44_valid = model_v44_base.predict(X_valid_base)
        preds_v15_valid_norm = (preds_v15_valid - preds_v15_valid.min()) / (preds_v15_valid.max() - preds_v15_valid.min() + 1e-8)
        preds_v44_valid_norm = (preds_v44_valid - preds_v44_valid.min()) / (preds_v44_valid.max() - preds_v44_valid.min() + 1e-8)
        preds_ensemble_valid = preds_v15_valid_norm * 0.5 + preds_v44_valid_norm * 0.5
        roi_valid_base, _ = calc_roi(valid_f, preds_ensemble_valid)
        gap_base = abs(roi_valid_base - roi_base)
        
        results_baseline.append({'year': year, 'valid_roi': roi_valid_base, 'test_roi': roi_base, 'gap': gap_base})
        
        # ========== V16追加版 ==========
        print("  V16追加版学習中...")
        model_v15_v16 = train_v15(train_f, valid_f, all_features, v15_params)
        model_v44_v16 = train_v44(train_f.copy(), valid_f.copy(), all_features, v44_params)
        
        X_test_v16 = test_f[all_features].fillna(0)
        preds_v15_v16 = model_v15_v16.predict(X_test_v16)
        preds_v44_v16 = model_v44_v16.predict(X_test_v16)
        
        preds_v15_v16_norm = (preds_v15_v16 - preds_v15_v16.min()) / (preds_v15_v16.max() - preds_v15_v16.min() + 1e-8)
        preds_v44_v16_norm = (preds_v44_v16 - preds_v44_v16.min()) / (preds_v44_v16.max() - preds_v44_v16.min() + 1e-8)
        preds_ensemble_v16 = preds_v15_v16_norm * 0.5 + preds_v44_v16_norm * 0.5
        
        roi_v16, hit_v16 = calc_roi(test_f, preds_ensemble_v16)
        
        # Valid ROI（Gap計算用）
        X_valid_v16 = valid_f[all_features].fillna(0)
        preds_v15_v16_valid = model_v15_v16.predict(X_valid_v16)
        preds_v44_v16_valid = model_v44_v16.predict(X_valid_v16)
        preds_v15_v16_valid_norm = (preds_v15_v16_valid - preds_v15_v16_valid.min()) / (preds_v15_v16_valid.max() - preds_v15_v16_valid.min() + 1e-8)
        preds_v44_v16_valid_norm = (preds_v44_v16_valid - preds_v44_v16_valid.min()) / (preds_v44_v16_valid.max() - preds_v44_v16_valid.min() + 1e-8)
        preds_ensemble_v16_valid = preds_v15_v16_valid_norm * 0.5 + preds_v44_v16_valid_norm * 0.5
        roi_valid_v16, _ = calc_roi(valid_f, preds_ensemble_v16_valid)
        gap_v16 = abs(roi_valid_v16 - roi_v16)
        
        results_with_v16.append({'year': year, 'valid_roi': roi_valid_v16, 'test_roi': roi_v16, 'gap': gap_v16})
        
        print(f"\n  結果:")
        print(f"    ベースライン V15+V4.4:  Test ROI {roi_base:.1f}%, Gap {gap_base:.1f}%")
        print(f"    V16追加版:              Test ROI {roi_v16:.1f}%, Gap {gap_v16:.1f}%")
        print(f"    差分:                   ROI {roi_v16 - roi_base:+.1f}pt, Gap {gap_v16 - gap_base:+.1f}pt")
    
    # サマリー
    print("\n" + "=" * 80)
    print("検証結果サマリー")
    print("=" * 80)
    
    avg_roi_base = np.mean([r['test_roi'] for r in results_baseline])
    avg_gap_base = np.mean([r['gap'] for r in results_baseline])
    std_roi_base = np.std([r['test_roi'] for r in results_baseline])
    
    avg_roi_v16 = np.mean([r['test_roi'] for r in results_with_v16])
    avg_gap_v16 = np.mean([r['gap'] for r in results_with_v16])
    std_roi_v16 = np.std([r['test_roi'] for r in results_with_v16])
    
    print(f"\n{'モデル':20} | {'平均ROI':>10} | {'σ':>6} | {'平均Gap':>10}")
    print("-" * 60)
    print(f"{'V15+V4.4 (ベースライン)':20} | {avg_roi_base:>8.1f}% | {std_roi_base:>5.1f}% | {avg_gap_base:>8.1f}%")
    print(f"{'V15+V4.4+V16':20} | {avg_roi_v16:>8.1f}% | {std_roi_v16:>5.1f}% | {avg_gap_v16:>8.1f}%")
    print("-" * 60)
    print(f"{'差分':20} | {avg_roi_v16 - avg_roi_base:>+7.1f}pt | {std_roi_v16 - std_roi_base:>+5.1f}% | {avg_gap_v16 - avg_gap_base:>+7.1f}pt")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_gap_v16 < avg_gap_base:
        print(f"\n✅ V16追加により安定性向上: Gap {avg_gap_base:.1f}% → {avg_gap_v16:.1f}% ({avg_gap_v16 - avg_gap_base:+.1f}pt)")
    else:
        print(f"\n⚠️ V16追加でGap変化: {avg_gap_base:.1f}% → {avg_gap_v16:.1f}%")
    
    if avg_roi_v16 >= avg_roi_base - 1:
        print(f"✅ ROI維持: {avg_roi_base:.1f}% → {avg_roi_v16:.1f}% ({avg_roi_v16 - avg_roi_base:+.1f}pt)")
    else:
        print(f"⚠️ ROI低下: {avg_roi_base:.1f}% → {avg_roi_v16:.1f}% ({avg_roi_v16 - avg_roi_base:+.1f}pt)")
    
    print("\n年度別詳細:")
    print(f"{'年':>6} | {'Base ROI':>10} | {'Base Gap':>10} | {'V16 ROI':>10} | {'V16 Gap':>10} | {'ROI差':>8} | {'Gap差':>8}")
    print("-" * 85)
    for base, v16 in zip(results_baseline, results_with_v16):
        roi_diff = v16['test_roi'] - base['test_roi']
        gap_diff = v16['gap'] - base['gap']
        print(f"{base['year']:>6} | {base['test_roi']:>8.1f}% | {base['gap']:>8.1f}% | {v16['test_roi']:>8.1f}% | {v16['gap']:>8.1f}% | {roi_diff:>+6.1f}pt | {gap_diff:>+6.1f}pt")


if __name__ == "__main__":
    main()
