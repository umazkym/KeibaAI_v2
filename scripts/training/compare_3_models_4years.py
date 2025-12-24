# -*- coding: utf-8 -*-
"""
3モデル4年間検証スクリプト

【検証対象】
1. V15 Binary Classification (LeakFreeFeatureEngineerV15)
2. V4.4 Optuna (LambdaRank, 155特徴量)
3. V4.4 Ultra Safe (LambdaRank, リーク完全排除)

【検証期間】
2022年, 2023年, 2024年, 2025年の4年間

【評価指標】
- Test ROI（フィルタリングなし、予測1位に単勝100円）
- 的中率
- Train-Test Gap
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import json
import yaml
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


# ===============================
# V4.4系で使うリーク特徴量リスト
# ===============================
LEAKY_FEATURES_V44 = [
    'form_rank', 'gap_course_fit_popularity', 'gap_jockey_popularity',
    'gap_pedigree_popularity', 'gap_trainer_popularity', 'is_overvalued',
]

LEAKY_FEATURES_ULTRASAFE = LEAKY_FEATURES_V44 + [
    'turn_match', 'turn_preference', 'horse_left_turn_perf', 
    'horse_right_turn_perf', 'is_left_turn',
]


def load_course_master():
    yaml_path = project_root / "keibaai/configs/course_master.yaml"
    if yaml_path.exists():
        with open(yaml_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}


def get_course_features(course_master, venue, surface, distance_m):
    if not course_master or venue not in course_master:
        return 0, 300, 0
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    try:
        dist = int(distance_m)
    except:
        return 0, 300, 0
    distance_data = surface_data.get(dist, {})
    if not distance_data:
        return 0, 300, 0
    if 'default' in distance_data:
        info = distance_data['default']
    else:
        first_key = list(distance_data.keys())[0] if distance_data else None
        if first_key and isinstance(distance_data[first_key], dict):
            info = distance_data[first_key]
        else:
            info = distance_data
    return info.get('slope_percent', 0), info.get('final_straight_m', 300), info.get('corner_count', 0)


def calc_roi(df, preds):
    """ROI計算"""
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate, len(bet_df)


def train_v15_binary(train_df, test_df, feature_cols):
    """V15 Binary Classification"""
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_test = test_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    train_df_copy = train_df.copy()
    test_df_copy = test_df.copy()
    
    train_roi, _, _ = calc_roi(train_df_copy.assign(score=train_preds), train_preds)
    test_roi, test_hit, n = calc_roi(test_df_copy.assign(score=test_preds), test_preds)
    
    return train_roi, test_roi, test_hit, n


def train_v44_optuna(train_df, test_df, feature_cols):
    """V4.4 Optuna (LambdaRank)"""
    # ターゲット: オッズ加重ゲイン
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_df))
    gain[train_df['finish_position'] == 1] = log_odds[train_df['finish_position'] == 1] * weight_1st
    gain[train_df['finish_position'] == 2] = log_odds[train_df['finish_position'] == 2] * weight_2nd
    gain[train_df['finish_position'] == 3] = log_odds[train_df['finish_position'] == 3] * weight_3rd
    train_df = train_df.copy()
    train_df['target_relevance'] = gain.astype(int)
    train_df['sample_weight'] = np.log1p(train_df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # Optunaで最適化されたパラメータ（保存済みモデルから）
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 39,
        'max_depth': 6,
        'min_child_samples': 141,
        'learning_rate': 0.097,
        'reg_alpha': 1.7,
        'reg_lambda': 3.6,
        'feature_fraction': 0.61,
        'bagging_fraction': 0.60,
        'bagging_freq': 6,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_test = test_df[feature_cols].fillna(0)
    
    groups = train_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(
        X_train, train_df['target_relevance'],
        group=groups, sample_weight=train_df['sample_weight'],
    )
    
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    train_roi, _, _ = calc_roi(train_df, train_preds)
    test_roi, test_hit, n = calc_roi(test_df, test_preds)
    
    return train_roi, test_roi, test_hit, n


def train_v44_ultrasafe(train_df, test_df, feature_cols):
    """V4.4 Ultra Safe (LambdaRank, より強い正則化)"""
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_df))
    gain[train_df['finish_position'] == 1] = log_odds[train_df['finish_position'] == 1] * weight_1st
    gain[train_df['finish_position'] == 2] = log_odds[train_df['finish_position'] == 2] * weight_2nd
    gain[train_df['finish_position'] == 3] = log_odds[train_df['finish_position'] == 3] * weight_3rd
    train_df = train_df.copy()
    train_df['target_relevance'] = gain.astype(int)
    train_df['sample_weight'] = np.log1p(train_df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # Ultra Safe: より強い正則化
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'num_leaves': 35,
        'max_depth': 5,
        'min_child_samples': 150,
        'learning_rate': 0.05,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'feature_fraction': 0.6,
        'bagging_fraction': 0.7,
        'bagging_freq': 5,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_test = test_df[feature_cols].fillna(0)
    
    groups = train_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(
        X_train, train_df['target_relevance'],
        group=groups, sample_weight=train_df['sample_weight'],
    )
    
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    train_roi, _, _ = calc_roi(train_df, train_preds)
    test_roi, test_hit, n = calc_roi(test_df, test_preds)
    
    return train_roi, test_roi, test_hit, n


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("3モデル4年間検証（V15, V4.4 Optuna, V4.4 Ultra Safe）")
    print("=" * 80)
    
    # データ読み込み
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # V4.4用特徴量データ
    v43_model_dir = project_root / 'keibaai/models/mu_v4_3'
    with open(v43_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        all_v43_features = json.load(f)
    
    print(f"\n総データ: {len(races):,}件")
    
    # Test期間定義
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年] Train: ～{train_end}, Test: {test_start}～{test_end}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}件, Test: {len(test):,}件")
        
        results = {'year': year}
        
        # === V15 Binary ===
        print("\n  [Model 1: V15 Binary]")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols_v15 = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"    特徴量数: {len(feature_cols_v15)}")
        
        train_roi, test_roi, test_hit, n = train_v15_binary(train_f, test_f, feature_cols_v15)
        gap = train_roi - test_roi
        print(f"    Train ROI: {train_roi:.1f}%, Test ROI: {test_roi:.1f}%, Gap: {gap:.1f}%")
        
        results['v15_train'] = train_roi
        results['v15_test'] = test_roi
        results['v15_gap'] = gap
        
        # === V4.4 Optuna ===
        print("\n  [Model 2: V4.4 Optuna]")
        
        # V4.4用特徴量を準備（V15から基本情報をコピー、V4.3特徴量を追加）
        safe_features_optuna = [f for f in all_v43_features if f not in LEAKY_FEATURES_V44]
        available_features_optuna = [f for f in safe_features_optuna if f in train_f.columns]
        
        # コース特徴量を追加
        course_master = load_course_master()
        for df in [train_f, test_f]:
            cf = df.apply(
                lambda row: get_course_features(
                    course_master, row.get('venue'), row.get('track_surface'), row.get('distance_m')
                ), axis=1
            )
            df['course_slope_percent'] = [x[0] for x in cf]
            df['course_final_straight_m'] = [x[1] for x in cf]
            df['course_corner_count'] = [x[2] for x in cf]
        
        course_feature_list = ['course_slope_percent', 'course_final_straight_m', 'course_corner_count']
        available_features_optuna = available_features_optuna + [f for f in course_feature_list if f in train_f.columns]
        
        print(f"    特徴量数: {len(available_features_optuna)}")
        
        train_roi2, test_roi2, test_hit2, n2 = train_v44_optuna(train_f, test_f, available_features_optuna)
        gap2 = train_roi2 - test_roi2
        print(f"    Train ROI: {train_roi2:.1f}%, Test ROI: {test_roi2:.1f}%, Gap: {gap2:.1f}%")
        
        results['optuna_train'] = train_roi2
        results['optuna_test'] = test_roi2
        results['optuna_gap'] = gap2
        
        # === V4.4 Ultra Safe ===
        print("\n  [Model 3: V4.4 Ultra Safe]")
        
        safe_features_ultrasafe = [f for f in all_v43_features if f not in LEAKY_FEATURES_ULTRASAFE]
        available_features_ultrasafe = [f for f in safe_features_ultrasafe if f in train_f.columns]
        available_features_ultrasafe = available_features_ultrasafe + [f for f in course_feature_list if f in train_f.columns]
        
        print(f"    特徴量数: {len(available_features_ultrasafe)}")
        
        train_roi3, test_roi3, test_hit3, n3 = train_v44_ultrasafe(train_f, test_f, available_features_ultrasafe)
        gap3 = train_roi3 - test_roi3
        print(f"    Train ROI: {train_roi3:.1f}%, Test ROI: {test_roi3:.1f}%, Gap: {gap3:.1f}%")
        
        results['ultrasafe_train'] = train_roi3
        results['ultrasafe_test'] = test_roi3
        results['ultrasafe_gap'] = gap3
        
        all_results.append(results)
    
    # === サマリー ===
    print("\n" + "=" * 80)
    print("検証結果サマリー（Test ROI）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15 Binary':>12} {'V4.4 Optuna':>12} {'V4.4 Ultra':>12}")
    print("-" * 50)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v15_test']:>11.1f}% {r['optuna_test']:>11.1f}% {r['ultrasafe_test']:>11.1f}%")
    
    avg_v15 = np.mean([r['v15_test'] for r in all_results])
    avg_optuna = np.mean([r['optuna_test'] for r in all_results])
    avg_ultrasafe = np.mean([r['ultrasafe_test'] for r in all_results])
    
    print("-" * 50)
    print(f"{'平均':>6} {avg_v15:>11.1f}% {avg_optuna:>11.1f}% {avg_ultrasafe:>11.1f}%")
    
    # Gap サマリー
    print("\n" + "=" * 80)
    print("過学習チェック（Train-Test Gap）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15 Binary':>12} {'V4.4 Optuna':>12} {'V4.4 Ultra':>12}")
    print("-" * 50)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['v15_gap']:>11.1f}% {r['optuna_gap']:>11.1f}% {r['ultrasafe_gap']:>11.1f}%")
    
    avg_gap_v15 = np.mean([r['v15_gap'] for r in all_results])
    avg_gap_optuna = np.mean([r['optuna_gap'] for r in all_results])
    avg_gap_ultrasafe = np.mean([r['ultrasafe_gap'] for r in all_results])
    
    print("-" * 50)
    print(f"{'平均':>6} {avg_gap_v15:>11.1f}% {avg_gap_optuna:>11.1f}% {avg_gap_ultrasafe:>11.1f}%")
    
    # 結論
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best_model = max(
        [('V15 Binary', avg_v15), ('V4.4 Optuna', avg_optuna), ('V4.4 Ultra Safe', avg_ultrasafe)],
        key=lambda x: x[1]
    )
    
    print(f"\n  最高平均ROI: {best_model[0]} ({best_model[1]:.1f}%)")


if __name__ == "__main__":
    main()
