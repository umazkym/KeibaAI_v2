# -*- coding: utf-8 -*-
"""
V17改善モデル - 弱点分析に基づく改善

改善点:
1. 多頭数補正特徴量
2. 復帰馬評価（前走4-5着からの復帰）
3. 展開依存特徴量の強化
4. 穴馬（6-10番人気）の評価強化
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
    
    # === 基本特徴量 ===
    df['month'] = df['race_date'].dt.month
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    # 過去着順
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
    
    # === 改善1: 多頭数補正 ===
    # 16頭以上のレースでは予測精度が下がる → 頭数による難易度を特徴量化
    if 'field_size' in df.columns:
        df['is_large_field'] = (df['field_size'] >= 16).astype(int)
        df['field_size_normalized'] = df['field_size'] / 18  # 18頭で正規化
        df['field_difficulty'] = np.log1p(df['field_size'])  # 対数スケール
    
    # === 改善2: 復帰馬評価 ===
    # 前走4-5着からの復帰馬はROIが高い
    if 'prev_1_finish' in df.columns:
        prev_1 = df['prev_1_finish'].fillna(99)
        # 前走4-5着フラグ
        df['is_recovery_candidate'] = ((prev_1 >= 4) & (prev_1 <= 5)).astype(int)
        
        # 前走との着順変化トレンド
        if 'prev_2_finish' in df.columns:
            prev_2 = df['prev_2_finish'].fillna(99)
            df['finish_trend'] = prev_2 - prev_1  # 正=上昇傾向
            df['is_improving'] = (df['finish_trend'] > 0).astype(int)
        
        # 直近3走の最高着順
        finish_cols = [c for c in ['prev_1_finish', 'prev_2_finish', 'prev_3_finish'] if c in df.columns]
        if len(finish_cols) > 0:
            df['best_recent_finish'] = df[finish_cols].min(axis=1)
            df['worst_recent_finish'] = df[finish_cols].max(axis=1)
            df['finish_variance'] = df[finish_cols].std(axis=1).fillna(0)  # 成績の安定度
    
    # === 改善3: 展開依存特徴量 ===
    # 脚質と枠の相性をより詳細に
    if 'horse_number' in df.columns and 'field_size' in df.columns:
        # 相対的な枠位置
        df['relative_post'] = df['horse_number'] / df['field_size']
        df['is_inner'] = (df['relative_post'] <= 0.33).astype(int)
        df['is_outer'] = (df['relative_post'] >= 0.67).astype(int)
    
    # 逃げ馬の競争度（同一レース内での逃げ馬数）
    if 'horse_running_style' in df.columns:
        def count_front_runners(group):
            n_front = (group['horse_running_style'] == 1).sum()
            group['n_front_runners'] = n_front
            return group
        df = df.groupby('race_id', group_keys=False).apply(count_front_runners)
        df['front_runner_competition'] = df['n_front_runners'] / df['field_size']
    
    # === 改善4: 穴馬評価強化 ===
    # 6-10番人気の馬はROIが高い → 期待値の観点で評価
    if 'popularity' in df.columns:
        df['is_mid_popularity'] = ((df['popularity'] >= 6) & (df['popularity'] <= 10)).astype(int)
        df['popularity_band'] = pd.cut(df['popularity'], 
                                       bins=[0, 3, 5, 10, 99], 
                                       labels=[1, 2, 3, 4]).astype(float)
    
    # オッズと人気の乖離（期待値の手がかり）
    if 'win_odds' in df.columns and 'popularity' in df.columns:
        # オッズから逆算した期待人気
        df['expected_popularity'] = df.groupby('race_id')['win_odds'].rank(method='min')
        df['popularity_odds_gap'] = df['popularity'] - df['expected_popularity']
    
    return df


def train_model(train_df, valid_df, feature_cols):
    """V17モデル学習"""
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 80, 'reg_alpha': 4.0, 'reg_lambda': 6.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
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


def calc_roi_detailed(df, preds, max_odds=60):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bets = d[d['rank_pred'] == 1]
    n_bets = len(bets)
    
    hits = bets[bets['finish_position'] == 1]
    hits_filtered = hits[hits['win_odds'] <= max_odds]
    
    roi_all = hits['win_odds'].sum() / n_bets * 100 if n_bets > 0 else 0
    roi_filtered = hits_filtered['win_odds'].sum() / n_bets * 100 if n_bets > 0 else 0
    hit_rate = len(hits) / n_bets * 100 if n_bets > 0 else 0
    
    return roi_all, roi_filtered, hit_rate, n_bets


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("V17改善モデル - 弱点分析に基づく改善")
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
    
    all_results_v15 = []
    all_results_v17 = []
    
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
        
        # V15用の特徴量
        v15_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # V17用の改善特徴量を追加
        train_v17_df = add_v17_features(train_f, races_raw, course_master)
        valid_v17_df = add_v17_features(valid_f, races_raw, course_master)
        test_v17_df = add_v17_features(test_f, races_raw, course_master)
        
        # V17の特徴量リスト
        new_features = [
            'is_large_field', 'field_size_normalized', 'field_difficulty',
            'is_recovery_candidate', 'finish_trend', 'is_improving',
            'best_recent_finish', 'worst_recent_finish', 'finish_variance',
            'relative_post', 'is_inner', 'is_outer',
            'front_runner_competition', 'is_mid_popularity', 'popularity_band',
            'popularity_odds_gap', 'month_sin', 'month_cos',
            'course_slope_percent', 'course_final_straight_m',
        ]
        for i in range(1, 6):
            new_features.append(f'prev_{i}_finish')
        
        v17_features = list(dict.fromkeys(v15_features + [f for f in new_features if f in train_v17_df.columns]))
        
        print(f"  V15特徴量: {len(v15_features)}, V17特徴量: {len(v17_features)}")
        
        # V15モデル学習
        print("  V15学習中...")
        model_v15 = train_model(train_f, valid_f, v15_features)
        preds_v15 = model_v15.predict(test_f[v15_features].fillna(0))
        
        # V17モデル学習
        print("  V17学習中...")
        model_v17 = train_model(train_v17_df, valid_v17_df, v17_features)
        preds_v17 = model_v17.predict(test_v17_df[v17_features].fillna(0))
        
        # ROI計算
        roi_v15, roi_v15_60, hr_v15, n_bets = calc_roi_detailed(test_f, preds_v15)
        roi_v17, roi_v17_60, hr_v17, _ = calc_roi_detailed(test_v17_df, preds_v17)
        
        print(f"\n  【V15】 ROI: {roi_v15:.1f}% (60倍以下: {roi_v15_60:.1f}%), 的中率: {hr_v15:.1f}%")
        print(f"  【V17】 ROI: {roi_v17:.1f}% (60倍以下: {roi_v17_60:.1f}%), 的中率: {hr_v17:.1f}%")
        
        diff = roi_v17 - roi_v15
        print(f"  → 差分: {diff:+.1f}%")
        
        all_results_v15.append({'year': year, 'roi': roi_v15, 'roi_60': roi_v15_60, 'hit_rate': hr_v15})
        all_results_v17.append({'year': year, 'roi': roi_v17, 'roi_60': roi_v17_60, 'hit_rate': hr_v17})
        
        # V17の新特徴量の重要度
        if year == '2024':
            importance = pd.DataFrame({
                'feature': v17_features,
                'importance': model_v17.feature_importance(importance_type='gain')
            }).sort_values('importance', ascending=False)
            
            print("\n  【V17 新特徴量の重要度】")
            for feat in new_features[:10]:
                if feat in importance['feature'].values:
                    imp = importance[importance['feature'] == feat]['importance'].values[0]
                    print(f"    {feat}: {imp:.0f}")
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間検証サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15 ROI':>10} {'V17 ROI':>10} {'差分':>8}")
    print("-" * 40)
    
    for v15, v17 in zip(all_results_v15, all_results_v17):
        diff = v17['roi'] - v15['roi']
        print(f"{v15['year']:>6} {v15['roi']:>9.1f}% {v17['roi']:>9.1f}% {diff:>+7.1f}%")
    
    avg_v15 = np.mean([r['roi'] for r in all_results_v15])
    avg_v17 = np.mean([r['roi'] for r in all_results_v17])
    avg_diff = avg_v17 - avg_v15
    
    print("-" * 40)
    print(f"{'平均':>6} {avg_v15:>9.1f}% {avg_v17:>9.1f}% {avg_diff:>+7.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_diff > 0:
        print(f"\n  ✅ V17改善効果: +{avg_diff:.1f}%")
    else:
        print(f"\n  ❌ V17改善効果なし: {avg_diff:.1f}%")


if __name__ == "__main__":
    main()
