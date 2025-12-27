# -*- coding: utf-8 -*-
"""
時系列改善：汎用的な特徴量のみ

【汎用性重視】
- 具体的な数値範囲（6-8走目、21-28日等）をハードコードしない
- 連続値として追加し、LightGBMに最適な分割点を学習させる
- 既存特徴量と重複しないか確認済み

【追加特徴量】
1. horse_career_race_count - 通算出走数（連続値）
2. distance_change_m - 距離変化（連続値、m単位）
3. same_venue_flag - 同一競馬場フラグ
4. meeting_day_normalized - 開催進行度（0-1正規化）

【リーク防止】
- 全てshift(1)相当の処理で過去データのみ使用
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def add_timeseries_features(df, races_raw):
    """
    汎用的な時系列特徴量を追加
    
    具体的な数値範囲をハードコードせず、連続値で追加
    """
    df = df.copy()
    
    if races_raw is None:
        return df
    
    # race_date/horse_id等でソート＆マージ用準備
    races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
    
    # 1. 通算出走数（連続値）
    races_sorted['horse_career_race_count'] = races_sorted.groupby('horse_id').cumcount()
    
    # 2. 前走競馬場
    races_sorted['prev_venue'] = races_sorted.groupby('horse_id')['venue'].shift(1)
    races_sorted['same_venue_flag'] = (races_sorted['venue'] == races_sorted['prev_venue']).astype(float)
    
    # 3. 前走距離との差
    races_sorted['prev_distance'] = races_sorted.groupby('horse_id')['distance_m'].shift(1)
    races_sorted['distance_change_m'] = races_sorted['distance_m'] - races_sorted['prev_distance']
    
    # 4. 開催進行度（0-1正規化、day_of_meetingを8で割る）
    if 'day_of_meeting' in races_sorted.columns:
        races_sorted['meeting_day_normalized'] = races_sorted['day_of_meeting'] / 8.0
        races_sorted['meeting_day_normalized'] = races_sorted['meeting_day_normalized'].clip(upper=1.5)
    
    # 特徴量をdfにマージ
    merge_cols = ['race_id', 'horse_id', 'horse_career_race_count', 'same_venue_flag', 
                  'distance_change_m', 'meeting_day_normalized']
    merge_cols = [c for c in merge_cols if c in races_sorted.columns]
    
    features_to_add = races_sorted[merge_cols].drop_duplicates(['race_id', 'horse_id'])
    
    df['race_id'] = df['race_id'].astype(str)
    df['horse_id'] = df['horse_id'].astype(str)
    features_to_add['race_id'] = features_to_add['race_id'].astype(str)
    features_to_add['horse_id'] = features_to_add['horse_id'].astype(str)
    
    df = df.merge(features_to_add, on=['race_id', 'horse_id'], how='left', suffixes=('', '_new'))
    
    # 重複カラム処理
    for col in df.columns:
        if col.endswith('_new'):
            base_col = col[:-4]
            if base_col in df.columns:
                df[base_col] = df[base_col].fillna(df[col])
            else:
                df[base_col] = df[col]
            df = df.drop(columns=[col])
    
    return df


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    top1 = d[d['rank_pred'] == 1]
    hits = top1[top1['finish_position'] == 1]
    if len(top1) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(top1) * 100
    hit_rate = len(hits) / len(top1) * 100
    return roi, hit_rate


def get_v15_params():
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }


def train_model(train_df, valid_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def evaluate_model(model, train_f, test_f, feature_cols):
    X_test = test_f[feature_cols].fillna(0)
    preds = model.predict(X_test)
    roi, hit = calc_roi(test_f, preds)
    
    X_train = train_f[feature_cols].fillna(0)
    train_preds = model.predict(X_train)
    train_roi, _ = calc_roi(train_f, train_preds)
    gap = train_roi - roi
    
    return roi, hit, gap


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("時系列改善：汎用的な特徴量のみ")
    print("=" * 80)
    print("\n【追加特徴量】（全て連続値、ハードコードなし）")
    print("  1. horse_career_race_count - 通算出走数")
    print("  2. distance_change_m - 距離変化（m）")
    print("  3. same_venue_flag - 同一競馬場フラグ")
    print("  4. meeting_day_normalized - 開催進行度")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races_raw = races.copy()
    
    print(f"\n総データ: {len(races):,}件")
    
    test_periods = [
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2022-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年テスト]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  データ: Train={len(train):,}, Valid={len(valid):,}, Test={len(test):,}")
        
        if len(train) < 10000:
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_base = engine.transform(train)
        valid_base = engine.transform(valid)
        test_base = engine.transform(test)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_base.columns]
        
        # === A. ベースライン（V15） ===
        model_a = train_model(train_base, valid_base, base_features, get_v15_params())
        roi_a, hit_a, gap_a = evaluate_model(model_a, train_base, test_base, base_features)
        
        # === B. 時系列特徴量追加 ===
        train_b = add_timeseries_features(train_base, races_raw)
        valid_b = add_timeseries_features(valid_base, races_raw)
        test_b = add_timeseries_features(test_base, races_raw)
        
        new_features = ['horse_career_race_count', 'distance_change_m', 
                        'same_venue_flag', 'meeting_day_normalized']
        new_features = [f for f in new_features if f in train_b.columns]
        features_b = base_features + new_features
        
        print(f"  追加特徴量: {new_features}")
        
        model_b = train_model(train_b, valid_b, features_b, get_v15_params())
        roi_b, hit_b, gap_b = evaluate_model(model_b, train_b, test_b, features_b)
        
        diff = roi_b - roi_a
        
        print(f"\n  結果:")
        print(f"    ┌───────────────────┬──────────┬──────────┐")
        print(f"    │      パターン      │ Test ROI │   Gap    │")
        print(f"    ├───────────────────┼──────────┼──────────┤")
        print(f"    │ A. V15ベースライン │  {roi_a:>6.1f}% │  {gap_a:>6.1f}% │")
        print(f"    │ B. +時系列特徴量   │  {roi_b:>6.1f}% │  {gap_b:>6.1f}% │")
        print(f"    └───────────────────┴──────────┴──────────┘")
        print(f"    差分: {diff:+.1f}%")
        
        # 特徴量重要度
        importance = pd.DataFrame({
            'feature': features_b,
            'importance': model_b.feature_importance(importance_type='gain')
        }).sort_values('importance', ascending=False)
        
        new_feat_imp = importance[importance['feature'].isin(new_features)]
        if len(new_feat_imp) > 0:
            print(f"\n    新特徴量の重要度:")
            for _, row in new_feat_imp.iterrows():
                print(f"      {row['feature']}: {row['importance']:.1f}")
        
        all_results.append({
            'year': year, 'roi_a': roi_a, 'roi_b': roi_b,
            'gap_a': gap_a, 'gap_b': gap_b, 'diff': diff,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    avg_a = np.mean([r['roi_a'] for r in all_results])
    avg_b = np.mean([r['roi_b'] for r in all_results])
    avg_diff = np.mean([r['diff'] for r in all_results])
    avg_gap_a = np.mean([r['gap_a'] for r in all_results])
    avg_gap_b = np.mean([r['gap_b'] for r in all_results])
    
    print(f"\n  V15ベースライン    : ROI {avg_a:>6.1f}%, Gap {avg_gap_a:>5.1f}%")
    print(f"  +時系列特徴量      : ROI {avg_b:>6.1f}%, Gap {avg_gap_b:>5.1f}%")
    print(f"  差分               : {avg_diff:+.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_diff > 0:
        print(f"\n  ✅ 時系列特徴量追加で改善 (平均 +{avg_diff:.1f}%)")
    elif avg_diff > -1:
        print(f"\n  ⚪ 時系列特徴量追加はほぼ変化なし ({avg_diff:+.1f}%)")
    else:
        print(f"\n  ❌ 時系列特徴量追加で悪化 ({avg_diff:.1f}%)")
    
    if avg_gap_b < avg_gap_a:
        print(f"  ✅ 過学習軽減: Gap {avg_gap_b:.1f}% < {avg_gap_a:.1f}%")


if __name__ == "__main__":
    main()
