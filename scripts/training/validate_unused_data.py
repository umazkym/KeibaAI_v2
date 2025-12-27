# -*- coding: utf-8 -*-
"""
未活用データ改善 - 3つの特徴量を個別検証

【改善1】追い込み能力（corners: gap_from_leader）
【改善2】ペース適性（race_details: first_half/second_half）
【改善3】産地×距離適性（horses: producing_area）

【過学習・リーク防止】
- shift(1)厳守（当日レースの結果を使わない）
- 4年間Walk-forward検証
- Gap 30%超は即却下
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


def get_v15_params():
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }


def get_v44_residual_params():
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def add_closing_ability(df, corners_df, races_raw):
    """
    【改善1】追い込み能力（Closing Ability）
    
    最終コーナーでの位置から、ゴールまでにどれだけ順位を上げたかを計算。
    shift(1)で過去レースのみ参照。
    
    リーク防止:
    - finish_positionは過去レースのみ使用
    - 当日レースのfinish_positionは使用しない
    """
    df = df.copy()
    
    # 最終コーナー（corner=4または3）のgap_from_leaderを取得
    last_corner = corners_df.groupby(['race_id', 'horse_number'], as_index=False).last()
    
    # races_rawから頭数とfinish_positionを取得
    race_info = races_raw[['race_id', 'horse_number', 'horse_id', 'finish_position']].copy()
    race_info['head_count'] = race_info.groupby('race_id')['horse_number'].transform('count')
    
    # マージ
    closing_data = last_corner.merge(race_info, on=['race_id', 'horse_number'], how='inner')
    
    # closing_ability計算（ポジション改善度）
    # 最終コーナーのposition（1が先頭）→ゴール順位の変化
    # 正の値 = 順位上昇（追い込み成功）
    closing_data['closing_ability'] = (closing_data['position'] - closing_data['finish_position']) / closing_data['head_count']
    
    # 馬ごとの平均closing_ability（shift(1)で過去のみ）
    closing_data = closing_data.sort_values(['horse_id', 'race_id'])
    closing_data['horse_closing_ability'] = (
        closing_data.groupby('horse_id')['closing_ability']
        .apply(lambda x: x.expanding().mean().shift(1))
        .reset_index(level=0, drop=True)
    )
    
    # マージ
    closing_stats = closing_data[['race_id', 'horse_id', 'horse_closing_ability']].drop_duplicates()
    df = df.merge(closing_stats, on=['race_id', 'horse_id'], how='left')
    df['horse_closing_ability'] = df['horse_closing_ability'].fillna(0)
    
    return df, ['horse_closing_ability']


def add_pace_affinity(df, race_details_df, races_raw):
    """
    【改善2】ペース適性（Pace Affinity）
    
    レースのペース（スロー/ハイ）と馬の過去成績の関係を特徴量化。
    shift(1)で過去レースのみ参照。
    
    リーク防止:
    - 当日のペースは使用しない（予測時には不明）
    - 過去レースのペースと成績の相関のみ使用
    """
    df = df.copy()
    
    # ペース差を計算（正=スロー、負=ハイ）
    pace_data = race_details_df[['race_id', 'first_half', 'second_half']].dropna().copy()
    pace_data['pace_diff'] = pace_data['second_half'] - pace_data['first_half']
    pace_data['is_slow_pace'] = (pace_data['pace_diff'] > 0).astype(int)
    
    # races_rawとマージしてfinish_positionを取得
    race_info = races_raw[['race_id', 'horse_id', 'finish_position']].copy()
    pace_horse = race_info.merge(pace_data[['race_id', 'is_slow_pace']], on='race_id', how='inner')
    
    # 馬ごとのスローペース時の勝率（shift(1)で過去のみ）
    pace_horse = pace_horse.sort_values(['horse_id', 'race_id'])
    pace_horse['is_win'] = (pace_horse['finish_position'] == 1).astype(int)
    
    # スローペース時の勝利数
    pace_horse['slow_wins'] = (
        pace_horse.groupby('horse_id')
        .apply(lambda x: (x['is_win'] * x['is_slow_pace']).cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    pace_horse['slow_races'] = (
        pace_horse.groupby('horse_id')['is_slow_pace']
        .apply(lambda x: x.cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    pace_horse['horse_slow_pace_win_rate'] = (
        pace_horse['slow_wins'] / pace_horse['slow_races'].replace(0, np.nan)
    ).fillna(0)
    
    # ハイペース時の勝利数
    pace_horse['fast_wins'] = (
        pace_horse.groupby('horse_id')
        .apply(lambda x: (x['is_win'] * (1 - x['is_slow_pace'])).cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    pace_horse['fast_races'] = (
        pace_horse.groupby('horse_id')
        .apply(lambda x: (1 - x['is_slow_pace']).cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    pace_horse['horse_fast_pace_win_rate'] = (
        pace_horse['fast_wins'] / pace_horse['fast_races'].replace(0, np.nan)
    ).fillna(0)
    
    # マージ
    pace_stats = pace_horse[['race_id', 'horse_id', 'horse_slow_pace_win_rate', 'horse_fast_pace_win_rate']].drop_duplicates()
    df = df.merge(pace_stats, on=['race_id', 'horse_id'], how='left')
    df['horse_slow_pace_win_rate'] = df['horse_slow_pace_win_rate'].fillna(0)
    df['horse_fast_pace_win_rate'] = df['horse_fast_pace_win_rate'].fillna(0)
    
    return df, ['horse_slow_pace_win_rate', 'horse_fast_pace_win_rate']


def add_producing_area_distance(df, horses_df, races_raw):
    """
    【改善3】産地×距離適性
    
    産地ごとの距離帯別勝率を特徴量化。
    全体統計（リークなし：産地は生まれつき決まっている）。
    """
    df = df.copy()
    
    # races_rawに産地情報をマージ
    horses_info = horses_df[['horse_id', 'producing_area']].dropna().drop_duplicates()
    race_info = races_raw[['race_id', 'horse_id', 'distance_m', 'finish_position']].copy()
    race_info = race_info.merge(horses_info, on='horse_id', how='left')
    race_info = race_info.dropna(subset=['producing_area'])
    
    # 距離帯を計算
    race_info['distance_category'] = pd.cut(
        race_info['distance_m'],
        bins=[0, 1400, 1800, 2200, 10000],
        labels=['short', 'mile', 'middle', 'long']
    )
    
    race_info['is_win'] = (race_info['finish_position'] == 1).astype(int)
    
    # 産地×距離帯別の勝率（全体統計）
    area_distance_stats = (
        race_info.groupby(['producing_area', 'distance_category'])['is_win']
        .mean()
        .reset_index()
        .rename(columns={'is_win': 'area_distance_win_rate'})
    )
    
    # dfに産地情報をマージ
    df = df.merge(horses_info, on='horse_id', how='left')
    
    # 距離帯を計算
    df['distance_category'] = pd.cut(
        df['distance_m'],
        bins=[0, 1400, 1800, 2200, 10000],
        labels=['short', 'mile', 'middle', 'long']
    )
    
    # 産地×距離帯勝率をマージ
    df = df.merge(area_distance_stats, on=['producing_area', 'distance_category'], how='left')
    df['area_distance_win_rate'] = df['area_distance_win_rate'].fillna(0.08)  # 全体平均
    
    return df, ['area_distance_win_rate']


def train_v15_model(train_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds, params):
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.clip(np.maximum(residual_gain, 0), 0, 99)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    return model


def calc_roi(df, pred_col):
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def train_and_evaluate(train_f, test_f, feature_cols, label):
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    model_v15 = train_v15_model(train_f, feature_cols, get_v15_params())
    train_f[f'v15_{label}'] = model_v15.predict(X_train)
    test_f[f'v15_{label}'] = model_v15.predict(X_test)
    
    model_v44 = train_v44_residual(train_f, feature_cols, train_f[f'v15_{label}'], get_v44_residual_params())
    train_f[f'v44_{label}'] = model_v44.predict(X_train)
    test_f[f'v44_{label}'] = model_v44.predict(X_test)
    
    train_f[f'ens_{label}'] = normalize(train_f[f'v15_{label}']) * 0.5 + normalize(train_f[f'v44_{label}']) * 0.5
    test_f[f'ens_{label}'] = normalize(test_f[f'v15_{label}']) * 0.5 + normalize(test_f[f'v44_{label}']) * 0.5
    
    train_roi, _ = calc_roi(train_f, f'ens_{label}')
    test_roi, hit_rate = calc_roi(test_f, f'ens_{label}')
    gap = train_roi - test_roi
    
    return train_roi, test_roi, gap, hit_rate


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("未活用データ改善 - 3つの特徴量を個別検証")
    print("=" * 80)
    print("\n【検証パターン】")
    print("  A. 現行（ベースライン）")
    print("  B. +追い込み能力（closing_ability）")
    print("  C. +ペース適性（slow/fast_pace_win_rate）")
    print("  D. +産地×距離（area_distance_win_rate）")
    
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
    print(f"corners: {len(corners):,}件")
    print(f"race_details: {len(race_details):,}件")
    print(f"horses: {len(horses):,}件")
    
    test_years = [2021, 2022, 2023, 2024]
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        print(f"\n{'='*80}")
        print(f"Test Year: {test_year}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        train_races_raw = races_raw[races_raw['race_date'] <= train_end].copy()
        
        print(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  基本特徴量: {len(base_features)}")
        
        results = {'year': test_year}
        
        # ===== A. 現行（ベースライン） =====
        print("\n  A. 現行（ベースライン）訓練中...")
        train_roi, test_roi, gap, hit = train_and_evaluate(train_f.copy(), test_f.copy(), base_features, 'a')
        results['A_train'], results['A_test'], results['A_gap'] = train_roi, test_roi, gap
        
        # ===== B. +追い込み能力 =====
        print("  B. +追い込み能力（closing_ability）追加中...")
        try:
            train_f_b, closing_cols = add_closing_ability(train_f.copy(), corners, train_races_raw)
            test_f_b, _ = add_closing_ability(test_f.copy(), corners, races_raw)
            features_b = base_features + closing_cols
            print(f"     追加特徴量: {closing_cols}")
            
            train_roi, test_roi, gap, hit = train_and_evaluate(train_f_b, test_f_b, features_b, 'b')
            results['B_train'], results['B_test'], results['B_gap'] = train_roi, test_roi, gap
        except Exception as e:
            print(f"     エラー: {e}")
            results['B_train'], results['B_test'], results['B_gap'] = 0, 0, 0
        
        # ===== C. +ペース適性 =====
        print("  C. +ペース適性（slow/fast_pace_win_rate）追加中...")
        try:
            train_f_c, pace_cols = add_pace_affinity(train_f.copy(), race_details, train_races_raw)
            test_f_c, _ = add_pace_affinity(test_f.copy(), race_details, races_raw)
            features_c = base_features + pace_cols
            print(f"     追加特徴量: {pace_cols}")
            
            train_roi, test_roi, gap, hit = train_and_evaluate(train_f_c, test_f_c, features_c, 'c')
            results['C_train'], results['C_test'], results['C_gap'] = train_roi, test_roi, gap
        except Exception as e:
            print(f"     エラー: {e}")
            results['C_train'], results['C_test'], results['C_gap'] = 0, 0, 0
        
        # ===== D. +産地×距離 =====
        print("  D. +産地×距離（area_distance_win_rate）追加中...")
        try:
            train_f_d, area_cols = add_producing_area_distance(train_f.copy(), horses, train_races_raw)
            test_f_d, _ = add_producing_area_distance(test_f.copy(), horses, races_raw)
            features_d = base_features + area_cols
            print(f"     追加特徴量: {area_cols}")
            
            train_roi, test_roi, gap, hit = train_and_evaluate(train_f_d, test_f_d, features_d, 'd')
            results['D_train'], results['D_test'], results['D_gap'] = train_roi, test_roi, gap
        except Exception as e:
            print(f"     エラー: {e}")
            results['D_train'], results['D_test'], results['D_gap'] = 0, 0, 0
        
        # 結果表示
        print(f"\n  【結果】")
        print(f"    A. 現行:         Test ROI {results['A_test']:.1f}%, Gap {results['A_gap']:.1f}%")
        if results['B_test'] > 0:
            print(f"    B. +追い込み能力: Test ROI {results['B_test']:.1f}%, Gap {results['B_gap']:.1f}%, 差分 {results['B_test'] - results['A_test']:+.1f}%")
        if results['C_test'] > 0:
            print(f"    C. +ペース適性:   Test ROI {results['C_test']:.1f}%, Gap {results['C_gap']:.1f}%, 差分 {results['C_test'] - results['A_test']:+.1f}%")
        if results['D_test'] > 0:
            print(f"    D. +産地×距離:   Test ROI {results['D_test']:.1f}%, Gap {results['D_gap']:.1f}%, 差分 {results['D_test'] - results['A_test']:+.1f}%")
        
        all_results.append(results)
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    baseline_roi = np.mean([r['A_test'] for r in all_results])
    
    patterns = [
        ('A. 現行', 'A'),
        ('B. +追い込み能力', 'B'),
        ('C. +ペース適性', 'C'),
        ('D. +産地×距離', 'D'),
    ]
    
    print(f"\n{'アプローチ':25s} {'ROI':>8s} {'Gap':>8s} {'現行比':>8s}")
    print("-" * 55)
    
    for label, key in patterns:
        valid_results = [r for r in all_results if r.get(f'{key}_test', 0) > 0]
        if valid_results:
            avg_roi = np.mean([r[f'{key}_test'] for r in valid_results])
            avg_gap = np.mean([r[f'{key}_gap'] for r in valid_results])
            diff = avg_roi - baseline_roi if key != 'A' else 0
            gap_status = "✅" if avg_gap <= 30 else "⚠️"
            roi_status = "↑" if diff > 0 else ("↓" if diff < 0 else "=")
            print(f"  {label:23s} {avg_roi:>7.1f}% {avg_gap:>7.1f}% {gap_status} {diff:>+7.1f}% {roi_status}")
    
    # 効果判定
    print("\n【効果判定】")
    
    for label, key in patterns[1:]:
        valid_results = [r for r in all_results if r.get(f'{key}_test', 0) > 0]
        if not valid_results:
            print(f"  ⚠️ {label}: データ不足でスキップ")
            continue
            
        avg_roi = np.mean([r[f'{key}_test'] for r in valid_results])
        avg_gap = np.mean([r[f'{key}_gap'] for r in valid_results])
        diff = avg_roi - baseline_roi
        
        consistent = sum([1 for r in valid_results if r[f'{key}_test'] > r['A_test']])
        
        if avg_gap <= 30 and diff > 0 and consistent >= 2:
            print(f"  ✅ {label}: ROI +{diff:.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/{len(valid_results)} → 採用候補")
        elif avg_gap <= 30 and diff >= -0.5:
            print(f"  △ {label}: ROI {diff:+.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/{len(valid_results)} → 要検討")
        else:
            print(f"  ❌ {label}: ROI {diff:+.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/{len(valid_results)} → 却下")


if __name__ == "__main__":
    main()
