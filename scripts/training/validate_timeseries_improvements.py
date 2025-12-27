# -*- coding: utf-8 -*-
"""
改修4: 時系列的な改善検証

【検証内容】
1. より長期のトレンド特徴量
   - 過去10走、20走の累積統計
   - 長期的な調子の変化

2. 季節性の考慮
   - 月別Sin/Cos変換（既存）
   - 季節別カテゴリ（春夏秋冬）
   - 開催時期による影響

【リーク対策】
- 全て過去データのみ使用（shift(1)厳守）
- race_dateより前のデータのみで計算
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


def add_long_term_features(df, races_raw):
    """
    長期トレンド特徴量を追加
    
    【追加特徴量】
    - horse_last10_avg_finish: 過去10走の平均着順
    - horse_last20_avg_finish: 過去20走の平均着順
    - horse_form_trend_10: 直近5走と直近10走の差（調子の変化）
    - jockey_last10_win_rate: 騎手の直近10走勝率
    """
    df = df.copy()
    
    if races_raw is None:
        return df
    
    # ソートして履歴計算
    races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
    
    # 馬の長期統計
    races_sorted['horse_cum_finish_sum'] = races_sorted.groupby('horse_id')['finish_position'].cumsum().shift(1)
    races_sorted['horse_cum_race_count'] = races_sorted.groupby('horse_id').cumcount()
    
    # 過去10走の平均（ローリング）
    races_sorted['horse_last10_finish_sum'] = races_sorted.groupby('horse_id')['finish_position'].transform(
        lambda x: x.rolling(window=10, min_periods=5).sum().shift(1))
    races_sorted['horse_last10_count'] = races_sorted.groupby('horse_id')['finish_position'].transform(
        lambda x: x.rolling(window=10, min_periods=5).count().shift(1))
    races_sorted['horse_last10_avg_finish'] = races_sorted['horse_last10_finish_sum'] / races_sorted['horse_last10_count']
    
    # 過去20走の平均
    races_sorted['horse_last20_finish_sum'] = races_sorted.groupby('horse_id')['finish_position'].transform(
        lambda x: x.rolling(window=20, min_periods=10).sum().shift(1))
    races_sorted['horse_last20_count'] = races_sorted.groupby('horse_id')['finish_position'].transform(
        lambda x: x.rolling(window=20, min_periods=10).count().shift(1))
    races_sorted['horse_last20_avg_finish'] = races_sorted['horse_last20_finish_sum'] / races_sorted['horse_last20_count']
    
    # 調子の変化（直近5走 - 直近10走：負なら調子上昇）
    if 'horse_last3_avg_finish' in df.columns:
        # 外部で計算済みの場合はそれを使用
        pass
    else:
        races_sorted['horse_last5_finish_sum'] = races_sorted.groupby('horse_id')['finish_position'].transform(
            lambda x: x.rolling(window=5, min_periods=3).sum().shift(1))
        races_sorted['horse_last5_count'] = races_sorted.groupby('horse_id')['finish_position'].transform(
            lambda x: x.rolling(window=5, min_periods=3).count().shift(1))
        races_sorted['horse_last5_avg_finish'] = races_sorted['horse_last5_finish_sum'] / races_sorted['horse_last5_count']
    
    # フォームトレンド（直近5走と直近10走の差）
    races_sorted['horse_form_trend'] = races_sorted.get('horse_last5_avg_finish', races_sorted['horse_last10_avg_finish']) - races_sorted['horse_last10_avg_finish']
    
    # 騎手の長期統計
    jockey_sorted = races_raw.sort_values(['jockey_id', 'race_date']).copy()
    jockey_sorted['is_win'] = (jockey_sorted['finish_position'] == 1).astype(int)
    jockey_sorted['jockey_last10_win_sum'] = jockey_sorted.groupby('jockey_id')['is_win'].transform(
        lambda x: x.rolling(window=10, min_periods=5).sum().shift(1))
    jockey_sorted['jockey_last10_count'] = jockey_sorted.groupby('jockey_id')['is_win'].transform(
        lambda x: x.rolling(window=10, min_periods=5).count().shift(1))
    jockey_sorted['jockey_last10_win_rate'] = jockey_sorted['jockey_last10_win_sum'] / jockey_sorted['jockey_last10_count']
    
    # マージ
    horse_features = races_sorted[['race_id', 'horse_id', 'horse_last10_avg_finish', 'horse_last20_avg_finish', 'horse_form_trend']].drop_duplicates(['race_id', 'horse_id'])
    jockey_features = jockey_sorted[['race_id', 'jockey_id', 'jockey_last10_win_rate']].drop_duplicates(['race_id', 'jockey_id'])
    
    df['race_id'] = df['race_id'].astype(str)
    df['horse_id'] = df['horse_id'].astype(str)
    horse_features['race_id'] = horse_features['race_id'].astype(str)
    horse_features['horse_id'] = horse_features['horse_id'].astype(str)
    
    df = df.merge(horse_features, on=['race_id', 'horse_id'], how='left', suffixes=('', '_new'))
    
    if 'jockey_id' in df.columns:
        df['jockey_id'] = df['jockey_id'].astype(str)
        jockey_features['jockey_id'] = jockey_features['jockey_id'].astype(str)
        df = df.merge(jockey_features, on=['race_id', 'jockey_id'], how='left', suffixes=('', '_new'))
    
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


def add_seasonality_features(df):
    """
    季節性特徴量を追加
    
    【追加特徴量】
    - season: 季節カテゴリ（1=春、2=夏、3=秋、4=冬）
    - is_g1_season: G1シーズンフラグ（春秋）
    - month_sin/cos: 月のサイン/コサイン変換（周期性）
    - day_of_year_sin/cos: 年間日数の周期変換
    """
    df = df.copy()
    
    if 'race_date' not in df.columns:
        return df
    
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['month'] = df['race_date'].dt.month
    df['day_of_year'] = df['race_date'].dt.dayofyear
    
    # 季節カテゴリ
    df['season'] = pd.cut(df['month'], 
                          bins=[0, 3, 6, 9, 12], 
                          labels=[4, 1, 2, 3])  # 冬、春、夏、秋
    df['season'] = df['season'].astype(float)
    
    # G1シーズン（春：4-6月、秋：10-12月）
    df['is_g1_season'] = ((df['month'] >= 4) & (df['month'] <= 6) | 
                          (df['month'] >= 10) & (df['month'] <= 12)).astype(float)
    
    # 月の周期性
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    # 年間日数の周期性
    df['day_of_year_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
    df['day_of_year_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
    
    # 週番号
    df['week_of_year'] = df['race_date'].dt.isocalendar().week.astype(int)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)
    
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
    print("改修4: 時系列的な改善検証")
    print("=" * 80)
    print("\n【検証内容】")
    print("  1. 長期トレンド特徴量（過去10/20走統計）")
    print("  2. 季節性特徴量（季節カテゴリ、G1シーズン、周期変換）")
    
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
        
        # ===== A. ベースライン =====
        model_a = train_model(train_base, valid_base, base_features, get_v15_params())
        roi_a, hit_a, gap_a = evaluate_model(model_a, train_base, test_base, base_features)
        
        # ===== B. 長期トレンド特徴量追加 =====
        train_b = add_long_term_features(train_base, races_raw)
        valid_b = add_long_term_features(valid_base, races_raw)
        test_b = add_long_term_features(test_base, races_raw)
        
        long_term_features = ['horse_last10_avg_finish', 'horse_last20_avg_finish', 
                              'horse_form_trend', 'jockey_last10_win_rate']
        long_term_features = [f for f in long_term_features if f in train_b.columns]
        features_b = base_features + long_term_features
        
        model_b = train_model(train_b, valid_b, features_b, get_v15_params())
        roi_b, hit_b, gap_b = evaluate_model(model_b, train_b, test_b, features_b)
        
        # ===== C. 季節性特徴量追加 =====
        train_c = add_seasonality_features(train_base)
        valid_c = add_seasonality_features(valid_base)
        test_c = add_seasonality_features(test_base)
        
        seasonality_features = ['season', 'is_g1_season', 'month_sin', 'month_cos',
                                'day_of_year_sin', 'day_of_year_cos', 'week_sin', 'week_cos']
        seasonality_features = [f for f in seasonality_features if f in train_c.columns]
        features_c = base_features + seasonality_features
        
        model_c = train_model(train_c, valid_c, features_c, get_v15_params())
        roi_c, hit_c, gap_c = evaluate_model(model_c, train_c, test_c, features_c)
        
        # ===== D. 長期+季節性（両方） =====
        train_d = add_long_term_features(train_base, races_raw)
        train_d = add_seasonality_features(train_d)
        valid_d = add_long_term_features(valid_base, races_raw)
        valid_d = add_seasonality_features(valid_d)
        test_d = add_long_term_features(test_base, races_raw)
        test_d = add_seasonality_features(test_d)
        
        features_d = base_features + long_term_features + seasonality_features
        features_d = list(dict.fromkeys(features_d))  # 重複除去
        
        model_d = train_model(train_d, valid_d, features_d, get_v15_params())
        roi_d, hit_d, gap_d = evaluate_model(model_d, train_d, test_d, features_d)
        
        print(f"\n  結果:")
        print(f"    ┌───────────────────────┬──────────┬──────────┐")
        print(f"    │       パターン         │ Test ROI │   Gap    │")
        print(f"    ├───────────────────────┼──────────┼──────────┤")
        print(f"    │ A. V15ベースライン     │  {roi_a:>6.1f}% │  {gap_a:>6.1f}% │")
        print(f"    │ B. +長期トレンド       │  {roi_b:>6.1f}% │  {gap_b:>6.1f}% │")
        print(f"    │ C. +季節性            │  {roi_c:>6.1f}% │  {gap_c:>6.1f}% │")
        print(f"    │ D. +長期+季節性        │  {roi_d:>6.1f}% │  {gap_d:>6.1f}% │")
        print(f"    └───────────────────────┴──────────┴──────────┘")
        
        all_results.append({
            'year': year,
            'A': roi_a, 'B': roi_b, 'C': roi_c, 'D': roi_d,
            'gap_A': gap_a, 'gap_B': gap_b, 'gap_C': gap_c, 'gap_D': gap_d,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    for key in ['A', 'B', 'C', 'D']:
        avg_roi = np.mean([r[key] for r in all_results])
        avg_gap = np.mean([r[f'gap_{key}'] for r in all_results])
        labels = {
            'A': 'V15ベースライン',
            'B': '+長期トレンド',
            'C': '+季節性',
            'D': '+長期+季節性',
        }
        diff = avg_roi - np.mean([r['A'] for r in all_results])
        print(f"  {labels[key]:<18}: ROI {avg_roi:>6.1f}% ({diff:+.1f}%), Gap {avg_gap:>5.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    avg_A = np.mean([r['A'] for r in all_results])
    best_key = 'A'
    best_roi = avg_A
    
    for key in ['B', 'C', 'D']:
        avg = np.mean([r[key] for r in all_results])
        if avg > best_roi:
            best_roi = avg
            best_key = key
    
    labels = {'A': 'ベースライン', 'B': '長期トレンド', 'C': '季節性', 'D': '長期+季節性'}
    if best_key == 'A':
        print(f"\n  → ベースライン(V15)が最良。時系列改修の効果なし。")
    else:
        diff = best_roi - avg_A
        print(f"\n  ✅ {labels[best_key]}が最良 (ROI +{diff:.1f}%)")


if __name__ == "__main__":
    main()
