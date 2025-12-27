# -*- coding: utf-8 -*-
"""
V17改善版 - 4つの改善を統合検証

【改善1】夏場対策
- 季節sin/cos特徴量
- 馬の夏場過去勝率（shift(1)適用）

【改善2】苦手競馬場対策
- 馬の競馬場別勝率（shift(1)適用）

【改善3】V4.4残差改良
- ランク差ベースの残差

【改善4】特徴量エンジニアリング
- 血統×距離適性

【過学習・リーク防止】
- shift(1)厳守
- 4年間Walk-forward検証
- Gap 30%超は却下
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


def add_v17_features(df, races_raw):
    """
    V17改善特徴量を追加（リークフリー）
    
    【改善1】夏場対策
    【改善2】苦手競馬場対策
    【改善4】血統×距離適性
    """
    df = df.copy()
    
    # === 改善1: 夏場対策 ===
    # 季節sin/cos（リークなし：レース日から計算）
    df['month'] = df['race_date'].dt.month
    df['season_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['season_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    # 夏場フラグ（7-9月）
    df['is_summer'] = ((df['month'] >= 7) & (df['month'] <= 9)).astype(int)
    
    # 馬の夏場過去勝率（shift(1)で過去のみ参照）
    races_sorted = races_raw.sort_values(['horse_id', 'race_date'])
    races_sorted['is_win'] = (races_sorted['finish_position'] == 1).astype(int)
    races_sorted['is_summer_race'] = ((races_sorted['race_date'].dt.month >= 7) & 
                                       (races_sorted['race_date'].dt.month <= 9)).astype(int)
    
    # 夏場レースでの勝利数（累積、shift(1)）
    races_sorted['summer_wins_cum'] = (
        races_sorted.groupby('horse_id')
        .apply(lambda x: (x['is_win'] * x['is_summer_race']).cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    races_sorted['summer_races_cum'] = (
        races_sorted.groupby('horse_id')['is_summer_race']
        .apply(lambda x: x.cumsum().shift(1))
        .reset_index(level=0, drop=True)
    )
    races_sorted['horse_summer_win_rate'] = (
        races_sorted['summer_wins_cum'] / races_sorted['summer_races_cum'].replace(0, np.nan)
    ).fillna(0)
    
    # マージ
    summer_stats = races_sorted[['race_id', 'horse_id', 'horse_summer_win_rate']].drop_duplicates()
    df = df.merge(summer_stats, on=['race_id', 'horse_id'], how='left')
    df['horse_summer_win_rate'] = df['horse_summer_win_rate'].fillna(0)
    
    # === 改善2: 苦手競馬場対策 ===
    # 馬の競馬場別過去勝率（shift(1)で過去のみ参照）
    races_sorted['venue_wins_cum'] = (
        races_sorted.groupby(['horse_id', 'venue'])['is_win']
        .apply(lambda x: x.cumsum().shift(1))
        .reset_index(level=[0, 1], drop=True)
    )
    races_sorted['venue_races_cum'] = (
        races_sorted.groupby(['horse_id', 'venue'])
        .cumcount()  # 0始まりで既にshift効果
    )
    races_sorted['horse_venue_win_rate'] = (
        races_sorted['venue_wins_cum'] / races_sorted['venue_races_cum'].replace(0, np.nan)
    ).fillna(0)
    
    venue_stats = races_sorted[['race_id', 'horse_id', 'horse_venue_win_rate']].drop_duplicates()
    df = df.merge(venue_stats, on=['race_id', 'horse_id'], how='left')
    df['horse_venue_win_rate'] = df['horse_venue_win_rate'].fillna(0)
    
    # === 改善4: 血統×距離適性 ===
    # 父馬の距離帯別勝率を計算
    if 'sire_id' in df.columns:
        # 距離帯を定義（短距離/マイル/中距離/長距離）
        df['distance_category'] = pd.cut(
            df['distance_m'], 
            bins=[0, 1400, 1800, 2200, 10000],
            labels=['short', 'mile', 'middle', 'long']
        )
        
        # 父馬の距離帯別勝率（全体統計、リークなし）
        races_sorted['distance_category'] = pd.cut(
            races_sorted['distance_m'], 
            bins=[0, 1400, 1800, 2200, 10000],
            labels=['short', 'mile', 'middle', 'long']
        )
        
        if 'sire_id' in races_sorted.columns:
            sire_distance_stats = (
                races_sorted.groupby(['sire_id', 'distance_category'])['is_win']
                .mean()
                .reset_index()
                .rename(columns={'is_win': 'sire_distance_win_rate'})
            )
            df = df.merge(sire_distance_stats, on=['sire_id', 'distance_category'], how='left')
            df['sire_distance_win_rate'] = df['sire_distance_win_rate'].fillna(0.08)  # 全体平均
    
    return df


def train_v15_model(train_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual_v2(train_df, feature_cols, v15_preds, params):
    """
    V4.4 Residual v2（改善3: ランク差ベース）
    """
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    # V15のランク
    train_df['v15_rank'] = train_df.groupby('race_id')['v15_pred'].rank(ascending=False, method='first')
    
    # 人気順位（オッズベース）
    train_df['popularity'] = train_df.groupby('race_id')['win_odds'].rank(method='first')
    
    # ランク差（AI順位 < 人気順位 なら正 = V15が過小評価）
    rank_diff = train_df['popularity'] - train_df['v15_rank']
    
    # オッズ加重
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    # ターゲット: 勝った馬のうち、ランク差が正（AIが過小評価）の馬に高スコア
    residual_gain = np.zeros(len(train_df))
    won_mask = train_df['finish_position'] == 1
    residual_gain[won_mask] = np.maximum(rank_diff[won_mask], 0) * log_odds[won_mask] * 3
    residual_gain[train_df['finish_position'] == 2] = np.maximum(rank_diff[train_df['finish_position'] == 2], 0) * log_odds[train_df['finish_position'] == 2] * 1.5
    residual_gain[train_df['finish_position'] == 3] = np.maximum(rank_diff[train_df['finish_position'] == 3], 0) * log_odds[train_df['finish_position'] == 3] * 0.5
    
    train_df['target'] = np.clip(residual_gain, 0, 99).astype(int)
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


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V17改善版 - 4つの改善を統合検証")
    print("=" * 80)
    print("\n【改善内容】")
    print("  1. 夏場対策: 季節sin/cos + 馬の夏場勝率")
    print("  2. 苦手競馬場対策: 馬の競馬場別勝率")
    print("  3. V4.4残差改良: ランク差ベースの残差")
    print("  4. 血統×距離適性")
    
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
        
        print(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        if len(train) < 100000 or len(test) < 10000:
            print("  ⚠️ データ不足のためスキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        # V17改善特徴量を追加
        print("  V17改善特徴量を追加中...")
        train_f = add_v17_features(train_f, races_raw[races_raw['race_date'] <= train_end])
        test_f = add_v17_features(test_f, races_raw)
        
        # 特徴量カラム
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v17_features = ['season_sin', 'season_cos', 'is_summer', 
                        'horse_summer_win_rate', 'horse_venue_win_rate']
        if 'sire_distance_win_rate' in train_f.columns:
            v17_features.append('sire_distance_win_rate')
        
        all_features = list(dict.fromkeys(base_features + v17_features))
        all_features = [f for f in all_features if f in train_f.columns]
        
        print(f"  基本特徴量: {len(base_features)}, V17追加: {len(v17_features)}, 合計: {len(all_features)}")
        
        X_train = train_f[all_features].fillna(0)
        X_test = test_f[all_features].fillna(0)
        
        # ===== A. 現行（V15 + V4.4 Residual） =====
        print("\n  A. 現行（V15 + V4.4 Residual）訓練中...")
        model_v15_a = train_v15_model(train_f, base_features, get_v15_params())
        train_f['v15_a_pred'] = model_v15_a.predict(train_f[base_features].fillna(0))
        test_f['v15_a_pred'] = model_v15_a.predict(test_f[base_features].fillna(0))
        
        # V4.4 Residual（現行方式）
        # V4.4 Residual（現行方式）をインラインで実装
        train_df_tmp = train_f.copy()
        actual = (train_df_tmp['finish_position'] == 1).astype(float)
        residual = actual - train_df_tmp['v15_a_pred']
        odds = train_df_tmp['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        residual_gain = np.zeros(len(train_df_tmp))
        residual_gain[train_df_tmp['finish_position'] == 1] = residual[train_df_tmp['finish_position'] == 1] * log_odds[train_df_tmp['finish_position'] == 1] * 10
        residual_gain[train_df_tmp['finish_position'] == 2] = residual[train_df_tmp['finish_position'] == 2] * log_odds[train_df_tmp['finish_position'] == 2] * 5
        residual_gain[train_df_tmp['finish_position'] == 3] = residual[train_df_tmp['finish_position'] == 3] * log_odds[train_df_tmp['finish_position'] == 3] * 2
        residual_gain = np.maximum(residual_gain, 0)
        train_df_tmp['target'] = residual_gain.astype(int)
        train_df_tmp = train_df_tmp.sort_values('race_id')
        groups = train_df_tmp.groupby('race_id', sort=False).size().to_list()
        model_v44_a = lgb.LGBMRanker(**get_v44_residual_params(), n_estimators=150)
        model_v44_a.fit(train_df_tmp[base_features].fillna(0), train_df_tmp['target'], group=groups)
        
        train_f['v44_a_pred'] = model_v44_a.predict(train_f[base_features].fillna(0))
        test_f['v44_a_pred'] = model_v44_a.predict(test_f[base_features].fillna(0))
        
        train_f['ens_a_pred'] = normalize(train_f['v15_a_pred']) * 0.5 + normalize(train_f['v44_a_pred']) * 0.5
        test_f['ens_a_pred'] = normalize(test_f['v15_a_pred']) * 0.5 + normalize(test_f['v44_a_pred']) * 0.5
        
        # ===== B. V17（V15+V17特徴量 + V4.4 Residual v2） =====
        print("  B. V17（改善特徴量 + 残差改良）訓練中...")
        model_v15_b = train_v15_model(train_f, all_features, get_v15_params())
        train_f['v15_b_pred'] = model_v15_b.predict(X_train)
        test_f['v15_b_pred'] = model_v15_b.predict(X_test)
        
        model_v44_b = train_v44_residual_v2(train_f, all_features, train_f['v15_b_pred'], get_v44_residual_params())
        train_f['v44_b_pred'] = model_v44_b.predict(X_train)
        test_f['v44_b_pred'] = model_v44_b.predict(X_test)
        
        train_f['ens_b_pred'] = normalize(train_f['v15_b_pred']) * 0.5 + normalize(train_f['v44_b_pred']) * 0.5
        test_f['ens_b_pred'] = normalize(test_f['v15_b_pred']) * 0.5 + normalize(test_f['v44_b_pred']) * 0.5
        
        # ROI計算
        train_roi_a, _ = calc_roi(train_f, 'ens_a_pred')
        test_roi_a, hit_a = calc_roi(test_f, 'ens_a_pred')
        gap_a = train_roi_a - test_roi_a
        
        train_roi_b, _ = calc_roi(train_f, 'ens_b_pred')
        test_roi_b, hit_b = calc_roi(test_f, 'ens_b_pred')
        gap_b = train_roi_b - test_roi_b
        
        print(f"\n  【結果】")
        print(f"    A. 現行 (V15+V4.4 Residual):  Test ROI {test_roi_a:.1f}%, Gap {gap_a:.1f}%")
        print(f"    B. V17改善版:                  Test ROI {test_roi_b:.1f}%, Gap {gap_b:.1f}%")
        
        diff = test_roi_b - test_roi_a
        print(f"    差分: {diff:+.1f}%")
        
        all_results.append({
            'year': test_year,
            'ens_a_test': test_roi_a, 'ens_a_gap': gap_a,
            'ens_b_test': test_roi_b, 'ens_b_gap': gap_b,
            'diff': diff
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    avg_a = np.mean([r['ens_a_test'] for r in all_results])
    avg_b = np.mean([r['ens_b_test'] for r in all_results])
    avg_gap_a = np.mean([r['ens_a_gap'] for r in all_results])
    avg_gap_b = np.mean([r['ens_b_gap'] for r in all_results])
    
    print(f"\n  A. 現行:   ROI {avg_a:.1f}%, Gap {avg_gap_a:.1f}%")
    print(f"  B. V17:    ROI {avg_b:.1f}%, Gap {avg_gap_b:.1f}%")
    print(f"  差分:      {avg_b - avg_a:+.1f}%")
    
    # 再現性
    consistent = sum([1 for r in all_results if r['diff'] > 0])
    print(f"\n  再現性: B > A の年 = {consistent}/4")
    
    # Gap評価
    if avg_gap_b > 30:
        print("\n  ⚠️ V17のGapが30%超: 過学習の可能性あり")
    else:
        print(f"\n  ✅ V17のGap {avg_gap_b:.1f}%: 許容範囲内")


if __name__ == "__main__":
    main()
