# -*- coding: utf-8 -*-
"""
V15+V22 Binary Ensemble 探索的データ分析

【目的】
1. V15とV22の予測値の相関・分布を確認
2. どのケースで一致/不一致するかを分析
3. リークがないことを検証
4. 最適なブレンド比率のヒントを得る

【過学習/リーク防止策】
- 重み最適化には2021-2022年のみ使用
- 2023-2024年は純粋なテストとして保持
- 訓練データに未来情報が混入していないか確認
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

# パス設定
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
from keibaai.src.features.leak_free_feature_engineer_v22 import LeakFreeFeatureEngineerV22


def load_data():
    """データ読み込み"""
    data_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet'
    
    print("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / 'races' / 'races.parquet')
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees' / 'pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corners' / 'corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details' / 'race_details.parquet')
    
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    print(f"  races: {len(races_df):,}件")
    return races_df, pedigrees_df, corners_df, race_details_df


def train_binary_model(X_train, y_train, strong_reg=False):
    """Binary Classification モデル"""
    if strong_reg:
        params = {
            'objective': 'binary', 'metric': 'auc', 'boosting_type': 'gbdt',
            'num_leaves': 20, 'max_depth': 4, 'learning_rate': 0.05,
            'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 5.0,
            'subsample': 0.6, 'colsample_bytree': 0.6, 'verbose': -1,
        }
    else:
        params = {
            'objective': 'binary', 'metric': 'auc', 'boosting_type': 'gbdt',
            'num_leaves': 31, 'max_depth': 5, 'learning_rate': 0.05,
            'min_child_samples': 50, 'reg_alpha': 1.0, 'reg_lambda': 1.0,
            'subsample': 0.8, 'colsample_bytree': 0.8, 'verbose': -1,
        }
    
    train_data = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_data, num_boost_round=200)
    return model


def calc_roi_by_blending(df, v15_col, v22_col, weight_v15):
    """指定されたブレンド比率でROIを計算"""
    df = df.copy()
    df['pred_blend'] = df[v15_col] * weight_v15 + df[v22_col] * (1 - weight_v15)
    df['rank'] = df.groupby('race_id')['pred_blend'].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1]
    
    total_bet = len(top1)
    wins = top1[top1['finish_position'] == 1]
    total_return = wins['win_odds'].sum()
    
    return (total_return / total_bet * 100) if total_bet > 0 else 0


def analyze_year(train_df, test_df, pedigrees_df, corners_df, race_details_df, test_year):
    """1年分の詳細分析"""
    print(f"\n{'='*70}")
    print(f"  {test_year}年 分析")
    print(f"{'='*70}")
    
    # ===== V15 Fixed 特徴量 =====
    print(f"  V15特徴量エンジン fit/transform...")
    engine_v15 = LeakFreeFeatureEngineerV15Fixed()
    engine_v15.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_v15 = engine_v15.transform(train_df)
    test_v15 = engine_v15.transform(test_df)
    feature_cols_v15 = engine_v15.get_feature_columns()
    
    X_train_v15 = train_v15[feature_cols_v15].fillna(0).values
    X_test_v15 = test_v15[feature_cols_v15].fillna(0).values
    y_train = (train_v15['finish_position'] == 1).astype(int).values
    
    # ===== V22 特徴量 =====
    print(f"  V22特徴量エンジン fit/transform...")
    engine_v22 = LeakFreeFeatureEngineerV22()
    engine_v22.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_v22 = engine_v22.transform(train_df)
    test_v22 = engine_v22.transform(test_df)
    feature_cols_v22 = engine_v22.get_feature_columns()
    
    X_train_v22 = train_v22[feature_cols_v22].fillna(0).values
    X_test_v22 = test_v22[feature_cols_v22].fillna(0).values
    y_train_v22 = (train_v22['finish_position'] == 1).astype(int).values
    
    # ===== モデル学習 =====
    print(f"  V15 Binary モデル学習...")
    model_v15 = train_binary_model(X_train_v15, y_train, strong_reg=False)
    train_v15['pred_v15'] = model_v15.predict(X_train_v15)
    test_v15['pred_v15'] = model_v15.predict(X_test_v15)
    
    print(f"  V22 Binary モデル学習...")
    model_v22 = train_binary_model(X_train_v22, y_train_v22, strong_reg=True)
    train_v22['pred_v22'] = model_v22.predict(X_train_v22)
    test_v22['pred_v22'] = model_v22.predict(X_test_v22)
    
    # ===== リークチェック =====
    print(f"\n  【リークチェック】")
    print(f"    訓練データ期間: {train_df['race_date'].min()} ~ {train_df['race_date'].max()}")
    print(f"    テストデータ期間: {test_df['race_date'].min()} ~ {test_df['race_date'].max()}")
    
    # 訓練データ内に未来のrace_dateがないか確認
    train_max_date = train_df['race_date'].max()
    test_min_date = test_df['race_date'].min()
    if train_max_date >= test_min_date:
        print(f"    ⚠️ 警告: 訓練データが未来を含んでいる可能性あり!")
    else:
        print(f"    ✅ 訓練データは {test_min_date} より前のみ")
    
    # V22特徴量のNaN率を確認（リーク兆候）
    v22_new_features = ['horse_season_win_rate', 'horse_summer_performance', 
                        'horse_venue_win_rate', 'jockey_venue_win_rate', 'trainer_venue_win_rate']
    for feat in v22_new_features:
        if feat in train_v22.columns:
            train_non_nan = train_v22[feat].notna().mean() * 100
            test_non_nan = test_v22[feat].notna().mean() * 100
            print(f"    {feat}: Train非NaN率={train_non_nan:.1f}%, Test非NaN率={test_non_nan:.1f}%")
    
    # ===== 予測値分布の分析 =====
    print(f"\n  【予測値分布】")
    
    # テストデータでの分析
    # V22予測をV15にマージ
    v22_test_pred = test_v22[['race_id', 'horse_id', 'pred_v22']].drop_duplicates(['race_id', 'horse_id'])
    test_merged = test_v15.merge(v22_test_pred, on=['race_id', 'horse_id'], how='left')
    test_merged['pred_v22'] = test_merged['pred_v22'].fillna(test_merged['pred_v15'])
    
    # 相関係数
    corr = test_merged[['pred_v15', 'pred_v22']].corr().iloc[0, 1]
    print(f"    V15とV22の相関係数: {corr:.4f}")
    
    # 分布統計
    print(f"    V15予測: mean={test_merged['pred_v15'].mean():.4f}, std={test_merged['pred_v15'].std():.4f}")
    print(f"    V22予測: mean={test_merged['pred_v22'].mean():.4f}, std={test_merged['pred_v22'].std():.4f}")
    
    # ===== 不一致ケースの分析 =====
    print(f"\n  【Top1予測の一致/不一致分析】")
    test_merged['rank_v15'] = test_merged.groupby('race_id')['pred_v15'].rank(ascending=False, method='first')
    test_merged['rank_v22'] = test_merged.groupby('race_id')['pred_v22'].rank(ascending=False, method='first')
    
    top1_v15 = test_merged[test_merged['rank_v15'] == 1].copy()
    top1_v22 = test_merged[test_merged['rank_v22'] == 1].copy()
    
    # 一致レース
    same_pick = (top1_v15['horse_id'].values == top1_v22['horse_id'].values).sum() if len(top1_v15) == len(top1_v22) else 0
    total_races = len(top1_v15)
    print(f"    同一馬選択レース数: {same_pick}/{total_races} ({same_pick/total_races*100:.1f}%)")
    
    # 不一致時のパフォーマンス
    # V15のみ正解
    v15_correct = top1_v15[top1_v15['finish_position'] == 1]
    v22_correct = top1_v22[top1_v22['finish_position'] == 1]
    
    print(f"    V15正解数: {len(v15_correct)}, V22正解数: {len(v22_correct)}")
    
    # ===== ブレンド比率の探索 (Train ROI only for insight, NOT for optimization) =====
    print(f"\n  【ブレンド比率感度分析】（参考値）")
    
    # v22_train_predをv15_trainにマージ
    v22_train_pred = train_v22[['race_id', 'horse_id', 'pred_v22']].drop_duplicates(['race_id', 'horse_id'])
    train_merged = train_v15.merge(v22_train_pred, on=['race_id', 'horse_id'], how='left')
    train_merged['pred_v22'] = train_merged['pred_v22'].fillna(train_merged['pred_v15'])
    
    weights = [0.0, 0.3, 0.5, 0.7, 1.0]
    for w in weights:
        train_roi = calc_roi_by_blending(train_merged, 'pred_v15', 'pred_v22', w)
        test_roi = calc_roi_by_blending(test_merged, 'pred_v15', 'pred_v22', w)
        gap = train_roi - test_roi
        print(f"    w={w:.1f} (V15:{int(w*100)}%, V22:{int((1-w)*100)}%): Train={train_roi:.1f}%, Test={test_roi:.1f}%, Gap={gap:.1f}%")
    
    # ===== セグメント別分析 =====
    print(f"\n  【セグメント別分析】")
    test_merged['month'] = pd.to_datetime(test_merged['race_date']).dt.month
    
    # 夏場（8-9月）
    summer = test_merged[test_merged['month'].isin([8, 9])]
    if len(summer) > 0:
        summer_v15_roi = calc_roi_by_blending(summer, 'pred_v15', 'pred_v22', 1.0)
        summer_v22_roi = calc_roi_by_blending(summer, 'pred_v15', 'pred_v22', 0.0)
        summer_50_roi = calc_roi_by_blending(summer, 'pred_v15', 'pred_v22', 0.5)
        print(f"    夏場(8-9月): V15={summer_v15_roi:.1f}%, V22={summer_v22_roi:.1f}%, 50/50={summer_50_roi:.1f}%")
    
    # 冬場（1-2月, 12月）
    winter = test_merged[test_merged['month'].isin([1, 2, 12])]
    if len(winter) > 0:
        winter_v15_roi = calc_roi_by_blending(winter, 'pred_v15', 'pred_v22', 1.0)
        winter_v22_roi = calc_roi_by_blending(winter, 'pred_v15', 'pred_v22', 0.0)
        winter_50_roi = calc_roi_by_blending(winter, 'pred_v15', 'pred_v22', 0.5)
        print(f"    冬場(1-2,12月): V15={winter_v15_roi:.1f}%, V22={winter_v22_roi:.1f}%, 50/50={winter_50_roi:.1f}%")
    
    # 苦手競馬場
    weak_venues = ['札幌', '新潟', '阪神']
    weak = test_merged[test_merged['venue'].isin(weak_venues)]
    if len(weak) > 0:
        weak_v15_roi = calc_roi_by_blending(weak, 'pred_v15', 'pred_v22', 1.0)
        weak_v22_roi = calc_roi_by_blending(weak, 'pred_v15', 'pred_v22', 0.0)
        weak_50_roi = calc_roi_by_blending(weak, 'pred_v15', 'pred_v22', 0.5)
        print(f"    苦手場({','.join(weak_venues)}): V15={weak_v15_roi:.1f}%, V22={weak_v22_roi:.1f}%, 50/50={weak_50_roi:.1f}%")
    
    return {
        'year': test_year,
        'correlation': corr,
        'same_pick_rate': same_pick / total_races if total_races > 0 else 0,
        'v15_wins': len(v15_correct),
        'v22_wins': len(v22_correct),
    }


def main():
    print("=" * 80)
    print("V15+V22 Binary Ensemble 探索的データ分析")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    years = [2021, 2022, 2023, 2024]
    results = []
    
    for test_year in years:
        train_df = races_df[races_df['race_date'] < f'{test_year}-01-01'].copy()
        test_df = races_df[
            (races_df['race_date'] >= f'{test_year}-01-01') & 
            (races_df['race_date'] <= f'{test_year}-12-31')
        ].copy()
        
        print(f"\n  Train: {len(train_df):,}件, Test: {len(test_df):,}件")
        
        result = analyze_year(train_df, test_df, pedigrees_df, corners_df, race_details_df, test_year)
        results.append(result)
    
    print("\n" + "=" * 80)
    print("総合サマリー")
    print("=" * 80)
    
    df_results = pd.DataFrame(results)
    print(f"\n平均相関係数: {df_results['correlation'].mean():.4f}")
    print(f"平均同一選択率: {df_results['same_pick_rate'].mean()*100:.1f}%")
    print(f"\n年度別:")
    for _, row in df_results.iterrows():
        print(f"  {int(row['year'])}: 相関={row['correlation']:.4f}, 同一選択={row['same_pick_rate']*100:.1f}%")
    
    print("\n分析完了")


if __name__ == '__main__':
    main()
