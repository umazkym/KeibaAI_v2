# -*- coding: utf-8 -*-
"""
多角的詳細分析 - 実際のデータを1つずつ確認

目的:
- 年間を通じて意味のある馬券数（200件以上/年）
- 人気馬で当たりつつ、穴馬も狙える戦略
- 条件を広げつつROI>100%を目指す

分析観点:
1. オッズ帯別の詳細分析（なぜ各帯でROIが異なるか）
2. 人気馬の的中パターン（どの人気が期待値高いか）
3. スコア上位が外れるパターン（なぜ外れるか）
4. 年間安定する条件の探索
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

from keibaai.src.features.leak_free_feature_engineer_v33 import LeakFreeFeatureEngineerV33


def load_data():
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 8,
        'max_depth': 2,
        'min_child_samples': 300,
        'reg_alpha': 15.0,
        'reg_lambda': 20.0,
        'bagging_fraction': 0.5,
        'bagging_freq': 3,
        'feature_fraction': 0.5,
        'random_state': 42,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    
    return model


def detailed_analysis(bet_df):
    """実データを詳細に分析"""
    
    print("\n" + "=" * 80)
    print("【分析1: 人気別 × オッズ帯別 詳細分析】")
    print("=" * 80)
    print("※人気ごとにオッズがどう分布し、ROIがどうなるか")
    
    for pop in range(1, 11):
        subset = bet_df[bet_df['popularity'] == pop]
        if len(subset) < 100:
            continue
        
        hits = subset[subset['is_hit']]
        roi = hits['win_odds'].sum() / len(subset) * 100
        hit_rate = len(hits) / len(subset) * 100
        avg_odds = subset['win_odds'].mean()
        
        mark = "★" if roi >= 100 else ""
        print(f"  {pop}番人気: ROI {roi:.1f}%{mark}, 的中率 {hit_rate:.1f}%, 平均オッズ {avg_odds:.1f}倍, {len(subset)}件")
    
    print("\n" + "=" * 80)
    print("【分析2: モデルスコア四分位別の詳細分析】")
    print("=" * 80)
    print("※スコアの高さとROIの関係")
    
    bet_df = bet_df.copy()
    bet_df['score_quartile'] = pd.qcut(bet_df['score'], 4, labels=['Q1(低)', 'Q2', 'Q3', 'Q4(高)'])
    
    for q in ['Q1(低)', 'Q2', 'Q3', 'Q4(高)']:
        subset = bet_df[bet_df['score_quartile'] == q]
        hits = subset[subset['is_hit']]
        roi = hits['win_odds'].sum() / len(subset) * 100
        hit_rate = len(hits) / len(subset) * 100
        avg_odds = subset['win_odds'].mean()
        avg_pop = subset['popularity'].mean()
        
        mark = "★" if roi >= 100 else ""
        print(f"  {q}: ROI {roi:.1f}%{mark}, 的中率 {hit_rate:.1f}%, 平均オッズ {avg_odds:.1f}倍, 平均人気 {avg_pop:.1f}, {len(subset)}件")
    
    print("\n" + "=" * 80)
    print("【分析3: 人気 × スコア四分位 クロス分析】")
    print("=" * 80)
    print("※人気馬なのにスコア高い/低いケースの分析")
    
    bet_df['pop_group'] = pd.cut(bet_df['popularity'], bins=[0, 2, 5, 10, 18], labels=['1-2人気', '3-5人気', '6-10人気', '11人気以下'])
    
    print("\n       | Q1(低スコア) | Q2 | Q3 | Q4(高スコア) |")
    print("-" * 60)
    
    for pop_group in ['1-2人気', '3-5人気', '6-10人気', '11人気以下']:
        row = f"{pop_group:>10} |"
        for q in ['Q1(低)', 'Q2', 'Q3', 'Q4(高)']:
            subset = bet_df[(bet_df['pop_group'] == pop_group) & (bet_df['score_quartile'] == q)]
            if len(subset) >= 50:
                hits = subset[subset['is_hit']]
                roi = hits['win_odds'].sum() / len(subset) * 100
                mark = "★" if roi >= 100 else ""
                row += f" {roi:>5.0f}%{mark} |"
            else:
                row += "   N/A |"
        print(row)
    
    print("\n" + "=" * 80)
    print("【分析4: 「モデルが強気だが人気薄」のケース分析】")
    print("=" * 80)
    print("※穴馬狙いの可能性")
    
    # 高スコアかつ人気薄
    high_score_low_pop = bet_df[(bet_df['score_quartile'] == 'Q4(高)') & (bet_df['popularity'] >= 4)]
    if len(high_score_low_pop) > 0:
        hits = high_score_low_pop[high_score_low_pop['is_hit']]
        roi = hits['win_odds'].sum() / len(high_score_low_pop) * 100
        hit_rate = len(hits) / len(high_score_low_pop) * 100
        print(f"  高スコア(Q4) × 4人気以下: ROI {roi:.1f}%, 的中率 {hit_rate:.1f}%, {len(high_score_low_pop)}件")
    
    # さらに絞り込み
    for min_pop in [5, 6, 7]:
        subset = bet_df[(bet_df['score_quartile'].isin(['Q3', 'Q4(高)'])) & (bet_df['popularity'] >= min_pop)]
        if len(subset) >= 50:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100
            hit_rate = len(hits) / len(subset) * 100
            mark = "★" if roi >= 100 else ""
            print(f"  高スコア(Q3-Q4) × {min_pop}人気以下: ROI {roi:.1f}%{mark}, 的中率 {hit_rate:.1f}%, {len(subset)}件")
    
    print("\n" + "=" * 80)
    print("【分析5: 「モデルTop1かつ人気もTop3」のケース（堅い馬券）】")
    print("=" * 80)
    print("※人気馬で稼ぐ可能性")
    
    pop_top3 = bet_df[bet_df['popularity'] <= 3]
    if len(pop_top3) > 0:
        hits = pop_top3[pop_top3['is_hit']]
        roi = hits['win_odds'].sum() / len(pop_top3) * 100
        hit_rate = len(hits) / len(pop_top3) * 100
        print(f"  モデルTop1 × 1-3番人気: ROI {roi:.1f}%, 的中率 {hit_rate:.1f}%, {len(pop_top3)}件")
    
    # 人気×スコアでさらに絞り込み
    pop_top3_high_score = bet_df[(bet_df['popularity'] <= 3) & (bet_df['score_quartile'] == 'Q4(高)')]
    if len(pop_top3_high_score) >= 50:
        hits = pop_top3_high_score[pop_top3_high_score['is_hit']]
        roi = hits['win_odds'].sum() / len(pop_top3_high_score) * 100
        hit_rate = len(hits) / len(pop_top3_high_score) * 100
        print(f"  モデルTop1 × 1-3番人気 × 高スコア(Q4): ROI {roi:.1f}%, 的中率 {hit_rate:.1f}%, {len(pop_top3_high_score)}件")
    
    print("\n" + "=" * 80)
    print("【分析6: 複合戦略シミュレーション】")
    print("=" * 80)
    print("※人気馬で的中しつつ、穴馬も狙う戦略")
    
    # 戦略: 高スコア人気馬 + 高スコア穴馬
    strategy_pop = bet_df[(bet_df['score_quartile'] == 'Q4(高)') & (bet_df['popularity'] <= 3)]
    strategy_dark = bet_df[(bet_df['score_quartile'] == 'Q4(高)') & (bet_df['popularity'] >= 5)]
    
    combined = pd.concat([strategy_pop, strategy_dark])
    if len(combined) > 0:
        hits = combined[combined['is_hit']]
        roi = hits['win_odds'].sum() / len(combined) * 100
        hit_rate = len(hits) / len(combined) * 100
        print(f"\n  【戦略A: 高スコア人気馬 OR 高スコア穴馬】")
        print(f"    ROI: {roi:.1f}%, 的中率: {hit_rate:.1f}%, 馬券数: {len(combined)}件")
    
    # 戦略B: スコア上位50%で人気馬 OR スコア上位25%で穴馬
    strategy_b_pop = bet_df[(bet_df['score_quartile'].isin(['Q3', 'Q4(高)'])) & (bet_df['popularity'] <= 3)]
    strategy_b_dark = bet_df[(bet_df['score_quartile'] == 'Q4(高)') & (bet_df['popularity'] >= 6)]
    
    combined_b = pd.concat([strategy_b_pop, strategy_b_dark])
    if len(combined_b) > 0:
        hits = combined_b[combined_b['is_hit']]
        roi = hits['win_odds'].sum() / len(combined_b) * 100
        hit_rate = len(hits) / len(combined_b) * 100
        print(f"\n  【戦略B: (高スコア × 人気馬) OR (最高スコア × 穴馬)】")
        print(f"    ROI: {roi:.1f}%, 的中率: {hit_rate:.1f}%, 馬券数: {len(combined_b)}件")
    
    return bet_df


def main():
    print("=" * 80)
    print("多角的詳細分析 - 実データ1つずつ確認")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # まず直近2年（2024-2025）で詳細分析
    train_end = '2023-12-31'
    test_start = '2024-01-01'
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[races['race_date'] >= test_start].copy()
    
    print(f"\n訓練データ: {len(train):,}件")
    print(f"テストデータ: {len(test):,}件（2024-2025年）")
    
    print("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV33()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    print(f"特徴量数: {len(feature_cols)}")
    
    # Validation split
    val_start = '2023-01-01'
    train_sub = train_f[train_f['race_date'] < val_start]
    valid_sub = train_f[train_f['race_date'] >= val_start]
    
    print("モデル学習中...")
    model = train_model(train_sub, valid_sub, feature_cols)
    
    X_test = test_f[feature_cols].fillna(0)
    preds = model.predict(X_test)
    
    # 予測結果を整理
    d = test_f.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    print(f"\nモデルTop1馬: {len(bet_df)}件（2024-2025年）")
    
    # 詳細分析
    bet_df = detailed_analysis(bet_df)
    
    # 年別に分解して確認
    print("\n" + "=" * 80)
    print("【年別分析: 上記戦略の年間安定性】")
    print("=" * 80)
    
    bet_df['year'] = bet_df['race_date'].dt.year
    
    for year in [2024, 2025]:
        year_df = bet_df[bet_df['year'] == year]
        if len(year_df) == 0:
            continue
        
        print(f"\n■ {year}年")
        
        # 全体
        hits = year_df[year_df['is_hit']]
        roi = hits['win_odds'].sum() / len(year_df) * 100
        print(f"  全体: ROI {roi:.1f}%, {len(year_df)}件")
        
        # 戦略A
        strategy_pop = year_df[(year_df['score_quartile'] == 'Q4(高)') & (year_df['popularity'] <= 3)]
        strategy_dark = year_df[(year_df['score_quartile'] == 'Q4(高)') & (year_df['popularity'] >= 5)]
        combined = pd.concat([strategy_pop, strategy_dark])
        if len(combined) > 0:
            hits = combined[combined['is_hit']]
            roi = hits['win_odds'].sum() / len(combined) * 100
            mark = "★" if roi >= 100 else ""
            print(f"  戦略A: ROI {roi:.1f}%{mark}, {len(combined)}件")
        
        # 戦略B
        strategy_b_pop = year_df[(year_df['score_quartile'].isin(['Q3', 'Q4(高)'])) & (year_df['popularity'] <= 3)]
        strategy_b_dark = year_df[(year_df['score_quartile'] == 'Q4(高)') & (year_df['popularity'] >= 6)]
        combined_b = pd.concat([strategy_b_pop, strategy_b_dark])
        if len(combined_b) > 0:
            hits = combined_b[combined_b['is_hit']]
            roi = hits['win_odds'].sum() / len(combined_b) * 100
            mark = "★" if roi >= 100 else ""
            print(f"  戦略B: ROI {roi:.1f}%{mark}, {len(combined_b)}件")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
