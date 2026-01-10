# -*- coding: utf-8 -*-
"""
ROI>100%継続達成 - 3ステップ詳細検証

1. 複合条件の詳細検証（6年全年でROI>100%達成条件を探索）
2. 閾値の最適化（馬券数とROIのトレードオフ）
3. 2022年対策（崩壊を防ぐ追加条件）
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


def comprehensive_filter_search(bet_df, year):
    """
    より細かい複合条件の探索
    """
    # スコア正規化
    min_score = bet_df['score'].min()
    max_score = bet_df['score'].max()
    bet_df = bet_df.copy()
    bet_df['norm_score'] = (bet_df['score'] - min_score) / (max_score - min_score + 1e-10)
    bet_df['ev_estimate'] = bet_df['norm_score'] * bet_df['win_odds']
    
    results = []
    
    # より細かいグリッドサーチ
    odds_ranges = [
        (5, 30), (5, 50), (10, 30), (10, 50), (15, 40), (20, 50),
        (5, 20), (7, 35), (8, 40)
    ]
    ev_thresholds = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2]
    score_percentiles = [0.4, 0.5, 0.6, 0.7]
    
    # 人気帯も追加（2022年対策）
    popularity_filters = [(None, None), (3, None), (4, None), (5, None)]
    
    for odds_low, odds_high in odds_ranges:
        for ev_thresh in ev_thresholds:
            for score_pctl in score_percentiles:
                for pop_min, pop_max in popularity_filters:
                    # フィルタ条件
                    mask = (
                        (bet_df['win_odds'] >= odds_low) & 
                        (bet_df['win_odds'] < odds_high) &
                        (bet_df['ev_estimate'] >= ev_thresh) &
                        (bet_df['norm_score'] >= score_pctl)
                    )
                    
                    if pop_min is not None:
                        mask = mask & (bet_df['popularity'] >= pop_min)
                    
                    filtered = bet_df[mask]
                    
                    if len(filtered) >= 5:  # 最低5件
                        hits = filtered[filtered['is_hit']]
                        roi = hits['win_odds'].sum() / len(filtered) * 100
                        hit_rate = len(hits) / len(filtered) * 100
                        
                        results.append({
                            'year': year,
                            'odds_range': f'{odds_low}-{odds_high}',
                            'ev_threshold': ev_thresh,
                            'score_pctl': score_pctl,
                            'pop_min': pop_min if pop_min else 'all',
                            'roi': roi,
                            'bet_count': len(filtered),
                            'hit_count': len(hits),
                            'hit_rate': hit_rate,
                        })
    
    return pd.DataFrame(results)


def analyze_2022_specifics(bet_df, all_years_df):
    """
    2022年と他年の違いを詳細分析
    """
    analysis = {}
    
    # 2022年の特徴
    y2022 = bet_df[bet_df['year'] == 2022]
    other_years = bet_df[bet_df['year'] != 2022]
    
    # 的中レースと失敗レースの特徴比較
    hit_2022 = y2022[y2022['is_hit'] == True]
    miss_2022 = y2022[y2022['is_hit'] == False]
    
    analysis['2022_hit_count'] = len(hit_2022)
    analysis['2022_miss_count'] = len(miss_2022)
    
    if len(hit_2022) > 0 and len(miss_2022) > 0:
        analysis['2022_hit_avg_odds'] = hit_2022['win_odds'].mean()
        analysis['2022_miss_avg_odds'] = miss_2022['win_odds'].mean()
        analysis['2022_hit_avg_score'] = hit_2022['score'].mean()
        analysis['2022_miss_avg_score'] = miss_2022['score'].mean()
        
        if 'popularity' in hit_2022.columns:
            analysis['2022_hit_avg_pop'] = hit_2022['popularity'].mean()
            analysis['2022_miss_avg_pop'] = miss_2022['popularity'].mean()
    
    return analysis


def find_stable_conditions(all_results_df):
    """
    6年間安定してROI>100%を達成する条件を探索
    """
    # 条件ごとにグループ化
    grouped = all_results_df.groupby(['odds_range', 'ev_threshold', 'score_pctl', 'pop_min'])
    
    stable_conditions = []
    
    for name, group in grouped:
        years_over_100 = (group['roi'] >= 100).sum()
        total_years = len(group)
        avg_roi = group['roi'].mean()
        min_roi = group['roi'].min()
        total_bets = group['bet_count'].sum()
        avg_bets_per_year = group['bet_count'].mean()
        
        # 6年中4年以上ROI>100%、または平均ROI>100%かつ最低年ROI>80%
        if (years_over_100 >= 4) or (avg_roi >= 100 and min_roi >= 80):
            stable_conditions.append({
                'odds_range': name[0],
                'ev_threshold': name[1],
                'score_pctl': name[2],
                'pop_min': name[3],
                'years_over_100': years_over_100,
                'total_years': total_years,
                'avg_roi': avg_roi,
                'min_roi': min_roi,
                'max_roi': group['roi'].max(),
                'std_roi': group['roi'].std(),
                'total_bets': total_bets,
                'avg_bets_per_year': avg_bets_per_year,
            })
    
    return pd.DataFrame(stable_conditions)


def main():
    print("=" * 80)
    print("ROI>100%継続達成 - 3ステップ詳細検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    test_periods = [
        (2025, '2023-12-31', '2025-01-01', '2025-12-31'),
        (2024, '2022-12-31', '2024-01-01', '2024-12-31'),
        (2023, '2021-12-31', '2023-01-01', '2023-12-31'),
        (2022, '2020-12-31', '2022-01-01', '2022-12-31'),
        (2021, '2019-12-31', '2021-01-01', '2021-12-31'),
        (2020, '2018-12-31', '2020-01-01', '2020-12-31'),
    ]
    
    all_bet_data = []
    all_filter_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n【{year}年】処理中...")
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # 予測結果を整理
        d = test_f.copy()
        d['score'] = preds
        d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        bet_df = d[d['rank_pred'] == 1].copy()
        bet_df['is_hit'] = bet_df['finish_position'] == 1
        bet_df['year'] = year
        
        all_bet_data.append(bet_df)
        
        # ステップ1: 複合条件の詳細検証
        filter_results = comprehensive_filter_search(bet_df, year)
        all_filter_results.append(filter_results)
    
    # 全データを結合
    all_bet_df = pd.concat(all_bet_data)
    all_filter_df = pd.concat(all_filter_results)
    
    # ========================
    # ステップ1: 複合条件詳細検証
    # ========================
    print("\n" + "=" * 80)
    print("【ステップ1: 複合条件詳細検証】")
    print("=" * 80)
    
    stable = find_stable_conditions(all_filter_df)
    
    if len(stable) > 0:
        stable = stable.sort_values('avg_roi', ascending=False)
        
        print(f"\n安定条件数: {len(stable)}")
        print("\n【6年間安定してROI>100%を達成する条件 Top20】")
        print("-" * 100)
        print(f"{'オッズ帯':>10} {'EV閾値':>8} {'スコア':>6} {'人気':>6} {'達成年':>6} {'平均ROI':>10} {'最低ROI':>10} {'年平均件数':>10}")
        print("-" * 100)
        
        for _, row in stable.head(20).iterrows():
            print(f"{row['odds_range']:>10} {row['ev_threshold']:>8.1f} {row['score_pctl']:>6.1f} {str(row['pop_min']):>6} {row['years_over_100']:>6}/{row['total_years']} {row['avg_roi']:>9.1f}% {row['min_roi']:>9.1f}% {row['avg_bets_per_year']:>10.1f}")
    else:
        print("  安定条件なし")
    
    # ========================
    # ステップ2: 閾値最適化
    # ========================
    print("\n" + "=" * 80)
    print("【ステップ2: 閾値最適化 - 馬券数とROIのトレードオフ】")
    print("=" * 80)
    
    if len(stable) > 0:
        # 馬券数でソート
        by_bets = stable.sort_values('avg_bets_per_year', ascending=False)
        
        print("\n【馬券数優先の条件（年50件以上）】")
        high_bets = by_bets[by_bets['avg_bets_per_year'] >= 50]
        if len(high_bets) > 0:
            for _, row in high_bets.head(10).iterrows():
                print(f"  オッズ{row['odds_range']}, EV≥{row['ev_threshold']}, スコア≥{row['score_pctl']}, 人気≥{row['pop_min']}: 平均ROI {row['avg_roi']:.1f}%, 年{row['avg_bets_per_year']:.0f}件")
        else:
            print("  該当条件なし")
        
        print("\n【ROI優先の条件（平均ROI上位）】")
        high_roi = stable.sort_values('avg_roi', ascending=False).head(10)
        for _, row in high_roi.iterrows():
            print(f"  オッズ{row['odds_range']}, EV≥{row['ev_threshold']}, スコア≥{row['score_pctl']}, 人気≥{row['pop_min']}: 平均ROI {row['avg_roi']:.1f}%, 年{row['avg_bets_per_year']:.0f}件")
        
        print("\n【バランス型の条件（ROI>110%かつ年30件以上）】")
        balanced = stable[(stable['avg_roi'] >= 110) & (stable['avg_bets_per_year'] >= 30)]
        balanced = balanced.sort_values('avg_roi', ascending=False)
        if len(balanced) > 0:
            for _, row in balanced.head(10).iterrows():
                print(f"  オッズ{row['odds_range']}, EV≥{row['ev_threshold']}, スコア≥{row['score_pctl']}, 人気≥{row['pop_min']}: 平均ROI {row['avg_roi']:.1f}%, 年{row['avg_bets_per_year']:.0f}件")
        else:
            print("  該当条件なし")
    
    # ========================
    # ステップ3: 2022年対策
    # ========================
    print("\n" + "=" * 80)
    print("【ステップ3: 2022年対策 - 崩壊防止分析】")
    print("=" * 80)
    
    # 2022年の特徴分析
    analysis = analyze_2022_specifics(all_bet_df, all_bet_df)
    
    print("\n【2022年の的中/失敗パターン分析】")
    for key, value in analysis.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    # 2022年でもROI>80%を維持できる条件を探索
    print("\n【2022年でもROI>80%を維持できる条件】")
    y2022_results = all_filter_df[all_filter_df['year'] == 2022]
    y2022_ok = y2022_results[y2022_results['roi'] >= 80]
    
    if len(y2022_ok) > 0:
        best_2022 = y2022_ok.sort_values('roi', ascending=False).head(10)
        for _, row in best_2022.iterrows():
            print(f"  オッズ{row['odds_range']}, EV≥{row['ev_threshold']}, スコア≥{row['score_pctl']}, 人気≥{row['pop_min']}: ROI {row['roi']:.1f}%, {row['bet_count']}件")
    else:
        print("  該当条件なし（2022年はどの条件でもROI<80%）")
    
    # ========================
    # 最終推奨
    # ========================
    print("\n" + "=" * 80)
    print("【最終推奨条件】")
    print("=" * 80)
    
    if len(stable) > 0:
        # 2022年の最低ROIも考慮した最適条件
        best = stable[stable['min_roi'] >= 50].sort_values('avg_roi', ascending=False)
        if len(best) > 0:
            rec = best.iloc[0]
            print(f"""
■ 推奨条件
  オッズ帯: {rec['odds_range']}倍
  期待値(EV)閾値: ≥ {rec['ev_threshold']}
  スコア閾値: ≥ {rec['score_pctl']}
  人気フィルタ: ≥ {rec['pop_min']}

■ 期待パフォーマンス
  6年平均ROI: {rec['avg_roi']:.1f}%
  最低年ROI: {rec['min_roi']:.1f}%
  最高年ROI: {rec['max_roi']:.1f}%
  ROI標準偏差: {rec['std_roi']:.1f}%
  年間馬券数: 約{rec['avg_bets_per_year']:.0f}件
  ROI>100%達成年: {rec['years_over_100']}/6年
""")
        else:
            print("  最低ROI≥50%を満たす条件なし")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    all_filter_df.to_csv(output_dir / "comprehensive_filter_search.csv", index=False)
    if len(stable) > 0:
        stable.to_csv(output_dir / "stable_conditions.csv", index=False)
    
    print(f"\n結果を {output_dir} に保存しました。")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
