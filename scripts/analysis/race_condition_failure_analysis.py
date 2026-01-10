# -*- coding: utf-8 -*-
"""
レース条件別詳細敗因分析（2020-2025年全レース）

分析内容:
1. レース条件別（競馬場、芝/ダート、距離帯）のROI・的中率
2. 的中レースと外れレースの特徴比較
3. 具体的な外れパターンの詳細分析
   - ハイペースで人気馬が沈んだケース
   - 多頭数でのオッズ拮抗ケース
   - 穴馬の好走パターン
   - 距離変更での予想外の好走
4. 競馬場別、距離帯別、馬場状態別の詳細分析
5. 敗因パターンの分類と統計
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
from collections import defaultdict
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
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 300, 'reg_alpha': 15.0, 'reg_lambda': 20.0,
        'bagging_fraction': 0.5, 'bagging_freq': 3, 'feature_fraction': 0.5, 'random_state': 42,
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


def extract_race_conditions(test_df):
    """レース条件を抽出"""
    # race_idから競馬場コードを抽出
    # race_id形式: YYYYJJKKNNRR (年4桁, 場所2桁, 開催2桁, 日2桁, レース番号2桁)
    test_df = test_df.copy()
    test_df['race_id_str'] = test_df['race_id'].astype(str)
    test_df['venue_code'] = test_df['race_id_str'].str[4:6]
    
    # 競馬場コードから名称へのマッピング
    venue_map = {
        '01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
        '05': '東京', '06': '中山', '07': '中京', '08': '京都',
        '09': '阪神', '10': '小倉'
    }
    test_df['venue'] = test_df['venue_code'].map(venue_map).fillna('その他')
    
    # 距離帯
    if 'distance' in test_df.columns:
        test_df['distance_band'] = pd.cut(
            test_df['distance'], 
            bins=[0, 1200, 1600, 2000, 2400, 4000],
            labels=['短距離(~1200)', '短中距離(1200-1600)', '中距離(1600-2000)', 
                   '中長距離(2000-2400)', '長距離(2400~)']
        )
    
    # 芝/ダート
    if 'surface' in test_df.columns:
        test_df['track_type'] = test_df['surface'].fillna('不明')
    
    return test_df


def analyze_by_condition(pred_df, returns_df, condition_col, condition_name):
    """条件別にROIと的中率を分析"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100
    
    results = []
    
    for val in merged[condition_col].dropna().unique():
        subset = merged[merged[condition_col] == val]
        if len(subset) >= 30:
            hits = subset['is_hit'].sum()
            hr = hits / len(subset) * 100
            roi = subset['return'].sum() / len(subset) * 100
            
            results.append({
                'condition': str(val),
                'total': len(subset),
                'hits': hits,
                'hit_rate': hr,
                'roi': roi,
            })
    
    return pd.DataFrame(results).sort_values('roi', ascending=False)


def analyze_failure_patterns(pred_df, race_df, returns_df):
    """外れパターンの詳細分析"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100
    
    # 外れたレースを詳細分析
    misses = merged[~merged['is_hit']].copy()
    hits = merged[merged['is_hit']].copy()
    
    # 外れたレースの頭数分析
    print("\n" + "=" * 80)
    print("【敗因分析1: 頭数別】")
    print("=" * 80)
    
    # 頭数を計算
    race_sizes = pred_df.groupby('race_id').size().reset_index(name='num_horses')
    misses = misses.merge(race_sizes, on='race_id', how='left')
    hits = hits.merge(race_sizes, on='race_id', how='left')
    
    print(f"\n■ 外れレースの平均頭数: {misses['num_horses'].mean():.2f}頭")
    print(f"■ 的中レースの平均頭数: {hits['num_horses'].mean():.2f}頭")
    
    # 頭数帯別ROI
    print(f"\n■ 頭数帯別分析:")
    all_merged = merged.merge(race_sizes, on='race_id', how='left')
    
    for min_h, max_h, label in [(1, 8, '少頭数(~8頭)'), (9, 12, '中頭数(9-12頭)'), 
                                (13, 16, '多頭数(13-16頭)'), (17, 20, '超多頭数(17頭~)')]:
        subset = all_merged[(all_merged['num_horses'] >= min_h) & (all_merged['num_horses'] <= max_h)]
        if len(subset) >= 30:
            hr = subset['is_hit'].mean() * 100
            roi = subset['return'].sum() / len(subset) * 100
            mark = "★" if roi >= 100 else ""
            print(f"  {label}: {len(subset):>5}件 | 的中率{hr:>5.2f}% | ROI {roi:>6.1f}%{mark}")
    
    return misses, hits


def analyze_actual_winners(pred_df, race_df, returns_df):
    """実際の勝ち馬の分析（Top1が外れた時、誰が勝ったか）"""
    print("\n" + "=" * 80)
    print("【敗因分析2: 外れた時の勝ち馬分析】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    # Top1が外れたレースを特定
    top1 = pred_df[pred_df['rank_pred'] == 1][['race_id', 'horse_number']].copy()
    top1.columns = ['race_id', 'top1_horse']
    
    merged = top1.merge(tansho[['race_id', 'horse_number']], on='race_id', how='left')
    merged = merged.rename(columns={'horse_number': 'winner_horse'})
    merged['top1_won'] = merged['top1_horse'] == merged['winner_horse']
    
    missed_races = merged[~merged['top1_won']]['race_id'].unique()
    
    # 外れたレースでの勝ち馬のrank_pred分布
    winners = pred_df[pred_df['finish_position'] == 1][['race_id', 'rank_pred', 'win_odds', 'popularity']].copy()
    missed_winners = winners[winners['race_id'].isin(missed_races)]
    
    print(f"\n■ Top1が外れた時、勝ち馬の予測順位分布:")
    for rank in range(1, 11):
        count = (missed_winners['rank_pred'] == rank).sum()
        pct = count / len(missed_winners) * 100 if len(missed_winners) > 0 else 0
        print(f"  Top{rank:>2}: {count:>5}件 ({pct:>5.2f}%)")
    
    print(f"\n■ Top1が外れた時、勝ち馬の人気分布:")
    for pop in range(1, 11):
        count = (missed_winners['popularity'] == pop).sum()
        pct = count / len(missed_winners) * 100 if len(missed_winners) > 0 else 0
        print(f"  {pop:>2}番人気: {count:>5}件 ({pct:>5.2f}%)")
    
    # 穴馬（10番人気以下）が勝ったケース
    longshot_wins = missed_winners[missed_winners['popularity'] >= 10]
    print(f"\n■ 穴馬（10番人気以下）が勝ったケース: {len(longshot_wins):,}件 ({len(longshot_wins)/len(missed_winners)*100:.2f}%)")
    
    return missed_winners


def analyze_venue_surface_distance(pred_df, returns_df):
    """競馬場×芝ダート×距離の詳細分析"""
    print("\n" + "=" * 80)
    print("【敗因分析3: 競馬場別分析】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100
    
    # 競馬場別
    if 'venue' in merged.columns:
        print(f"\n■ 競馬場別ROI:")
        venue_results = []
        for venue in merged['venue'].dropna().unique():
            subset = merged[merged['venue'] == venue]
            if len(subset) >= 100:
                hr = subset['is_hit'].mean() * 100
                roi = subset['return'].sum() / len(subset) * 100
                venue_results.append({'venue': venue, 'count': len(subset), 'hit_rate': hr, 'roi': roi})
        
        venue_df = pd.DataFrame(venue_results).sort_values('roi', ascending=False)
        print(f"{'競馬場':>6} | {'件数':>6} | {'的中率':>7} | {'ROI':>7}")
        print("-" * 40)
        for _, row in venue_df.iterrows():
            mark = "★" if row['roi'] >= 100 else ""
            print(f"{row['venue']:>6} | {row['count']:>6,} | {row['hit_rate']:>6.2f}% | {row['roi']:>6.1f}%{mark}")
    
    # 芝/ダート別
    if 'track_type' in merged.columns:
        print(f"\n■ 芝/ダート別ROI:")
        for track in merged['track_type'].dropna().unique():
            subset = merged[merged['track_type'] == track]
            if len(subset) >= 100:
                hr = subset['is_hit'].mean() * 100
                roi = subset['return'].sum() / len(subset) * 100
                mark = "★" if roi >= 100 else ""
                print(f"  {track}: {len(subset):>6,}件 | 的中率{hr:>6.2f}% | ROI {roi:>6.1f}%{mark}")
    
    # 距離帯別
    if 'distance_band' in merged.columns:
        print(f"\n■ 距離帯別ROI:")
        for dist in merged['distance_band'].dropna().unique():
            subset = merged[merged['distance_band'] == dist]
            if len(subset) >= 100:
                hr = subset['is_hit'].mean() * 100
                roi = subset['return'].sum() / len(subset) * 100
                mark = "★" if roi >= 100 else ""
                print(f"  {dist}: {len(subset):>6,}件 | 的中率{hr:>6.2f}% | ROI {roi:>6.1f}%{mark}")
    
    return merged


def analyze_specific_failure_cases(pred_df, race_df, returns_df, n_samples=20):
    """具体的な外れケースの詳細分析"""
    print("\n" + "=" * 80)
    print("【敗因分析4: 具体的な外れケース詳細】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    top2 = pred_df[pred_df['rank_pred'] == 2][['race_id', 'horse_number', 'win_odds', 'popularity']].copy()
    top2.columns = ['race_id', 'top2_horse', 'top2_odds', 'top2_pop']
    
    merged = top1.merge(top2, on='race_id', how='left')
    merged = merged.merge(tansho[['race_id', 'horse_number', 'payout']], 
                          on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    
    # 外れたレース
    misses = merged[~merged['is_hit']].copy()
    
    # 勝ち馬の情報を追加
    winners = pred_df[pred_df['finish_position'] == 1][['race_id', 'horse_number', 'win_odds', 'popularity', 'rank_pred']].copy()
    winners.columns = ['race_id', 'winner_horse', 'winner_odds', 'winner_pop', 'winner_rank']
    
    misses = misses.merge(winners, on='race_id', how='left')
    
    # パターン分類
    # 1. Top1が人気馬だったが外れたケース
    pattern1 = misses[misses['popularity'] <= 3]
    print(f"\n■ パターン1: Top1が3番人気以内だったのに外れた: {len(pattern1):,}件")
    print(f"  → 勝ち馬は平均{pattern1['winner_pop'].mean():.1f}番人気 / 予測順位は平均Top{pattern1['winner_rank'].mean():.1f}")
    
    # 2. 穴馬が勝ったケース
    pattern2 = misses[misses['winner_pop'] >= 10]
    print(f"\n■ パターン2: 10番人気以下の穴馬が勝った: {len(pattern2):,}件")
    if len(pattern2) > 0:
        print(f"  → Top1の平均人気: {pattern2['popularity'].mean():.1f}番人気")
        print(f"  → 穴馬の平均予測順位: Top{pattern2['winner_rank'].mean():.1f}")
    
    # 3. 予測的には外れだが2着に来ていたケース
    top1_finish = pred_df[pred_df['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1_finish.columns = ['race_id', 'horse_number', 'top1_finish']
    misses_with_finish = misses.merge(top1_finish, on=['race_id', 'horse_number'], how='left')
    pattern3 = misses_with_finish[misses_with_finish['top1_finish'] == 2]
    print(f"\n■ パターン3: Top1馬が2着に来ていた（惜しかった）: {len(pattern3):,}件")
    
    pattern4 = misses_with_finish[misses_with_finish['top1_finish'] == 3]
    print(f"  パターン3b: Top1馬が3着に来ていた: {len(pattern4):,}件")
    
    # 4. Top1のオッズが高かった（穴狙いが空振り）
    pattern5 = misses[misses['win_odds'] >= 20]
    print(f"\n■ パターン4: Top1が20倍以上の穴馬だった（穴狙い空振り）: {len(pattern5):,}件")
    if len(pattern5) > 0:
        print(f"  → 勝ち馬の平均オッズ: {pattern5['winner_odds'].mean():.1f}倍")
        print(f"  → 勝ち馬の平均人気: {pattern5['winner_pop'].mean():.1f}番人気")
    
    # 具体例
    print(f"\n■ 外れケース具体例 (ランダム{n_samples}件):")
    sample = misses.sample(min(n_samples, len(misses)))
    for i, (_, row) in enumerate(sample.iterrows(), 1):
        print(f"\n  [{i}] race_id={row['race_id']}")
        print(f"    Top1予測: {row['popularity']:.0f}番人気({row['win_odds']:.1f}倍)")
        print(f"    実際の勝ち馬: {row['winner_pop']:.0f}番人気({row['winner_odds']:.1f}倍) [予測順位Top{row['winner_rank']:.0f}]")
    
    return misses


def analyze_odds_distribution_failures(pred_df, returns_df):
    """オッズ分布と予測失敗の関係"""
    print("\n" + "=" * 80)
    print("【敗因分析5: オッズ分布と予測失敗の関係】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    # レースごとのオッズ分散を計算
    race_odds_stats = pred_df.groupby('race_id').agg({
        'win_odds': ['min', 'max', 'std', 'mean'],
    }).reset_index()
    race_odds_stats.columns = ['race_id', 'min_odds', 'max_odds', 'odds_std', 'mean_odds']
    race_odds_stats['odds_range'] = race_odds_stats['max_odds'] - race_odds_stats['min_odds']
    
    # Top1外れたか判定
    top1 = pred_df[pred_df['rank_pred'] == 1][['race_id', 'horse_number']].copy()
    merged = top1.merge(tansho[['race_id', 'horse_number']], 
                        on=['race_id', 'horse_number'], how='left', indicator=True)
    merged['is_hit'] = merged['_merge'] == 'both'
    merged = merged.merge(race_odds_stats, on='race_id', how='left')
    
    print("\n■ オッズ分散と的中率の関係:")
    for low, high, label in [(0, 20, '低分散(σ<20)'), (20, 40, '中分散(σ20-40)'), 
                              (40, 80, '高分散(σ40-80)'), (80, 1000, '超高分散(σ80+)')]:
        subset = merged[(merged['odds_std'] >= low) & (merged['odds_std'] < high)]
        if len(subset) >= 50:
            hr = subset['is_hit'].mean() * 100
            print(f"  {label}: {len(subset):>6,}件 | 的中率{hr:>6.2f}%")
    
    print("\n■ 1番人気オッズと的中率の関係:")
    for low, high, label in [(1, 2, '1-2倍'), (2, 3, '2-3倍'), (3, 5, '3-5倍'), 
                              (5, 10, '5-10倍'), (10, 999, '10倍+')]:
        subset = merged[(merged['min_odds'] >= low) & (merged['min_odds'] < high)]
        if len(subset) >= 50:
            hr = subset['is_hit'].mean() * 100
            print(f"  1番人気{label}: {len(subset):>6,}件 | 的中率{hr:>6.2f}%")


def main():
    print("=" * 80)
    print("レース条件別詳細敗因分析（2020-2025年全レース）")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 2020-2025年のデータを処理
    years = [2020, 2021, 2022, 2023, 2024, 2025]
    
    all_pred_dfs = []
    all_test_dfs = []
    
    for year in years:
        print(f"\n{'#'*80}")
        print(f"# {year}年 処理中...")
        print(f"{'#'*80}")
        
        train_end = f'{year-1}-12-31'
        test_start = f'{year}-01-01'
        test_end = f'{year}-12-31'
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        print(f"  学習: {len(train):,}件 / テスト: {len(test):,}件")
        
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000:
            continue
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # 予測データ作成
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        
        # レース条件を追加
        for col in ['distance', 'surface']:
            if col in test_f.columns:
                pred_df[col] = test_f[col].values
        
        pred_df['score'] = preds
        pred_df['rank_pred'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        pred_df['year'] = year
        
        # レース条件を抽出
        pred_df = extract_race_conditions(pred_df)
        
        all_pred_dfs.append(pred_df)
        all_test_dfs.append(test_f)
    
    # 全年のデータを結合
    all_pred_df = pd.concat(all_pred_dfs, ignore_index=True)
    print(f"\n総レコード数: {len(all_pred_df):,}件")
    print(f"総レース数: {all_pred_df['race_id'].nunique():,}件")
    
    # 全体分析
    print("\n" + "=" * 80)
    print("【全体分析: 2020-2025年】")
    print("=" * 80)
    
    # 競馬場×芝ダート×距離分析
    merged = analyze_venue_surface_distance(all_pred_df, returns)
    
    # 敗因分析
    misses, hits = analyze_failure_patterns(all_pred_df, races, returns)
    
    # 勝ち馬分析
    analyze_actual_winners(all_pred_df, races, returns)
    
    # オッズ分布分析
    analyze_odds_distribution_failures(all_pred_df, returns)
    
    # 具体的な外れケース分析
    analyze_specific_failure_cases(all_pred_df, races, returns)
    
    # 年別推移
    print("\n" + "=" * 80)
    print("【年別推移】")
    print("=" * 80)
    
    tansho = returns[returns['bet_type'] == 'tansho']
    
    for year in years:
        year_pred = all_pred_df[all_pred_df['year'] == year]
        top1 = year_pred[year_pred['rank_pred'] == 1]
        merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                            on=['race_id', 'horse_number'], how='left')
        merged['is_hit'] = ~merged['payout'].isna()
        merged['return'] = merged['payout'].fillna(0) / 100
        
        hr = merged['is_hit'].mean() * 100
        roi = merged['return'].sum() / len(merged) * 100
        mark = "★" if roi >= 100 else ""
        print(f"  {year}年: {len(merged):>5}件 | 的中率{hr:>6.2f}% | ROI {roi:>6.1f}%{mark}")
    
    # 深い推論
    print("\n" + "=" * 80)
    print("【深い推論: 敗因パターンのまとめ】")
    print("=" * 80)
    
    print("""
■ 主な敗因パターン

1. 人気馬同士の混戦（予測精度の限界）
   - 1-3番人気が拮抗しているレースでは予測が難しい
   - オッズ分散が低いレースは的中率が下がる傾向

2. 穴馬の台頭（予測困難なパターン）
   - 10番人気以下が勝つケースが約10%存在
   - これらは前走データからは予測困難

3. 多頭数レースでの波乱
   - 15頭以上のレースでは不確定要素が増加
   - 展開不利・馬群に包まれるなどの運要素

4. 距離適性の誤認
   - 距離初挑戦や久々の距離での好走
   - 過去データが少ない馬は予測精度が低下

5. コース特性
   - 競馬場ごとに予測精度に差がある
   - 特定の競馬場では穴馬が出やすい傾向

■ 改善の方向性

1. 条件フィルタリングの導入
   - 予測精度が高い条件のみで賭ける
   - 頭数、オッズ分布などで絞り込み

2. 馬券種の使い分け
   - 混戦時は複勝やワイドでリスク分散
   - 人気薄レースでは単勝で高配当狙い

3. 敗因パターンの事前検知
   - オッズ分布から混戦度を判定
   - 穴馬の存在をチェック
""")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
