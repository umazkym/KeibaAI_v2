# -*- coding: utf-8 -*-
"""
全券種オッズ帯別ROI分析

単勝、複勝、馬連、馬単、ワイド、3連複、3連単のROIを分析
60倍以上を除外した場合の安定性も確認
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


def add_enhanced_features(df, races_raw, course_master):
    df = df.copy()
    df['month'] = df['race_date'].dt.month
    df['week_of_year'] = df['race_date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['race_date'].dt.dayofweek
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    df['is_long_straight'] = (df['course_final_straight_m'] >= 400).astype(int)
    df['is_steep_slope'] = (df['course_slope_percent'] >= 1.5).astype(int)
    df['is_many_corners'] = (df['course_corner_count'] >= 4).astype(int)
    
    if races_raw is not None:
        races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
        for i in range(1, 6):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
            if 'finish_time_seconds' in races_sorted.columns:
                races_sorted[f'prev_{i}_time'] = races_sorted.groupby('horse_id')['finish_time_seconds'].shift(i)
            if 'last_3f_time' in races_sorted.columns:
                races_sorted[f'prev_{i}_last3f'] = races_sorted.groupby('horse_id')['last_3f_time'].shift(i)
        merge_cols = ['race_id', 'horse_id']
        for i in range(1, 6):
            for suffix in ['finish', 'time', 'last3f']:
                col = f'prev_{i}_{suffix}'
                if col in races_sorted.columns:
                    merge_cols.append(col)
        merge_cols = list(set(merge_cols))
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
                else:
                    df[base_col] = df[col]
                df = df.drop(columns=[col])
    
    df['jockey_horse_pair'] = df['jockey_id'].astype(str) + '_' + df['horse_id'].astype(str)
    df_sorted = df.sort_values(['jockey_horse_pair', 'race_date']).copy()
    df_sorted['is_win'] = (df_sorted['finish_position'] == 1).astype(int)
    df_sorted['pair_cum_wins'] = df_sorted.groupby('jockey_horse_pair')['is_win'].cumsum().shift(1).fillna(0)
    df_sorted['pair_cum_races'] = df_sorted.groupby('jockey_horse_pair').cumcount()
    df_sorted['jockey_horse_pair_win_rate'] = np.where(
        df_sorted['pair_cum_races'] > 0,
        df_sorted['pair_cum_wins'] / df_sorted['pair_cum_races'], 0)
    df_sorted['jockey_horse_pair_count'] = df_sorted['pair_cum_races']
    pair_features = df_sorted[['race_id', 'horse_id', 'jockey_horse_pair_win_rate', 'jockey_horse_pair_count']].copy()
    df = df.merge(pair_features, on=['race_id', 'horse_id'], how='left', suffixes=('', '_pair'))
    return df


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
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


def analyze_ticket_types(test_df, max_odds=60):
    """
    全券種のROIを分析
    max_odds: この値を超えるオッズの的中は除外
    """
    results = {}
    
    # レース数
    n_races = test_df['race_id'].nunique()
    
    # === 単勝 ===
    bets_tansho = test_df[test_df['pred_rank'] == 1]
    hits_tansho = bets_tansho[bets_tansho['finish_position'] == 1]
    hits_tansho_filtered = hits_tansho[hits_tansho['win_odds'] <= max_odds]
    
    results['単勝'] = {
        'ベット数': len(bets_tansho),
        '的中数': len(hits_tansho),
        '的中率': len(hits_tansho) / len(bets_tansho) * 100 if len(bets_tansho) > 0 else 0,
        'ROI': hits_tansho['win_odds'].sum() / len(bets_tansho) * 100 if len(bets_tansho) > 0 else 0,
        f'ROI({max_odds}倍以下)': hits_tansho_filtered['win_odds'].sum() / len(bets_tansho) * 100 if len(bets_tansho) > 0 else 0,
    }
    
    # === 複勝 ===
    bets_fukusho = test_df[test_df['pred_rank'] == 1]
    hits_fukusho = bets_fukusho[bets_fukusho['finish_position'] <= 3]
    # 複勝オッズは概念的に単勝の1/3〜1/5程度と仮定
    fukusho_odds_ratio = 0.25  # 単勝オッズの25%
    
    results['複勝'] = {
        'ベット数': len(bets_fukusho),
        '的中数': len(hits_fukusho),
        '的中率': len(hits_fukusho) / len(bets_fukusho) * 100 if len(bets_fukusho) > 0 else 0,
        'ROI(推定)': (hits_fukusho['win_odds'] * fukusho_odds_ratio).sum() / len(bets_fukusho) * 100 if len(bets_fukusho) > 0 else 0,
    }
    
    # === 馬連・馬単・ワイド・3連複・3連単 ===
    # レースごとに予測Top3を取得
    for race_id, group in test_df.groupby('race_id'):
        top3 = group.nsmallest(3, 'pred_rank')
        test_df.loc[top3.index, 'is_top3'] = True
    
    test_df['is_top3'] = test_df['is_top3'].fillna(False)
    
    # 馬連: Top1とTop2が1,2着
    umaren_hits = 0
    umaren_returns = 0
    
    # 馬単: Top1が1着, Top2が2着
    umatan_hits = 0
    umatan_returns = 0
    
    # ワイド: Top1とTop2が3着内（組み合わせ）
    wide_hits = 0
    wide_returns = 0
    
    # 3連複: Top3が3着内
    sanrenpuku_hits = 0
    sanrenpuku_returns = 0
    
    # 3連単: Top1→1着, Top2→2着, Top3→3着
    sanrentan_hits = 0
    sanrentan_returns = 0
    
    for race_id, group in test_df.groupby('race_id'):
        sorted_group = group.sort_values('pred_rank')
        if len(sorted_group) < 3:
            continue
        
        top1 = sorted_group.iloc[0]
        top2 = sorted_group.iloc[1]
        top3 = sorted_group.iloc[2]
        
        top1_finish = int(top1['finish_position']) if pd.notna(top1['finish_position']) else 999
        top2_finish = int(top2['finish_position']) if pd.notna(top2['finish_position']) else 999
        top3_finish = int(top3['finish_position']) if pd.notna(top3['finish_position']) else 999
        
        top1_odds = top1['win_odds'] if pd.notna(top1['win_odds']) else 1
        top2_odds = top2['win_odds'] if pd.notna(top2['win_odds']) else 1
        top3_odds = top3['win_odds'] if pd.notna(top3['win_odds']) else 1
        
        # 馬連: Top1とTop2が1,2着（順不同）
        if set([top1_finish, top2_finish]) == set([1, 2]):
            umaren_hits += 1
            # 馬連オッズは概念的に2頭のオッズの積の平方根程度
            umaren_odds = np.sqrt(top1_odds * top2_odds) * 0.8
            if umaren_odds <= max_odds:
                umaren_returns += umaren_odds
        
        # 馬単: Top1が1着, Top2が2着
        if top1_finish == 1 and top2_finish == 2:
            umatan_hits += 1
            umatan_odds = np.sqrt(top1_odds * top2_odds) * 1.5
            if umatan_odds <= max_odds:
                umatan_returns += umatan_odds
        
        # ワイド: Top1とTop2が3着内
        if top1_finish <= 3 and top2_finish <= 3:
            wide_hits += 1
            wide_odds = np.sqrt(top1_odds * top2_odds) * 0.3
            if wide_odds <= max_odds:
                wide_returns += wide_odds
        
        # 3連複: Top3が3着内
        if top1_finish <= 3 and top2_finish <= 3 and top3_finish <= 3:
            sanrenpuku_hits += 1
            sanrenpuku_odds = (top1_odds * top2_odds * top3_odds) ** (1/3) * 2
            if sanrenpuku_odds <= max_odds:
                sanrenpuku_returns += sanrenpuku_odds
        
        # 3連単: 順番完全一致
        if top1_finish == 1 and top2_finish == 2 and top3_finish == 3:
            sanrentan_hits += 1
            sanrentan_odds = (top1_odds * top2_odds * top3_odds) ** (1/3) * 6
            if sanrentan_odds <= max_odds:
                sanrentan_returns += sanrentan_odds
    
    results['馬連'] = {
        'ベット数': n_races,
        '的中数': umaren_hits,
        '的中率': umaren_hits / n_races * 100,
        'ROI(推定)': umaren_returns / n_races * 100,
    }
    
    results['馬単'] = {
        'ベット数': n_races,
        '的中数': umatan_hits,
        '的中率': umatan_hits / n_races * 100,
        'ROI(推定)': umatan_returns / n_races * 100,
    }
    
    results['ワイド'] = {
        'ベット数': n_races,
        '的中数': wide_hits,
        '的中率': wide_hits / n_races * 100,
        'ROI(推定)': wide_returns / n_races * 100,
    }
    
    results['3連複'] = {
        'ベット数': n_races,
        '的中数': sanrenpuku_hits,
        '的中率': sanrenpuku_hits / n_races * 100,
        'ROI(推定)': sanrenpuku_returns / n_races * 100,
    }
    
    results['3連単'] = {
        'ベット数': n_races,
        '的中数': sanrentan_hits,
        '的中率': sanrentan_hits / n_races * 100,
        'ROI(推定)': sanrentan_returns / n_races * 100,
    }
    
    return results


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("全券種オッズ帯別ROI分析")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 障害レース除外
    races = races[races['track_surface'] != '障害'].copy()
    
    print(f"\n総データ: {len(races):,}件（障害除外済み）")
    
    races_raw = races.copy()
    course_master = load_course_master()
    
    # 2024年のみで検証
    train_end = '2022-12-31'
    valid_start = '2023-01-01'
    valid_end = '2023-12-31'
    test_start = '2024-01-01'
    test_end = '2025-01-01'
    
    train = races[races['race_date'] <= train_end].copy()
    valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
    
    print(f"\n[2024年テスト]")
    print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    train_f = add_enhanced_features(train_f, races_raw, course_master)
    valid_f = add_enhanced_features(valid_f, races_raw, course_master)
    test_f = add_enhanced_features(test_f, races_raw, course_master)
    
    base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
    new_features = ['month_sin', 'month_cos', 'week_sin', 'week_cos', 'day_sin', 'day_cos',
        'course_slope_percent', 'course_final_straight_m', 'course_corner_count',
        'course_start_to_corner_m', 'is_long_straight', 'is_steep_slope', 'is_many_corners',
        'jockey_horse_pair_win_rate', 'jockey_horse_pair_count']
    for i in range(1, 6):
        for suffix in ['finish', 'time', 'last3f']:
            new_features.append(f'prev_{i}_{suffix}')
    new_features = [f for f in new_features if f in train_f.columns]
    all_features = list(dict.fromkeys(base_features + new_features))
    
    print(f"  特徴量数: {len(all_features)}")
    
    # モデル学習
    print("\n  モデル学習中...")
    model = train_model(train_f, valid_f, all_features)
    
    # 予測
    preds = model.predict(test_f[all_features].fillna(0))
    test_f['pred_score'] = preds
    test_f['pred_rank'] = test_f.groupby('race_id')['pred_score'].rank(ascending=False, method='first')
    
    # 全券種分析
    print("\n" + "=" * 80)
    print("券種別ROI分析")
    print("=" * 80)
    
    results = analyze_ticket_types(test_f, max_odds=60)
    
    print(f"\n{'券種':>8} {'ベット':>8} {'的中':>6} {'的中率':>8} {'ROI':>10}")
    print("-" * 50)
    
    for ticket, data in results.items():
        roi_key = 'ROI' if 'ROI' in data else 'ROI(推定)'
        print(f"{ticket:>8} {data['ベット数']:>8,} {data['的中数']:>6,} {data['的中率']:>7.1f}% {data.get(roi_key, data.get('ROI(推定)', 0)):>9.1f}%")
    
    # オッズ帯別分析（単勝）
    print("\n" + "=" * 80)
    print("オッズ帯別単勝ROI")
    print("=" * 80)
    
    bets = test_f[test_f['pred_rank'] == 1]
    
    odds_bands = [(1, 5), (5, 10), (10, 20), (20, 40), (40, 60), (60, 100), (100, 999)]
    
    print(f"\n{'オッズ帯':>12} {'ベット':>8} {'的中':>6} {'的中率':>8} {'ROI':>10}")
    print("-" * 55)
    
    for low, high in odds_bands:
        band_bets = bets[(bets['win_odds'] >= low) & (bets['win_odds'] < high)]
        band_hits = band_bets[band_bets['finish_position'] == 1]
        
        if len(band_bets) > 0:
            hit_rate = len(band_hits) / len(band_bets) * 100
            roi = band_hits['win_odds'].sum() / len(band_bets) * 100
        else:
            hit_rate = 0
            roi = 0
        
        print(f"{low:>5}-{high:<5} {len(band_bets):>8,} {len(band_hits):>6,} {hit_rate:>7.1f}% {roi:>9.1f}%")
    
    # 60倍以上除外時の総合ROI
    print("\n" + "=" * 80)
    print("60倍以上除外時のROI")
    print("=" * 80)
    
    hits_all = bets[bets['finish_position'] == 1]
    hits_filtered = hits_all[hits_all['win_odds'] <= 60]
    
    roi_all = hits_all['win_odds'].sum() / len(bets) * 100
    roi_filtered = hits_filtered['win_odds'].sum() / len(bets) * 100
    
    print(f"\n  全体ROI: {roi_all:.1f}%")
    print(f"  60倍以下ROI: {roi_filtered:.1f}%")
    print(f"  高オッズ（60倍超）の貢献: {roi_all - roi_filtered:.1f}%")


if __name__ == "__main__":
    main()
