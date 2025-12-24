# -*- coding: utf-8 -*-
"""
期待値ベース全券種戦略

各券種で全組み合わせの期待値を計算し、最大期待値の組み合わせを選択
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import yaml
from itertools import combinations, permutations
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
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m', 
                'course_corner_count', 'course_start_to_corner_m']:
        df[col] = [x.get(col, 0) for x in course_features_list]
    
    if races_raw is not None:
        races_sorted = races_raw.sort_values(['horse_id', 'race_date']).copy()
        for i in range(1, 4):
            races_sorted[f'prev_{i}_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(i)
        merge_cols = ['race_id', 'horse_id'] + [f'prev_{i}_finish' for i in range(1, 4)]
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


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def expected_value_strategy(race_df, max_odds=60):
    """
    各券種で期待値最大の組み合わせを選択
    """
    results = {
        'tansho': {'bet': None, 'ev': 0},
        'fukusho': {'bet': None, 'ev': 0},
        'umaren': {'bet': None, 'ev': 0},
        'umatan': {'bet': None, 'ev': 0},
        'wide': {'bet': None, 'ev': 0},
        'sanrenpuku': {'bet': None, 'ev': 0},
        'sanrentan': {'bet': None, 'ev': 0},
    }
    
    horses = race_df.sort_values('pred_score', ascending=False)
    
    # 予測スコアを確率に変換（正規化）
    scores = horses['pred_score'].values
    probs = sigmoid(scores - scores.mean())
    probs = probs / probs.sum()  # 確率として正規化
    horses = horses.copy()
    horses['prob'] = probs
    
    # === 単勝 ===
    for _, h in horses.iterrows():
        odds = h['win_odds'] if pd.notna(h['win_odds']) else 1
        if odds > max_odds:
            continue
        ev = h['prob'] * odds
        if ev > results['tansho']['ev']:
            results['tansho'] = {'bet': h['horse_id'], 'ev': ev, 'odds': odds, 'prob': h['prob']}
    
    # === 複勝（Top3内の確率を使用）===
    top3_probs = horses.head(3)['prob'].sum()
    for _, h in horses.head(5).iterrows():
        odds = h['win_odds'] * 0.25 if pd.notna(h['win_odds']) else 1
        ev = h['prob'] * 3 * odds  # 3着内なので確率3倍と仮定
        if ev > results['fukusho']['ev']:
            results['fukusho'] = {'bet': h['horse_id'], 'ev': ev}
    
    # === 馬連（2頭の組み合わせ）===
    top_horses = horses.head(6)  # 上位6頭で検討
    for h1, h2 in combinations(top_horses.itertuples(), 2):
        odds1 = h1.win_odds if pd.notna(h1.win_odds) else 1
        odds2 = h2.win_odds if pd.notna(h2.win_odds) else 1
        est_odds = np.sqrt(odds1 * odds2) * 0.8
        if est_odds > max_odds:
            continue
        # 2頭が1-2着になる確率（独立仮定）
        prob = h1.prob * h2.prob * 2  # 順不同なので2倍
        ev = prob * est_odds
        if ev > results['umaren']['ev']:
            results['umaren'] = {'bet': (h1.horse_id, h2.horse_id), 'ev': ev, 'odds': est_odds}
    
    # === 馬単（順番あり）===
    for h1 in top_horses.itertuples():
        for h2 in top_horses.itertuples():
            if h1.horse_id == h2.horse_id:
                continue
            odds1 = h1.win_odds if pd.notna(h1.win_odds) else 1
            odds2 = h2.win_odds if pd.notna(h2.win_odds) else 1
            est_odds = np.sqrt(odds1 * odds2) * 1.5
            if est_odds > max_odds:
                continue
            prob = h1.prob * h2.prob
            ev = prob * est_odds
            if ev > results['umatan']['ev']:
                results['umatan'] = {'bet': (h1.horse_id, h2.horse_id), 'ev': ev, 'odds': est_odds}
    
    # === ワイド ===
    for h1, h2 in combinations(top_horses.itertuples(), 2):
        odds1 = h1.win_odds if pd.notna(h1.win_odds) else 1
        odds2 = h2.win_odds if pd.notna(h2.win_odds) else 1
        est_odds = np.sqrt(odds1 * odds2) * 0.3
        if est_odds > max_odds:
            continue
        prob = h1.prob * h2.prob * 6  # 3着内の組み合わせなので確率高め
        ev = prob * est_odds
        if ev > results['wide']['ev']:
            results['wide'] = {'bet': (h1.horse_id, h2.horse_id), 'ev': ev, 'odds': est_odds}
    
    # === 3連複（3頭の組み合わせ）===
    for h1, h2, h3 in combinations(top_horses.itertuples(), 3):
        odds1 = h1.win_odds if pd.notna(h1.win_odds) else 1
        odds2 = h2.win_odds if pd.notna(h2.win_odds) else 1
        odds3 = h3.win_odds if pd.notna(h3.win_odds) else 1
        est_odds = (odds1 * odds2 * odds3) ** (1/3) * 2
        if est_odds > max_odds:
            continue
        prob = h1.prob * h2.prob * h3.prob * 6  # 順不同
        ev = prob * est_odds
        if ev > results['sanrenpuku']['ev']:
            results['sanrenpuku'] = {'bet': (h1.horse_id, h2.horse_id, h3.horse_id), 'ev': ev, 'odds': est_odds}
    
    # === 3連単（順番あり）===
    for h1, h2, h3 in permutations(top_horses.head(4).itertuples(), 3):
        odds1 = h1.win_odds if pd.notna(h1.win_odds) else 1
        odds2 = h2.win_odds if pd.notna(h2.win_odds) else 1
        odds3 = h3.win_odds if pd.notna(h3.win_odds) else 1
        est_odds = (odds1 * odds2 * odds3) ** (1/3) * 6
        if est_odds > max_odds:
            continue
        prob = h1.prob * h2.prob * h3.prob
        ev = prob * est_odds
        if ev > results['sanrentan']['ev']:
            results['sanrentan'] = {'bet': (h1.horse_id, h2.horse_id, h3.horse_id), 'ev': ev, 'odds': est_odds}
    
    return results


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("期待値ベース全券種戦略")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['track_surface'] != '障害'].copy()
    
    print(f"\n総データ: {len(races):,}件")
    
    races_raw = races.copy()
    course_master = load_course_master()
    
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
    
    all_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    print(f"  特徴量数: {len(all_features)}")
    print("\n  モデル学習中...")
    model = train_model(train_f, valid_f, all_features)
    
    preds = model.predict(test_f[all_features].fillna(0))
    test_f['pred_score'] = preds
    
    # 期待値戦略で各レースを評価
    print("\n  期待値戦略適用中...")
    
    stats = {
        'tansho': {'bets': 0, 'hits': 0, 'returns': 0},
        'fukusho': {'bets': 0, 'hits': 0, 'returns': 0},
        'umaren': {'bets': 0, 'hits': 0, 'returns': 0},
        'umatan': {'bets': 0, 'hits': 0, 'returns': 0},
        'wide': {'bets': 0, 'hits': 0, 'returns': 0},
        'sanrenpuku': {'bets': 0, 'hits': 0, 'returns': 0},
        'sanrentan': {'bets': 0, 'hits': 0, 'returns': 0},
    }
    
    for race_id, race_df in test_f.groupby('race_id'):
        if len(race_df) < 3:
            continue
        
        ev_bets = expected_value_strategy(race_df, max_odds=60)
        
        # 実際の着順を取得
        actual = race_df.sort_values('finish_position')
        if len(actual) < 3:
            continue
        
        actual_1st = actual.iloc[0]['horse_id']
        actual_2nd = actual.iloc[1]['horse_id']
        actual_3rd = actual.iloc[2]['horse_id']
        actual_top3 = set([actual_1st, actual_2nd, actual_3rd])
        
        # 単勝
        if ev_bets['tansho']['bet'] is not None and ev_bets['tansho']['ev'] > 1.0:
            stats['tansho']['bets'] += 1
            if ev_bets['tansho']['bet'] == actual_1st:
                stats['tansho']['hits'] += 1
                stats['tansho']['returns'] += ev_bets['tansho']['odds']
        
        # 複勝
        if ev_bets['fukusho']['bet'] is not None:
            stats['fukusho']['bets'] += 1
            if ev_bets['fukusho']['bet'] in actual_top3:
                stats['fukusho']['hits'] += 1
                stats['fukusho']['returns'] += 1.5  # 複勝平均1.5倍と仮定
        
        # 馬連
        if ev_bets['umaren']['bet'] is not None and ev_bets['umaren']['ev'] > 0.5:
            stats['umaren']['bets'] += 1
            bet_set = set(ev_bets['umaren']['bet'])
            actual_top2 = set([actual_1st, actual_2nd])
            if bet_set == actual_top2:
                stats['umaren']['hits'] += 1
                stats['umaren']['returns'] += ev_bets['umaren']['odds']
        
        # 馬単
        if ev_bets['umatan']['bet'] is not None and ev_bets['umatan']['ev'] > 0.5:
            stats['umatan']['bets'] += 1
            if ev_bets['umatan']['bet'][0] == actual_1st and ev_bets['umatan']['bet'][1] == actual_2nd:
                stats['umatan']['hits'] += 1
                stats['umatan']['returns'] += ev_bets['umatan']['odds']
        
        # ワイド
        if ev_bets['wide']['bet'] is not None:
            stats['wide']['bets'] += 1
            bet_set = set(ev_bets['wide']['bet'])
            if bet_set.issubset(actual_top3):
                stats['wide']['hits'] += 1
                stats['wide']['returns'] += ev_bets['wide']['odds']
        
        # 3連複
        if ev_bets['sanrenpuku']['bet'] is not None and ev_bets['sanrenpuku']['ev'] > 0.3:
            stats['sanrenpuku']['bets'] += 1
            bet_set = set(ev_bets['sanrenpuku']['bet'])
            if bet_set == actual_top3:
                stats['sanrenpuku']['hits'] += 1
                stats['sanrenpuku']['returns'] += ev_bets['sanrenpuku']['odds']
        
        # 3連単
        if ev_bets['sanrentan']['bet'] is not None and ev_bets['sanrentan']['ev'] > 0.2:
            stats['sanrentan']['bets'] += 1
            bet = ev_bets['sanrentan']['bet']
            if bet[0] == actual_1st and bet[1] == actual_2nd and bet[2] == actual_3rd:
                stats['sanrentan']['hits'] += 1
                stats['sanrentan']['returns'] += ev_bets['sanrentan']['odds']
    
    # 結果表示
    print("\n" + "=" * 80)
    print("期待値戦略 vs 従来戦略（2024年）")
    print("=" * 80)
    
    ticket_names = {
        'tansho': '単勝', 'fukusho': '複勝', 'umaren': '馬連',
        'umatan': '馬単', 'wide': 'ワイド', 'sanrenpuku': '3連複', 'sanrentan': '3連単'
    }
    
    print(f"\n{'券種':>8} {'ベット':>8} {'的中':>6} {'的中率':>8} {'ROI':>10}")
    print("-" * 50)
    
    for key, name in ticket_names.items():
        s = stats[key]
        if s['bets'] > 0:
            hit_rate = s['hits'] / s['bets'] * 100
            roi = s['returns'] / s['bets'] * 100
        else:
            hit_rate = 0
            roi = 0
        print(f"{name:>8} {s['bets']:>8,} {s['hits']:>6,} {hit_rate:>7.1f}% {roi:>9.1f}%")


if __name__ == "__main__":
    main()
