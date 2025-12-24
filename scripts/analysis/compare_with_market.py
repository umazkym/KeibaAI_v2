# -*- coding: utf-8 -*-
"""
市場（人気順）との比較分析

AIモデルの本質的な価値を検証
- 人気通りに買った場合との的中率・回収率の比較
- 予測1-3位 vs 人気1-3位
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
        return {'course_slope_percent': 0, 'course_final_straight_m': 300}
    venue_data = course_master.get(venue, {})
    surface_data = venue_data.get(surface, {})
    try:
        dist = int(distance_m)
    except:
        return {'course_slope_percent': 0, 'course_final_straight_m': 300}
    distance_data = surface_data.get(dist, {})
    if not distance_data:
        return {'course_slope_percent': 0, 'course_final_straight_m': 300}
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
    }


def add_features(df, races_raw, course_master):
    df = df.copy()
    df['month'] = df['race_date'].dt.month
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


def train_v15(train_df, valid_df, feature_cols):
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


def train_v44(train_df, valid_df, feature_cols):
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    for d in [train_df, valid_df]:
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train, 
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def normalize(preds):
    return (preds - preds.min()) / (preds.max() - preds.min() + 1e-8)


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 100)
    print("市場（人気順）との比較分析")
    print("AIモデルの本質的な価値を検証")
    print("=" * 100)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
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
    
    print(f"\n[2024年テスト] Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    train_f = add_features(train_f, races_raw, course_master)
    valid_f = add_features(valid_f, races_raw, course_master)
    test_f = add_features(test_f, races_raw, course_master)
    
    all_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    print(f"  特徴量数: {len(all_features)}")
    
    print("\n  モデル学習中...")
    model_v15 = train_v15(train_f, valid_f, all_features)
    model_v44 = train_v44(train_f.copy(), valid_f.copy(), all_features)
    
    X_test = test_f[all_features].fillna(0)
    preds_v15 = model_v15.predict(X_test)
    preds_v44 = model_v44.predict(X_test)
    preds_ensemble = (normalize(preds_v15) + normalize(preds_v44)) / 2
    
    test_f['score_ensemble'] = preds_ensemble
    test_f['rank_ensemble'] = test_f.groupby('race_id')['score_ensemble'].rank(ascending=False, method='first')
    
    n_races = test_f['race_id'].nunique()
    
    # =========================================================
    # 1. 単勝比較（AI予測1位 vs 人気1位）
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【1. 単勝比較】")
    print(f"=" * 100)
    
    # 人気1位
    pop1 = test_f[test_f['popularity'] == 1]
    pop1_hits = pop1[pop1['finish_position'] == 1]
    pop1_hit_rate = len(pop1_hits) / len(pop1) * 100
    pop1_roi = pop1_hits['win_odds'].sum() / len(pop1) * 100
    
    # AI予測1位
    ai1 = test_f[test_f['rank_ensemble'] == 1]
    ai1_hits = ai1[ai1['finish_position'] == 1]
    ai1_hit_rate = len(ai1_hits) / len(ai1) * 100
    ai1_roi = ai1_hits['win_odds'].sum() / len(ai1) * 100
    
    print(f"\n{'指標':>15} {'人気1位':>12} {'AI予測1位':>12} {'差分':>10}")
    print("-" * 55)
    print(f"{'ベット数':>15} {len(pop1):>12} {len(ai1):>12}")
    print(f"{'的中数':>15} {len(pop1_hits):>12} {len(ai1_hits):>12} {len(ai1_hits)-len(pop1_hits):>+10}")
    print(f"{'的中率':>15} {pop1_hit_rate:>11.1f}% {ai1_hit_rate:>11.1f}% {ai1_hit_rate-pop1_hit_rate:>+9.1f}%")
    print(f"{'ROI':>15} {pop1_roi:>11.1f}% {ai1_roi:>11.1f}% {ai1_roi-pop1_roi:>+9.1f}%")
    
    # =========================================================
    # 2. 3着内比較（AI予測1位 vs 人気1位）
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【2. 3着内率比較】")
    print(f"=" * 100)
    
    pop1_top3 = (pop1['finish_position'] <= 3).mean() * 100
    ai1_top3 = (ai1['finish_position'] <= 3).mean() * 100
    
    print(f"\n{'指標':>15} {'人気1位':>12} {'AI予測1位':>12} {'差分':>10}")
    print("-" * 55)
    print(f"{'3着内率':>15} {pop1_top3:>11.1f}% {ai1_top3:>11.1f}% {ai1_top3-pop1_top3:>+9.1f}%")
    
    # =========================================================
    # 3. Top3比較（AI予測1-3位 vs 人気1-3位）
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【3. Top3比較】")
    print(f"=" * 100)
    
    for rank in [1, 2, 3]:
        pop_rank = test_f[test_f['popularity'] == rank]
        ai_rank = test_f[test_f['rank_ensemble'] == rank]
        
        pop_hit1 = (pop_rank['finish_position'] == 1).mean() * 100
        ai_hit1 = (ai_rank['finish_position'] == 1).mean() * 100
        
        pop_top3 = (pop_rank['finish_position'] <= 3).mean() * 100
        ai_top3 = (ai_rank['finish_position'] <= 3).mean() * 100
        
        pop_roi = pop_rank[pop_rank['finish_position'] == 1]['win_odds'].sum() / len(pop_rank) * 100
        ai_roi = ai_rank[ai_rank['finish_position'] == 1]['win_odds'].sum() / len(ai_rank) * 100
        
        print(f"\n--- {rank}位 比較 ---")
        print(f"{'指標':>15} {'人気{rank}位':>12} {'AI予測{rank}位':>12} {'差分':>10}")
        print(f"{'1着的中率':>15} {pop_hit1:>11.1f}% {ai_hit1:>11.1f}% {ai_hit1-pop_hit1:>+9.1f}%")
        print(f"{'3着内率':>15} {pop_top3:>11.1f}% {ai_top3:>11.1f}% {ai_top3-pop_top3:>+9.1f}%")
        print(f"{'単勝ROI':>15} {pop_roi:>11.1f}% {ai_roi:>11.1f}% {ai_roi-pop_roi:>+9.1f}%")
    
    # =========================================================
    # 4. 人気超え率比較
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【4. 人気超え率比較】")
    print(f"=" * 100)
    
    # AI予測が人気を超える率
    for rank in [1, 2, 3]:
        ai_rank = test_f[test_f['rank_ensemble'] == rank]
        beat_pop = (ai_rank['finish_position'] < ai_rank['popularity']).mean() * 100
        beat_pop_top3 = ((ai_rank['finish_position'] < ai_rank['popularity']) & (ai_rank['finish_position'] <= 3)).mean() * 100
        
        print(f"  AI予測{rank}位: 人気超え {beat_pop:.1f}%, 人気超え×3着内 {beat_pop_top3:.1f}%")
    
    # =========================================================
    # 5. AIと人気が異なる馬を選んだ時
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【5. AIと人気が異なる馬を選んだ時】")
    print(f"=" * 100)
    
    # AI予測1位 ≠ 人気1位のケース
    ai1_not_pop1 = ai1[ai1['popularity'] != 1]
    ai1_not_pop1_hits = ai1_not_pop1[ai1_not_pop1['finish_position'] == 1]
    ai1_not_pop1_top3 = ai1_not_pop1[ai1_not_pop1['finish_position'] <= 3]
    
    print(f"\n--- AI予測1位 ≠ 人気1位 の場合 ---")
    print(f"  件数: {len(ai1_not_pop1):,} ({len(ai1_not_pop1)/len(ai1)*100:.1f}%)")
    print(f"  1着的中率: {len(ai1_not_pop1_hits)/len(ai1_not_pop1)*100:.1f}%")
    print(f"  3着内率: {len(ai1_not_pop1_top3)/len(ai1_not_pop1)*100:.1f}%")
    if len(ai1_not_pop1) > 0:
        roi = ai1_not_pop1_hits['win_odds'].sum() / len(ai1_not_pop1) * 100
        print(f"  単勝ROI: {roi:.1f}%")
    
    # =========================================================
    # 6. 複勝比較
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【6. 複勝比較（推定）】")
    print(f"=" * 100)
    
    print(f"\n{'順位':>10} {'人気の3着内率':>15} {'AIの3着内率':>15} {'差分':>10}")
    print("-" * 55)
    
    for rank in [1, 2, 3]:
        pop_rank = test_f[test_f['popularity'] == rank]
        ai_rank = test_f[test_f['rank_ensemble'] == rank]
        
        pop_top3 = (pop_rank['finish_position'] <= 3).mean() * 100
        ai_top3 = (ai_rank['finish_position'] <= 3).mean() * 100
        
        print(f"{rank:>10}位 {pop_top3:>14.1f}% {ai_top3:>14.1f}% {ai_top3-pop_top3:>+9.1f}%")
    
    # 推定複勝ROI
    print(f"\n--- 推定複勝ROI（単勝オッズ×0.27で推定）---")
    for rank in [1, 2, 3]:
        pop_rank = test_f[test_f['popularity'] == rank]
        ai_rank = test_f[test_f['rank_ensemble'] == rank]
        
        pop_hits = pop_rank[pop_rank['finish_position'] <= 3]
        ai_hits = ai_rank[ai_rank['finish_position'] <= 3]
        
        pop_roi = pop_hits['win_odds'].sum() * 0.27 / len(pop_rank) * 100
        ai_roi = ai_hits['win_odds'].sum() * 0.27 / len(ai_rank) * 100
        
        print(f"  {rank}位: 人気 {pop_roi:.1f}%, AI {ai_roi:.1f}%, 差分 {ai_roi-pop_roi:+.1f}%")
    
    # =========================================================
    # 7. ワイド比較
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【7. ワイド比較（1-2位の組み合わせ）】")
    print(f"=" * 100)
    
    # 人気1-2位のワイド
    pop_wide_hits = 0
    ai_wide_hits = 0
    pop_wide_returns = 0
    ai_wide_returns = 0
    n_bets = 0
    
    for race_id in test_f['race_id'].unique():
        race = test_f[test_f['race_id'] == race_id]
        if len(race) < 3:
            continue
        n_bets += 1
        
        # 人気1-2位
        pop_top2 = set(race[race['popularity'] <= 2]['horse_id'].values)
        # AI予測1-2位
        ai_top2 = set(race[race['rank_ensemble'] <= 2]['horse_id'].values)
        # 実際の3着内
        actual_top3 = set(race[race['finish_position'] <= 3]['horse_id'].values)
        
        # ワイド的中（両方が3着内）
        if len(pop_top2 & actual_top3) == 2:
            pop_wide_hits += 1
            odds1 = race[race['popularity'] == 1]['win_odds'].iloc[0] if 1 in race['popularity'].values else 1
            odds2 = race[race['popularity'] == 2]['win_odds'].iloc[0] if 2 in race['popularity'].values else 1
            pop_wide_returns += np.sqrt(odds1 * odds2) * 0.3
        
        if len(ai_top2 & actual_top3) == 2:
            ai_wide_hits += 1
            sorted_race = race.sort_values('rank_ensemble')
            odds1 = sorted_race.iloc[0]['win_odds'] if pd.notna(sorted_race.iloc[0]['win_odds']) else 1
            odds2 = sorted_race.iloc[1]['win_odds'] if pd.notna(sorted_race.iloc[1]['win_odds']) else 1
            ai_wide_returns += np.sqrt(odds1 * odds2) * 0.3
    
    print(f"\n{'指標':>15} {'人気1-2位':>12} {'AI予測1-2位':>12} {'差分':>10}")
    print("-" * 55)
    print(f"{'ベット数':>15} {n_bets:>12}")
    print(f"{'的中数':>15} {pop_wide_hits:>12} {ai_wide_hits:>12} {ai_wide_hits-pop_wide_hits:>+10}")
    print(f"{'的中率':>15} {pop_wide_hits/n_bets*100:>11.1f}% {ai_wide_hits/n_bets*100:>11.1f}% {(ai_wide_hits-pop_wide_hits)/n_bets*100:>+9.1f}%")
    print(f"{'推定ROI':>15} {pop_wide_returns/n_bets*100:>11.1f}% {ai_wide_returns/n_bets*100:>11.1f}% {(ai_wide_returns-pop_wide_returns)/n_bets*100:>+9.1f}%")
    
    # =========================================================
    # 8. 3連複比較
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【8. 3連複比較（1-2-3位の組み合わせ）】")
    print(f"=" * 100)
    
    pop_sanren_hits = 0
    ai_sanren_hits = 0
    pop_sanren_returns = 0
    ai_sanren_returns = 0
    n_bets = 0
    
    for race_id in test_f['race_id'].unique():
        race = test_f[test_f['race_id'] == race_id]
        if len(race) < 3:
            continue
        n_bets += 1
        
        pop_top3 = set(race[race['popularity'] <= 3]['horse_id'].values)
        ai_top3 = set(race[race['rank_ensemble'] <= 3]['horse_id'].values)
        actual_top3 = set(race[race['finish_position'] <= 3]['horse_id'].values)
        
        if pop_top3 == actual_top3:
            pop_sanren_hits += 1
            odds_list = race[race['popularity'] <= 3]['win_odds'].values
            pop_sanren_returns += np.prod(odds_list) ** (1/3) * 2
        
        if ai_top3 == actual_top3:
            ai_sanren_hits += 1
            sorted_race = race.sort_values('rank_ensemble')
            odds_list = sorted_race.head(3)['win_odds'].fillna(1).values
            ai_sanren_returns += np.prod(odds_list) ** (1/3) * 2
    
    print(f"\n{'指標':>15} {'人気1-2-3位':>12} {'AI予測1-2-3位':>12} {'差分':>10}")
    print("-" * 60)
    print(f"{'ベット数':>15} {n_bets:>12}")
    print(f"{'的中数':>15} {pop_sanren_hits:>12} {ai_sanren_hits:>12} {ai_sanren_hits-pop_sanren_hits:>+10}")
    print(f"{'的中率':>15} {pop_sanren_hits/n_bets*100:>11.1f}% {ai_sanren_hits/n_bets*100:>11.1f}% {(ai_sanren_hits-pop_sanren_hits)/n_bets*100:>+9.1f}%")
    print(f"{'推定ROI':>15} {pop_sanren_returns/n_bets*100:>11.1f}% {ai_sanren_returns/n_bets*100:>11.1f}% {(ai_sanren_returns-pop_sanren_returns)/n_bets*100:>+9.1f}%")
    
    # =========================================================
    # 9. まとめ
    # =========================================================
    print(f"\n" + "=" * 100)
    print(f"【9. まとめ：AIの付加価値】")
    print(f"=" * 100)
    
    print(f"""

=== 市場（人気順）との比較結果 ===

【単勝】
  人気1位: 的中率 {pop1_hit_rate:.1f}%, ROI {pop1_roi:.1f}%
  AI予測1位: 的中率 {ai1_hit_rate:.1f}%, ROI {ai1_roi:.1f}%
  → 差分: 的中率 {ai1_hit_rate-pop1_hit_rate:+.1f}%, ROI {ai1_roi-pop1_roi:+.1f}%

【3着内率】
  人気1位: {pop1_top3:.1f}%
  AI予測1位: {ai1_top3:.1f}%
  → 差分: {ai1_top3-pop1_top3:+.1f}%

【ワイド（1-2位）】
  人気1-2位: 的中率 {pop_wide_hits/n_bets*100:.1f}%
  AI予測1-2位: 的中率 {ai_wide_hits/n_bets*100:.1f}%
  → 差分: {(ai_wide_hits-pop_wide_hits)/n_bets*100:+.1f}%

【3連複（1-2-3位）】
  人気1-2-3位: 的中率 {pop_sanren_hits/n_bets*100:.1f}%
  AI予測1-2-3位: 的中率 {ai_sanren_hits/n_bets*100:.1f}%
  → 差分: {(ai_sanren_hits-pop_sanren_hits)/n_bets*100:+.1f}%

=== AIの本質的価値 ===

AIが市場を上回っている点:
  - 単勝ROI: {ai1_roi-pop1_roi:+.1f}%
  - ワイド的中率: {(ai_wide_hits-pop_wide_hits)/n_bets*100:+.1f}%

AIと市場が同程度の点:
  - 3着内率（予測1位）

課題:
  - 的中率では市場と大きな差がない
  - ROIの差が本質的な価値
""")


if __name__ == "__main__":
    main()
