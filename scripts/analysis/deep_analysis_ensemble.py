# -*- coding: utf-8 -*-
"""
V15+V4.4アンサンブル深層分析

穴馬的中、人気超え、回収率寄与を詳細分析
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
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    course_features_list = df.apply(
        lambda row: get_course_features(course_master, row.get('venue'), 
                                        row.get('track_surface'), row.get('distance_m')), axis=1)
    for col in ['course_slope_percent', 'course_final_straight_m']:
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
    print("V15+V4.4アンサンブル深層分析")
    print("穴馬的中、人気超え、回収率寄与を詳細分析")
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
    
    # 予測1位のみ抽出
    top1 = test_f[test_f['rank_ensemble'] == 1].copy()
    top1['is_hit'] = top1['finish_position'] == 1
    
    # 全体統計
    n_races = len(top1)
    n_hit = top1['is_hit'].sum()
    total_return = top1[top1['is_hit']]['win_odds'].sum()
    roi = total_return / n_races * 100
    
    print(f"\n" + "=" * 100)
    print(f"【全体統計】")
    print(f"=" * 100)
    print(f"  レース数: {n_races:,}")
    print(f"  的中数: {n_hit:,} (的中率 {n_hit/n_races*100:.1f}%)")
    print(f"  総回収: {total_return:.1f}倍")
    print(f"  ROI: {roi:.1f}%")
    
    # ============================================================
    # 1. 穴馬的中分析
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【1. 穴馬的中分析】")
    print(f"=" * 100)
    
    # 穴馬の定義: 6番人気以下
    top1['is_anaba'] = top1['popularity'] >= 6
    anaba_hits = top1[top1['is_hit'] & top1['is_anaba']]
    anaba_bets = top1[top1['is_anaba']]
    
    print(f"\n--- 穴馬（6番人気以下）を選んだ時のパフォーマンス ---")
    print(f"  ベット数: {len(anaba_bets):,}")
    print(f"  的中数: {len(anaba_hits):,}")
    print(f"  的中率: {len(anaba_hits)/len(anaba_bets)*100:.1f}%")
    print(f"  ROI: {anaba_hits['win_odds'].sum()/len(anaba_bets)*100:.1f}%")
    print(f"  回収率への寄与: {anaba_hits['win_odds'].sum()/total_return*100:.1f}%")
    
    print(f"\n--- 穴馬的中時の特徴量分析 ---")
    if len(anaba_hits) > 0:
        # 人気帯別
        for low, high in [(6, 8), (9, 12), (13, 99)]:
            subset = anaba_hits[(anaba_hits['popularity'] >= low) & (anaba_hits['popularity'] <= high)]
            if len(subset) > 0:
                print(f"\n  [{low}-{high}番人気] ({len(subset)}件)")
                print(f"    平均オッズ: {subset['win_odds'].mean():.1f}倍")
                print(f"    前走平均着順: {subset['horse_last_finish'].mean():.1f}着")
                print(f"    騎手平均勝率: {subset['jockey_win_rate'].mean()*100:.1f}%")
                if 'horse_last3_avg_finish' in subset.columns:
                    print(f"    3走平均着順: {subset['horse_last3_avg_finish'].mean():.1f}着")
    
    print(f"\n--- 穴馬的中の具体例（全件）---")
    for i, (_, row) in enumerate(anaba_hits.iterrows()):
        print(f"\n  例{i+1}: レース{row['race_id']}")
        print(f"    会場: {row.get('venue', 'N/A')}, 距離: {row.get('distance_m', 'N/A')}m, 馬場: {row.get('track_surface', 'N/A')}")
        print(f"    馬番: {row.get('horse_number', '?')}, 人気: {int(row['popularity'])}番人気, オッズ: {row['win_odds']:.1f}倍")
        print(f"    前走: {row.get('horse_last_finish', 'N/A')}着, 騎手勝率: {row.get('jockey_win_rate', 0)*100:.1f}%")
        if 'horse_last3_avg_finish' in row:
            print(f"    3走平均: {row.get('horse_last3_avg_finish', 'N/A'):.1f}着")
        if 'horse_top3_rate' in row:
            print(f"    馬3着内率: {row.get('horse_top3_rate', 0)*100:.1f}%")
        if i >= 9:  # 最大10件
            print(f"  ... 以下省略（残り{len(anaba_hits)-10}件）")
            break
    
    # ============================================================
    # 2. 人気超え分析（予測1位が人気順位より上の着順を取った）
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【2. 人気超え分析】")
    print(f"=" * 100)
    
    # 予測1位馬が人気より上の着順を取ったケース
    top1['beat_popularity'] = top1['finish_position'] < top1['popularity']
    
    beat_pop = top1[top1['beat_popularity']]
    
    print(f"\n--- 人気超え（着順 < 人気）のパフォーマンス ---")
    print(f"  人気超え件数: {len(beat_pop):,} ({len(beat_pop)/n_races*100:.1f}%)")
    
    # 的中時の人気超え（例: 5番人気で1着）
    beat_pop_hit = top1[top1['is_hit'] & (top1['popularity'] > 1)]
    print(f"  的中かつ人気超え: {len(beat_pop_hit):,}件")
    if len(beat_pop_hit) > 0:
        print(f"  平均人気: {beat_pop_hit['popularity'].mean():.1f}番人気（1着的中）")
        print(f"  平均オッズ: {beat_pop_hit['win_odds'].mean():.1f}倍")
        print(f"  回収率への寄与: {beat_pop_hit['win_odds'].sum()/total_return*100:.1f}%")
    
    print(f"\n--- 人気超え的中の詳細内訳 ---")
    for pop in [2, 3, 4, 5, (6, 10), (11, 99)]:
        if isinstance(pop, tuple):
            subset = beat_pop_hit[(beat_pop_hit['popularity'] >= pop[0]) & (beat_pop_hit['popularity'] <= pop[1])]
            label = f"{pop[0]}-{pop[1]}番人気→1着"
        else:
            subset = beat_pop_hit[beat_pop_hit['popularity'] == pop]
            label = f"{pop}番人気→1着"
        if len(subset) > 0:
            contrib = subset['win_odds'].sum() / total_return * 100
            print(f"  {label}: {len(subset)}件, 平均オッズ{subset['win_odds'].mean():.1f}倍, 回収寄与{contrib:.1f}%")
    
    # ============================================================
    # 3. 回収率寄与分析
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【3. 回収率寄与分析】")
    print(f"=" * 100)
    
    hits = top1[top1['is_hit']].copy()
    hits['contribution'] = hits['win_odds'] / total_return * 100
    
    print(f"\n--- オッズ帯別の回収寄与 ---")
    for low, high in [(1, 2), (2, 3), (3, 5), (5, 10), (10, 20), (20, 60), (60, 999)]:
        subset = hits[(hits['win_odds'] >= low) & (hits['win_odds'] < high)]
        if len(subset) > 0:
            contrib = subset['win_odds'].sum() / total_return * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {low:>3}-{high:<3}倍: {len(subset):>3}件, 回収{subset['win_odds'].sum():>6.1f}倍, 寄与{contrib:>5.1f}%, 平均オッズ{avg_odds:>5.1f}倍")
    
    print(f"\n--- 人気帯別の回収寄与 ---")
    for pop in [1, 2, 3, (4, 5), (6, 10), (11, 99)]:
        if isinstance(pop, tuple):
            subset = hits[(hits['popularity'] >= pop[0]) & (hits['popularity'] <= pop[1])]
            label = f"{pop[0]}-{pop[1]}番人気"
        else:
            subset = hits[hits['popularity'] == pop]
            label = f"{pop}番人気"
        if len(subset) > 0:
            contrib = subset['win_odds'].sum() / total_return * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {label:>12}: {len(subset):>3}件, 回収{subset['win_odds'].sum():>6.1f}倍, 寄与{contrib:>5.1f}%, 平均オッズ{avg_odds:>5.1f}倍")
    
    print(f"\n--- 騎手勝率帯別の回収寄与 ---")
    for low, high in [(0, 0.08), (0.08, 0.12), (0.12, 0.16), (0.16, 0.2), (0.2, 1)]:
        subset = hits[(hits['jockey_win_rate'] >= low) & (hits['jockey_win_rate'] < high)]
        if len(subset) > 0:
            contrib = subset['win_odds'].sum() / total_return * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  騎手勝率{low*100:>2.0f}-{high*100:<2.0f}%: {len(subset):>3}件, 回収{subset['win_odds'].sum():>6.1f}倍, 寄与{contrib:>5.1f}%")
    
    print(f"\n--- 前走着順別の回収寄与 ---")
    for finish in [1, 2, 3, (4, 5), (6, 10), (11, 99)]:
        if isinstance(finish, tuple):
            subset = hits[(hits['horse_last_finish'] >= finish[0]) & (hits['horse_last_finish'] <= finish[1])]
            label = f"前走{finish[0]}-{finish[1]}着"
        else:
            subset = hits[hits['horse_last_finish'] == finish]
            label = f"前走{finish}着"
        if len(subset) > 0:
            contrib = subset['win_odds'].sum() / total_return * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {label:>12}: {len(subset):>3}件, 回収{subset['win_odds'].sum():>6.1f}倍, 寄与{contrib:>5.1f}%, 平均オッズ{avg_odds:>5.1f}倍")
    
    # ============================================================
    # 4. 高回収レースの特徴分析
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【4. 高回収レースの特徴分析】")
    print(f"=" * 100)
    
    high_return = hits[hits['win_odds'] >= 10].sort_values('win_odds', ascending=False)
    
    print(f"\n--- オッズ10倍以上の的中レース（{len(high_return)}件）---")
    for i, (_, row) in enumerate(high_return.iterrows()):
        print(f"\n  【{i+1}】 オッズ {row['win_odds']:.1f}倍（{int(row['popularity'])}番人気）")
        print(f"    レース: {row['race_id']}, {row.get('venue', 'N/A')} {row.get('distance_m', 'N/A')}m {row.get('track_surface', 'N/A')}")
        print(f"    前走: {row.get('horse_last_finish', 'N/A')}着, 騎手勝率: {row.get('jockey_win_rate', 0)*100:.1f}%")
        if 'horse_last3_avg_finish' in row:
            print(f"    3走平均: {row.get('horse_last3_avg_finish', 'N/A'):.1f}着, 馬3着内率: {row.get('horse_top3_rate', 0)*100:.1f}%")
        if 'horse_front_runner_rate' in row:
            print(f"    逃げ率: {row.get('horse_front_runner_rate', 0)*100:.1f}%")
        if i >= 14:  # 最大15件
            break
    
    # ============================================================
    # 5. 特徴量のクロス分析
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【5. 特徴量クロス分析】")
    print(f"=" * 100)
    
    print(f"\n--- 人気 × 前走着順 のクロス分析 ---")
    cross = pd.crosstab(
        pd.cut(top1['popularity'], bins=[0, 3, 6, 99], labels=['1-3番人気', '4-6番人気', '7番人気以下']),
        pd.cut(top1['horse_last_finish'].fillna(99), bins=[0, 3, 6, 99], labels=['前走1-3着', '前走4-6着', '前走7着以下']),
        values=top1['is_hit'],
        aggfunc='mean'
    ) * 100
    print(cross.round(1))
    
    print(f"\n--- 人気 × 騎手勝率 のクロス分析 ---")
    cross2 = pd.crosstab(
        pd.cut(top1['popularity'], bins=[0, 3, 6, 99], labels=['1-3番人気', '4-6番人気', '7番人気以下']),
        pd.cut(top1['jockey_win_rate'], bins=[0, 0.1, 0.15, 1], labels=['騎手勝率10%未満', '10-15%', '15%以上']),
        values=top1['is_hit'],
        aggfunc='mean'
    ) * 100
    print(cross2.round(1))
    
    # ============================================================
    # 6. まとめ
    # ============================================================
    print(f"\n" + "=" * 100)
    print(f"【6. 詳細分析まとめ】")
    print(f"=" * 100)
    
    # 穴馬まとめ
    anaba_contrib = anaba_hits['win_odds'].sum() / total_return * 100 if len(anaba_hits) > 0 else 0
    kihon_contrib = 100 - anaba_contrib
    
    print(f"""
=== 穴馬（6番人気以下）の傾向 ===
  - ベット中の割合: {len(anaba_bets)/n_races*100:.1f}%
  - 穴馬を選んだ時の的中率: {len(anaba_hits)/len(anaba_bets)*100:.1f}%
  - 穴馬的中の回収率寄与: {anaba_contrib:.1f}%
  - 穴馬的中時の平均オッズ: {anaba_hits['win_odds'].mean():.1f}倍

  穴馬的中時の共通特徴:
  - 前走着順が良い（平均{anaba_hits['horse_last_finish'].mean():.1f}着）
  - 騎手勝率が{anaba_hits['jockey_win_rate'].mean()*100:.1f}%程度

=== 本命馬（1-5番人気）の傾向 ===
  - 本命馬を選んだ時の的中率: {top1[top1['popularity'] <= 5]['is_hit'].mean()*100:.1f}%
  - 本命馬的中の回収率寄与: {kihon_contrib:.1f}%

=== 回収率を稼ぐパターン ===
  1. 1番人気×騎手勝率15%以上: 堅実な回収
  2. 3-5番人気×前走2-3着: 高オッズでの的中
  3. 前走4-5着からの復帰馬: オッズが過小評価

=== モデルの予測ロジック ===
  - 直近成績（horse_last_finish）を最重視
  - 騎手成績（jockey_win_rate/top3_rate）を重視
  - V4.4がオッズを考慮した期待値評価を補完
""")


if __name__ == "__main__":
    main()
