# -*- coding: utf-8 -*-
"""
人気超え3着内分析

1着以外でも人気より着順が良い（特に3着以内）のパフォーマンスを分析
ワイド、馬連、3連複への応用可能性を検証
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
    print("人気超え3着内分析")
    print("1着以外でも人気より着順が良い（特に3着以内）のパフォーマンスを分析")
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
    
    # =====================================================
    # 予測Top1の分析
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【1. 予測Top1の人気超え分析】")
    print(f"=" * 100)
    
    top1 = test_f[test_f['rank_ensemble'] == 1].copy()
    top1['beat_popularity'] = top1['finish_position'] < top1['popularity']
    top1['in_top3'] = top1['finish_position'] <= 3
    
    n_races = len(top1)
    
    print(f"\n--- 予測1位馬の着順分布 ---")
    for pos in [1, 2, 3, (4, 5), (6, 10), (11, 99)]:
        if isinstance(pos, tuple):
            cnt = ((top1['finish_position'] >= pos[0]) & (top1['finish_position'] <= pos[1])).sum()
            label = f"{pos[0]}-{pos[1]}着"
        else:
            cnt = (top1['finish_position'] == pos).sum()
            label = f"{pos}着"
        pct = cnt / n_races * 100
        print(f"  {label}: {cnt:,}件 ({pct:.1f}%)")
    
    print(f"\n--- 予測1位馬の3着内率 ---")
    top3_rate = top1['in_top3'].mean() * 100
    print(f"  3着内率: {top3_rate:.1f}%")
    
    print(f"\n--- 人気超え（着順 < 人気）の分析 ---")
    beat_pop = top1[top1['beat_popularity']]
    print(f"  人気超え件数: {len(beat_pop):,} ({len(beat_pop)/n_races*100:.1f}%)")
    
    # 人気超え×3着内
    beat_pop_top3 = top1[top1['beat_popularity'] & top1['in_top3']]
    print(f"  人気超え かつ 3着内: {len(beat_pop_top3):,} ({len(beat_pop_top3)/n_races*100:.1f}%)")
    
    # =====================================================
    # 予測Top3の分析
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【2. 予測Top3の人気超え分析】")
    print(f"=" * 100)
    
    top3_pred = test_f[test_f['rank_ensemble'] <= 3].copy()
    top3_pred['beat_popularity'] = top3_pred['finish_position'] < top3_pred['popularity']
    top3_pred['in_top3'] = top3_pred['finish_position'] <= 3
    
    print(f"\n--- 予測Top3馬の着順分布 ---")
    for pos in [1, 2, 3, (4, 5), (6, 10), (11, 99)]:
        if isinstance(pos, tuple):
            cnt = ((top3_pred['finish_position'] >= pos[0]) & (top3_pred['finish_position'] <= pos[1])).sum()
            label = f"{pos[0]}-{pos[1]}着"
        else:
            cnt = (top3_pred['finish_position'] == pos).sum()
            label = f"{pos}着"
        pct = cnt / len(top3_pred) * 100
        print(f"  {label}: {cnt:,}件 ({pct:.1f}%)")
    
    print(f"\n--- 予測Top3馬の3着内率 ---")
    top3_rate = top3_pred['in_top3'].mean() * 100
    print(f"  3着内率: {top3_rate:.1f}%")
    
    print(f"\n--- 人気超え×3着内の詳細 ---")
    for pred_rank in [1, 2, 3]:
        subset = test_f[test_f['rank_ensemble'] == pred_rank].copy()
        subset['beat_popularity'] = subset['finish_position'] < subset['popularity']
        subset['in_top3'] = subset['finish_position'] <= 3
        
        beat_top3 = subset[subset['beat_popularity'] & subset['in_top3']]
        print(f"  予測{pred_rank}位: 人気超え×3着内 = {len(beat_top3):,}件 ({len(beat_top3)/len(subset)*100:.1f}%)")
    
    # =====================================================
    # 人気帯別の人気超え分析
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【3. 人気帯別の人気超え分析】")
    print(f"=" * 100)
    
    for pred_rank in [1, 2, 3]:
        subset = test_f[test_f['rank_ensemble'] == pred_rank].copy()
        subset['beat_popularity'] = subset['finish_position'] < subset['popularity']
        subset['in_top3'] = subset['finish_position'] <= 3
        
        print(f"\n--- 予測{pred_rank}位の人気帯別分析 ---")
        for pop in [(1, 3), (4, 6), (7, 10), (11, 99)]:
            pop_subset = subset[(subset['popularity'] >= pop[0]) & (subset['popularity'] <= pop[1])]
            if len(pop_subset) > 0:
                # 人気超え率
                beat_rate = pop_subset['beat_popularity'].mean() * 100
                # 3着内率
                top3_rate = pop_subset['in_top3'].mean() * 100
                # 人気超え×3着内率
                beat_top3_rate = (pop_subset['beat_popularity'] & pop_subset['in_top3']).mean() * 100
                
                print(f"  {pop[0]}-{pop[1]}番人気: 人気超え{beat_rate:.1f}%, 3着内{top3_rate:.1f}%, 人気超え×3着内{beat_top3_rate:.1f}% (n={len(pop_subset)})")
    
    # =====================================================
    # 馬券種別シミュレーション
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【4. 馬券種別シミュレーション（予測Top3を使用）】")
    print(f"=" * 100)
    
    n_test_races = test_f['race_id'].nunique()
    
    # 複勝シミュレーション（予測1-3位をそれぞれ購入）
    print(f"\n--- 複勝（予測上位を購入）---")
    for pred_rank in [1, 2, 3]:
        subset = test_f[test_f['rank_ensemble'] == pred_rank]
        hits = subset[subset['finish_position'] <= 3]
        hit_rate = len(hits) / len(subset) * 100
        # 複勝オッズを単勝の約25-30%と推定
        est_fukusho_roi = hits['win_odds'].sum() * 0.27 / len(subset) * 100
        print(f"  予測{pred_rank}位: 的中{len(hits):,}件 ({hit_rate:.1f}%), 推定ROI {est_fukusho_roi:.1f}%")
    
    # ワイドシミュレーション（予測1-2位の組み合わせ）
    print(f"\n--- ワイド（予測1位×2位）---")
    for race_id in test_f['race_id'].unique()[:1000]:  # サンプル1000レース
        race = test_f[test_f['race_id'] == race_id].sort_values('rank_ensemble')
        if len(race) < 3:
            continue
        
    # 実際のワイドシミュレーション
    wide_hits = 0
    wide_bets = 0
    wide_returns = 0
    
    for race_id in test_f['race_id'].unique():
        race = test_f[test_f['race_id'] == race_id].sort_values('rank_ensemble')
        if len(race) < 3:
            continue
        
        top2_pred = set(race.head(2)['horse_id'].values)
        top3_actual = set(race[race['finish_position'] <= 3]['horse_id'].values)
        
        if len(top2_pred & top3_actual) == 2:  # 両方3着内
            wide_hits += 1
            # ワイドオッズを推定（単勝オッズの積の平方根 × 0.3）
            odds1 = race.iloc[0]['win_odds'] if pd.notna(race.iloc[0]['win_odds']) else 1
            odds2 = race.iloc[1]['win_odds'] if pd.notna(race.iloc[1]['win_odds']) else 1
            wide_returns += np.sqrt(odds1 * odds2) * 0.3
        wide_bets += 1
    
    print(f"  ベット: {wide_bets:,}件, 的中: {wide_hits:,}件 ({wide_hits/wide_bets*100:.1f}%)")
    print(f"  推定ROI: {wide_returns/wide_bets*100:.1f}%")
    
    # 3連複シミュレーション（予測1-2-3位の組み合わせ）
    print(f"\n--- 3連複（予測1位×2位×3位）---")
    sanren_hits = 0
    sanren_bets = 0
    sanren_returns = 0
    
    for race_id in test_f['race_id'].unique():
        race = test_f[test_f['race_id'] == race_id].sort_values('rank_ensemble')
        if len(race) < 3:
            continue
        
        top3_pred = set(race.head(3)['horse_id'].values)
        top3_actual = set(race[race['finish_position'] <= 3]['horse_id'].values)
        
        if top3_pred == top3_actual:  # 完全一致
            sanren_hits += 1
            # 3連複オッズを推定（単勝オッズの積の立方根 × 2）
            odds1 = race.iloc[0]['win_odds'] if pd.notna(race.iloc[0]['win_odds']) else 1
            odds2 = race.iloc[1]['win_odds'] if pd.notna(race.iloc[1]['win_odds']) else 1
            odds3 = race.iloc[2]['win_odds'] if pd.notna(race.iloc[2]['win_odds']) else 1
            sanren_returns += (odds1 * odds2 * odds3) ** (1/3) * 2
        sanren_bets += 1
    
    print(f"  ベット: {sanren_bets:,}件, 的中: {sanren_hits:,}件 ({sanren_hits/sanren_bets*100:.1f}%)")
    print(f"  推定ROI: {sanren_returns/sanren_bets*100:.1f}%")
    
    # =====================================================
    # 人気超え×3着内の特徴分析
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【5. 人気超え×3着内の馬の特徴分析】")
    print(f"=" * 100)
    
    # 予測上位3頭で人気超え×3着内を達成した馬
    top3_pred = test_f[test_f['rank_ensemble'] <= 3].copy()
    top3_pred['beat_popularity'] = top3_pred['finish_position'] < top3_pred['popularity']
    top3_pred['in_top3'] = top3_pred['finish_position'] <= 3
    
    beat_top3 = top3_pred[top3_pred['beat_popularity'] & top3_pred['in_top3']]
    not_beat = top3_pred[~(top3_pred['beat_popularity'] & top3_pred['in_top3'])]
    
    print(f"\n人気超え×3着内の達成: {len(beat_top3):,}件")
    
    print(f"\n--- 特徴量の平均値比較 ---")
    features_to_compare = ['horse_last_finish', 'horse_last3_avg_finish', 'jockey_win_rate', 
                            'jockey_top3_rate', 'horse_top3_rate', 'popularity', 'win_odds']
    
    print(f"{'特徴量':<30} {'人気超え×3着内':>15} {'それ以外':>15} {'差':>10}")
    print("-" * 70)
    for feat in features_to_compare:
        if feat in beat_top3.columns and feat in not_beat.columns:
            avg_beat = beat_top3[feat].mean()
            avg_not = not_beat[feat].mean()
            diff = avg_beat - avg_not
            print(f"{feat:<30} {avg_beat:>15.2f} {avg_not:>15.2f} {diff:>+10.2f}")
    
    # =====================================================
    # 予測2-3位での人気超え成功パターン
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【6. 予測2-3位での人気超え成功パターン】")
    print(f"=" * 100)
    
    for pred_rank in [2, 3]:
        subset = test_f[test_f['rank_ensemble'] == pred_rank].copy()
        subset['beat_popularity'] = subset['finish_position'] < subset['popularity']
        subset['in_top3'] = subset['finish_position'] <= 3
        
        success = subset[subset['beat_popularity'] & subset['in_top3']]
        
        print(f"\n--- 予測{pred_rank}位で人気超え×3着内（{len(success)}件）---")
        
        if len(success) > 0:
            print(f"  平均人気: {success['popularity'].mean():.1f}番人気")
            print(f"  平均着順: {success['finish_position'].mean():.1f}着")
            print(f"  平均前走: {success['horse_last_finish'].mean():.1f}着")
            print(f"  平均騎手勝率: {success['jockey_win_rate'].mean()*100:.1f}%")
            
            # 人気帯別
            print(f"\n  人気帯別内訳:")
            for pop in [(1, 3), (4, 6), (7, 10), (11, 99)]:
                cnt = ((success['popularity'] >= pop[0]) & (success['popularity'] <= pop[1])).sum()
                if cnt > 0:
                    print(f"    {pop[0]}-{pop[1]}番人気: {cnt}件")
    
    # =====================================================
    # まとめ
    # =====================================================
    print(f"\n" + "=" * 100)
    print(f"【7. まとめ：現行モデルを踏まえた考察】")
    print(f"=" * 100)
    
    # 予測Top3の3着内率を計算
    for pred_rank in [1, 2, 3]:
        subset = test_f[test_f['rank_ensemble'] == pred_rank]
        top3_rate = (subset['finish_position'] <= 3).mean() * 100
        beat_top3_rate = ((subset['finish_position'] < subset['popularity']) & (subset['finish_position'] <= 3)).mean() * 100
        print(f"  予測{pred_rank}位: 3着内率{top3_rate:.1f}%, 人気超え×3着内{beat_top3_rate:.1f}%")
    
    print(f"""

=== 現行モデル（V15+V4.4）の評価 ===

1. 予測2-3位の活用価値
   - 予測2位: 3着内率が予測1位に次いで高い
   - 予測3位: 人気超え×3着内で「穴馬発掘」の可能性
   
2. 複勝系馬券への応用
   - 複勝: 予測上位3頭すべてに有効
   - ワイド: 予測1-2位の組み合わせ
   - 3連複: 予測1-2-3位の組み合わせ

3. V4.4の役割
   - 2-3着も学習対象（LambdaRankのreleavance設定）
   - 3着内を意識したランキング学習
   - 穴馬の過小評価を発見

4. 改善の方向性
   - 現行目的関数: 1着予測
   - 拡張目的関数: 3着内予測（Binary + LambdaRank）
   - 馬券種別ごとの最適化モデル
""")


if __name__ == "__main__":
    main()
