# -*- coding: utf-8 -*-
"""
期待値ベース 全組み合わせ分析

従来: Top1/Top2/Top3を単純に買う
改良: 全組み合わせのEVを計算し、EV > 閾値のみ購入

分析内容:
1. 馬連: 全nC2組み合わせのEVを計算
2. 三連複: 全nC3組み合わせのEVを計算
3. EVフィルタリングの効果を検証
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
from itertools import combinations
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def calc_combo_probabilities(race_df, pred_col='pred'):
    """
    レース内の全組み合わせの的中確率を計算
    
    馬連確率: P(A,Bが1-2着) ≈ P(A)×P(B)×2（順列考慮の簡易式）
    三連複確率: P(A,B,Cが1-2-3着) ≈ P(A)×P(B)×P(C)×6
    
    より正確には、モンテカルロシミュレーションが必要だが、
    ここでは簡易的にスコアの積で近似する
    """
    # 予測スコアを確率に変換（ソフトマックス）
    scores = race_df[pred_col].values
    probs = np.exp(scores) / np.exp(scores).sum()
    
    horses = race_df['horse_number'].astype(int).values
    
    # 馬連組み合わせ
    umaren_combos = []
    for i, j in combinations(range(len(horses)), 2):
        # 簡易確率: 上位2頭になる確率 ≈ P(i)×P(j)×定数
        # 正規化係数は後で調整
        prob = probs[i] * probs[j] * 2
        umaren_combos.append({
            'horse_set': tuple(sorted([horses[i], horses[j]])),
            'prob': prob,
            'horse_1': horses[i],
            'horse_2': horses[j],
            'score_1': scores[i],
            'score_2': scores[j]
        })
    
    # 三連複組み合わせ
    sanren_combos = []
    for i, j, k in combinations(range(len(horses)), 3):
        prob = probs[i] * probs[j] * probs[k] * 6
        sanren_combos.append({
            'horse_set': tuple(sorted([horses[i], horses[j], horses[k]])),
            'prob': prob,
            'horse_1': horses[i],
            'horse_2': horses[j],
            'horse_3': horses[k]
        })
    
    return pd.DataFrame(umaren_combos), pd.DataFrame(sanren_combos)


def analyze_ev_strategy(test_f, returns_df, ev_thresholds=[0.8, 1.0, 1.2, 1.5, 2.0]):
    """
    期待値ベースの戦略を分析
    """
    print("\n" + "=" * 80)
    print("期待値ベース 全組み合わせ分析")
    print("=" * 80)
    
    # 払い戻しデータの辞書化
    umaren_ret = returns_df[returns_df['bet_type'] == 'umaren'].dropna(subset=['horse_1', 'horse_2'])
    umaren_dict = {}
    for _, row in umaren_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2'])])))
        umaren_dict[key] = row['payout']
    
    sanren_ret = returns_df[returns_df['bet_type'] == 'sanrenpuku'].dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanren_dict = {}
    for _, row in sanren_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])])))
        sanren_dict[key] = row['payout']
    
    # レースごとに分析
    race_ids = test_f['race_id'].unique()
    
    all_umaren_bets = []
    all_sanren_bets = []
    
    print(f"\n{len(race_ids)}レースを分析中...")
    
    for i, race_id in enumerate(race_ids):
        if i % 500 == 0:
            print(f"  処理中: {i}/{len(race_ids)}")
        
        race_df = test_f[test_f['race_id'] == race_id].copy()
        
        if len(race_df) < 3:
            continue
        
        # 組み合わせ確率を計算
        umaren_combos, sanren_combos = calc_combo_probabilities(race_df)
        
        # 馬連のオッズと期待値を計算
        for _, combo in umaren_combos.iterrows():
            horse_set = combo['horse_set']
            payout = umaren_dict.get((race_id, horse_set), 0)
            
            # 的中した場合の払い戻しから逆算でオッズを推定
            # 実際には的中/未的中に関わらずオッズが必要だが、
            # ここでは的中時の払い戻しを使用（未的中はオッズ不明のため除外）
            if payout > 0:
                odds = payout / 100
                ev = combo['prob'] * odds
                
                all_umaren_bets.append({
                    'race_id': race_id,
                    'horse_set': horse_set,
                    'prob': combo['prob'],
                    'odds': odds,
                    'ev': ev,
                    'payout': payout,
                    'hit': 1
                })
        
        # 三連複も同様
        for _, combo in sanren_combos.iterrows():
            horse_set = combo['horse_set']
            payout = sanren_dict.get((race_id, horse_set), 0)
            
            if payout > 0:
                odds = payout / 100
                ev = combo['prob'] * odds
                
                all_sanren_bets.append({
                    'race_id': race_id,
                    'horse_set': horse_set,
                    'prob': combo['prob'],
                    'odds': odds,
                    'ev': ev,
                    'payout': payout,
                    'hit': 1
                })
    
    umaren_bets_df = pd.DataFrame(all_umaren_bets)
    sanren_bets_df = pd.DataFrame(all_sanren_bets)
    
    print(f"\n馬連的中ベット数: {len(umaren_bets_df)}")
    print(f"三連複的中ベット数: {len(sanren_bets_df)}")
    
    # 的中データのEV分布を分析
    print("\n" + "=" * 80)
    print("【分析1】的中時のEV分布")
    print("=" * 80)
    
    if len(umaren_bets_df) > 0:
        print(f"\n馬連（的中 {len(umaren_bets_df)}件）:")
        print(f"  EV平均: {umaren_bets_df['ev'].mean():.3f}")
        print(f"  EV中央値: {umaren_bets_df['ev'].median():.3f}")
        print(f"  EV最小-最大: {umaren_bets_df['ev'].min():.3f} - {umaren_bets_df['ev'].max():.3f}")
        
        for thresh in ev_thresholds:
            high_ev = umaren_bets_df[umaren_bets_df['ev'] >= thresh]
            print(f"  EV >= {thresh}: {len(high_ev)}件 ({len(high_ev)/len(umaren_bets_df)*100:.1f}%)")
    
    if len(sanren_bets_df) > 0:
        print(f"\n三連複（的中 {len(sanren_bets_df)}件）:")
        print(f"  EV平均: {sanren_bets_df['ev'].mean():.3f}")
        print(f"  EV中央値: {sanren_bets_df['ev'].median():.3f}")
        print(f"  EV最小-最大: {sanren_bets_df['ev'].min():.3f} - {sanren_bets_df['ev'].max():.3f}")
        
        for thresh in ev_thresholds:
            high_ev = sanren_bets_df[sanren_bets_df['ev'] >= thresh]
            print(f"  EV >= {thresh}: {len(high_ev)}件 ({len(high_ev)/len(sanren_bets_df)*100:.1f}%)")
    
    return umaren_bets_df, sanren_bets_df


def analyze_all_combos_with_prior_odds(test_f, returns_df):
    """
    全組み合わせのEVを計算（事前オッズ不明のため、スコア上位の組み合わせを分析）
    """
    print("\n" + "=" * 80)
    print("【分析2】予測スコア上位組み合わせのROI分析")
    print("=" * 80)
    
    # 払い戻しデータの辞書化
    umaren_ret = returns_df[returns_df['bet_type'] == 'umaren'].dropna(subset=['horse_1', 'horse_2'])
    umaren_dict = {}
    for _, row in umaren_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2'])])))
        umaren_dict[key] = row['payout']
    
    sanren_ret = returns_df[returns_df['bet_type'] == 'sanrenpuku'].dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanren_dict = {}
    for _, row in sanren_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])])))
        sanren_dict[key] = row['payout']
    
    race_ids = test_f['race_id'].unique()
    
    # 上位N組み合わせを買う戦略
    results = []
    
    for top_n in [1, 2, 3, 5, 10]:
        umaren_bets = 0
        umaren_payout = 0
        umaren_hits = 0
        
        sanren_bets = 0
        sanren_payout = 0
        sanren_hits = 0
        
        for race_id in race_ids:
            race_df = test_f[test_f['race_id'] == race_id].copy()
            
            if len(race_df) < 3:
                continue
            
            # 組み合わせ確率を計算
            umaren_combos, sanren_combos = calc_combo_probabilities(race_df)
            
            # 確率上位N組み合わせを選択
            top_umaren = umaren_combos.nlargest(top_n, 'prob')
            top_sanren = sanren_combos.nlargest(top_n, 'prob')
            
            for _, combo in top_umaren.iterrows():
                umaren_bets += 1
                payout = umaren_dict.get((race_id, combo['horse_set']), 0)
                if payout > 0:
                    umaren_payout += payout
                    umaren_hits += 1
            
            for _, combo in top_sanren.iterrows():
                sanren_bets += 1
                payout = sanren_dict.get((race_id, combo['horse_set']), 0)
                if payout > 0:
                    sanren_payout += payout
                    sanren_hits += 1
        
        umaren_roi = umaren_payout / (umaren_bets * 100) * 100 if umaren_bets > 0 else 0
        sanren_roi = sanren_payout / (sanren_bets * 100) * 100 if sanren_bets > 0 else 0
        
        results.append({
            'top_n': top_n,
            'umaren_bets': umaren_bets,
            'umaren_hits': umaren_hits,
            'umaren_hit_rate': umaren_hits / umaren_bets * 100 if umaren_bets > 0 else 0,
            'umaren_roi': umaren_roi,
            'sanren_bets': sanren_bets,
            'sanren_hits': sanren_hits,
            'sanren_hit_rate': sanren_hits / sanren_bets * 100 if sanren_bets > 0 else 0,
            'sanren_roi': sanren_roi
        })
    
    print("\n確率上位N組み合わせ戦略:")
    print("-" * 80)
    print("Top N | 馬連ベット | 馬連的中率 | 馬連ROI  | 三連複ベット | 三連複的中率 | 三連複ROI")
    print("-" * 80)
    
    for r in results:
        print(f"Top{r['top_n']:2d} | {r['umaren_bets']:8,} | {r['umaren_hit_rate']:8.2f}% | {r['umaren_roi']:7.1f}% | {r['sanren_bets']:10,} | {r['sanren_hit_rate']:10.2f}% | {r['sanren_roi']:9.1f}%")
    
    return results


def analyze_prediction_quality(test_f, returns_df):
    """
    予測スコアと実際の着順の関係を詳細分析
    """
    print("\n" + "=" * 80)
    print("【分析3】予測スコアと着順の関係")
    print("=" * 80)
    
    test_f = test_f.copy()
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # 予測スコアのパーセンタイル別の着順分布
    test_f['pred_percentile'] = test_f.groupby('race_id')['pred'].rank(pct=True)
    
    bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
    labels = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
    test_f['pred_bin'] = pd.cut(test_f['pred_percentile'], bins=bins, labels=labels)
    
    print("\n予測パーセンタイル別の平均着順:")
    for label in labels:
        subset = test_f[test_f['pred_bin'] == label]
        avg_pos = subset['finish_position'].mean()
        top3_rate = (subset['finish_position'] <= 3).mean() * 100
        print(f"  {label}: 平均着順 {avg_pos:.2f}, 複勝率 {top3_rate:.1f}%")
    
    # スコア上位者同士の組み合わせが1-2着/1-2-3着になる確率
    print("\n予測Top組み合わせの着順分布:")
    
    race_ids = test_f['race_id'].unique()
    
    # Top2の着順パターン
    top2_patterns = {}
    for race_id in race_ids:
        race_df = test_f[test_f['race_id'] == race_id].copy()
        top2 = race_df.nsmallest(2, 'pred_rank')
        
        if len(top2) == 2:
            positions = tuple(sorted(top2['finish_position'].values))
            top2_patterns[positions] = top2_patterns.get(positions, 0) + 1
    
    print("\n予測Top2の実際の着順パターン（上位10）:")
    sorted_patterns = sorted(top2_patterns.items(), key=lambda x: -x[1])
    total = sum(top2_patterns.values())
    cumulative = 0
    for pattern, count in sorted_patterns[:10]:
        pct = count / total * 100
        cumulative += pct
        print(f"  {pattern}: {count:5,}件 ({pct:5.1f}%) [累積: {cumulative:.1f}%]")
    
    # 1-2着パターンの割合
    umaren_hit = top2_patterns.get((1, 2), 0)
    print(f"\n→ Top2が1-2着: {umaren_hit:,}件 ({umaren_hit/total*100:.2f}%)")
    
    # Top3の着順パターン
    top3_patterns = {}
    for race_id in race_ids:
        race_df = test_f[test_f['race_id'] == race_id].copy()
        top3 = race_df.nsmallest(3, 'pred_rank')
        
        if len(top3) == 3:
            positions = tuple(sorted(top3['finish_position'].values))
            top3_patterns[positions] = top3_patterns.get(positions, 0) + 1
    
    print("\n予測Top3の実際の着順パターン（上位10）:")
    sorted_patterns3 = sorted(top3_patterns.items(), key=lambda x: -x[1])
    total3 = sum(top3_patterns.values())
    cumulative3 = 0
    for pattern, count in sorted_patterns3[:10]:
        pct = count / total3 * 100
        cumulative3 += pct
        print(f"  {pattern}: {count:5,}件 ({pct:5.1f}%) [累積: {cumulative3:.1f}%]")
    
    # 1-2-3着パターンの割合
    sanren_hit = top3_patterns.get((1, 2, 3), 0)
    print(f"\n→ Top3が1-2-3着: {sanren_hit:,}件 ({sanren_hit/total3*100:.2f}%)")
    
    return top2_patterns, top3_patterns


def main():
    print("=" * 80)
    print("期待値ベース 全組み合わせ深層分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2023-12-31'].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    print(f"\nTrain: {len(train):,}件 (~2023-12-31)")
    print(f"Test:  {len(test):,}件 (2024-01-01~2024-12-31)")
    print(f"Test レース数: {test['race_id'].nunique():,}")
    
    # 特徴量生成
    print("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # V15 Binary Modelの学習
    import lightgbm as lgb
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # テストデータで予測
    X_test = test_f[feature_cols].fillna(0)
    test_f['pred'] = model.predict(X_test)
    
    # 分析実行
    analyze_prediction_quality(test_f, returns)
    analyze_all_combos_with_prior_odds(test_f, returns)
    analyze_ev_strategy(test_f, returns)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    
    return test_f


if __name__ == "__main__":
    test_f = main()
