# -*- coding: utf-8 -*-
"""
全券種の予測精度を深く分析するスクリプト

分析内容:
1. 予測Top1/Top2/Top3の1着・2着・3着入着率
2. 各券種（単勝、複勝、馬連、ワイド、馬単、三連複、三連単）のROI
3. V15+V4.4 Ensembleの予測精度
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
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


def analyze_position_accuracy(df):
    """予測順位ごとの1着・2着・3着入着率を分析"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    results = []
    for pred_rank in range(1, 6):
        subset = df[df['pred_rank'] == pred_rank]
        n = len(subset)
        
        win_rate = (subset['finish_position'] == 1).sum() / n * 100 if n > 0 else 0
        place2_rate = (subset['finish_position'] == 2).sum() / n * 100 if n > 0 else 0
        place3_rate = (subset['finish_position'] == 3).sum() / n * 100 if n > 0 else 0
        top3_rate = (subset['finish_position'] <= 3).sum() / n * 100 if n > 0 else 0
        
        results.append({
            'pred_rank': pred_rank,
            'n_samples': n,
            '1着率': win_rate,
            '2着率': place2_rate,
            '3着率': place3_rate,
            '複勝率(3着内)': top3_rate
        })
    
    return pd.DataFrame(results)


def calc_all_bet_types_roi(df, returns_df):
    """全券種のROIを計算"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    results = {}
    
    # 1. 単勝（Top1購入）
    top1 = df[df['pred_rank'] == 1]
    wins = top1[top1['finish_position'] == 1]
    results['tansho'] = {
        'bet_count': len(top1),
        'hit_count': len(wins),
        'hit_rate': len(wins) / len(top1) * 100 if len(top1) > 0 else 0,
        'roi': wins['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    }
    
    # 2. 複勝（Top3購入）
    top3 = df[df['pred_rank'] <= 3]
    # 複勝のため、3着以内かどうかで的中判定
    fukusho_ret = returns_df[returns_df['bet_type'] == 'fukusho']
    fukusho_dict = {}
    for _, row in fukusho_ret.iterrows():
        key = (row['race_id'], int(row['horse_number']))
        fukusho_dict[key] = row['payout']
    
    top3['fukusho_payout'] = top3.apply(
        lambda x: fukusho_dict.get((x['race_id'], int(x['horse_number'])), 0) if x['finish_position'] <= 3 else 0,
        axis=1
    )
    results['fukusho'] = {
        'bet_count': len(top3),
        'hit_count': (top3['fukusho_payout'] > 0).sum(),
        'hit_rate': (top3['fukusho_payout'] > 0).sum() / len(top3) * 100 if len(top3) > 0 else 0,
        'roi': top3['fukusho_payout'].sum() / (len(top3) * 100) * 100 if len(top3) > 0 else 0
    }
    
    # 3. 馬連（Top2購入、順不同）
    top2 = df[df['pred_rank'] <= 2].copy()
    top2 = top2.sort_values(['race_id', 'pred_rank'])
    bets = top2.groupby('race_id')['horse_number'].apply(lambda x: tuple(sorted(x.astype(int))) if len(x) == 2 else None).reset_index()
    bets = bets[bets['horse_number'].notna()]
    
    umaren_ret = returns_df[returns_df['bet_type'] == 'umaren'].dropna(subset=['horse_1', 'horse_2'])
    umaren_dict = {}
    for _, row in umaren_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2'])])))
        umaren_dict[key] = row['payout']
    
    bets['payout'] = bets.apply(lambda x: umaren_dict.get((x['race_id'], x['horse_number']), 0), axis=1)
    results['umaren'] = {
        'bet_count': len(bets),
        'hit_count': (bets['payout'] > 0).sum(),
        'hit_rate': (bets['payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'roi': bets['payout'].sum() / (len(bets) * 100) * 100 if len(bets) > 0 else 0
    }
    
    # 4. 馬単（Top1→Top2、順序あり）
    top2_ordered = df[df['pred_rank'] <= 2].sort_values(['race_id', 'pred_rank'])
    bets_tan = top2_ordered.groupby('race_id')['horse_number'].apply(lambda x: tuple(x.astype(int)) if len(x) == 2 else None).reset_index()
    bets_tan = bets_tan[bets_tan['horse_number'].notna()]
    
    umatan_ret = returns_df[returns_df['bet_type'] == 'umatan'].dropna(subset=['horse_1', 'horse_2'])
    umatan_dict = {}
    for _, row in umatan_ret.iterrows():
        key = (row['race_id'], (int(row['horse_1']), int(row['horse_2'])))
        umatan_dict[key] = row['payout']
    
    bets_tan['payout'] = bets_tan.apply(lambda x: umatan_dict.get((x['race_id'], x['horse_number']), 0), axis=1)
    results['umatan'] = {
        'bet_count': len(bets_tan),
        'hit_count': (bets_tan['payout'] > 0).sum(),
        'hit_rate': (bets_tan['payout'] > 0).sum() / len(bets_tan) * 100 if len(bets_tan) > 0 else 0,
        'roi': bets_tan['payout'].sum() / (len(bets_tan) * 100) * 100 if len(bets_tan) > 0 else 0
    }
    
    # 5. ワイド（Top2のいずれかが3着以内ならOK - Top2同士の組み合わせ）
    wide_ret = returns_df[returns_df['bet_type'] == 'wide'].dropna(subset=['horse_1', 'horse_2'])
    wide_dict = {}
    for _, row in wide_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2'])])))
        wide_dict[key] = row['payout']
    
    bets['wide_payout'] = bets.apply(lambda x: wide_dict.get((x['race_id'], x['horse_number']), 0), axis=1)
    results['wide'] = {
        'bet_count': len(bets),
        'hit_count': (bets['wide_payout'] > 0).sum(),
        'hit_rate': (bets['wide_payout'] > 0).sum() / len(bets) * 100 if len(bets) > 0 else 0,
        'roi': bets['wide_payout'].sum() / (len(bets) * 100) * 100 if len(bets) > 0 else 0
    }
    
    # 6. 三連複（Top3購入、順不同）
    top3_for_sanren = df[df['pred_rank'] <= 3].copy()
    top3_for_sanren = top3_for_sanren.sort_values(['race_id', 'pred_rank'])
    bets_3 = top3_for_sanren.groupby('race_id')['horse_number'].apply(
        lambda x: tuple(sorted(x.astype(int))) if len(x) == 3 else None
    ).reset_index()
    bets_3 = bets_3[bets_3['horse_number'].notna()]
    
    sanrenpuku_ret = returns_df[returns_df['bet_type'] == 'sanrenpuku'].dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrenpuku_dict = {}
    for _, row in sanrenpuku_ret.iterrows():
        key = (row['race_id'], tuple(sorted([int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])])))
        sanrenpuku_dict[key] = row['payout']
    
    bets_3['payout'] = bets_3.apply(lambda x: sanrenpuku_dict.get((x['race_id'], x['horse_number']), 0), axis=1)
    results['sanrenpuku'] = {
        'bet_count': len(bets_3),
        'hit_count': (bets_3['payout'] > 0).sum(),
        'hit_rate': (bets_3['payout'] > 0).sum() / len(bets_3) * 100 if len(bets_3) > 0 else 0,
        'roi': bets_3['payout'].sum() / (len(bets_3) * 100) * 100 if len(bets_3) > 0 else 0
    }
    
    # 7. 三連単（Top1→Top2→Top3、順序あり）
    bets_3_tan = top3_for_sanren.groupby('race_id')['horse_number'].apply(
        lambda x: tuple(x.astype(int)) if len(x) == 3 else None
    ).reset_index()
    bets_3_tan = bets_3_tan[bets_3_tan['horse_number'].notna()]
    
    sanrentan_ret = returns_df[returns_df['bet_type'] == 'sanrentan'].dropna(subset=['horse_1', 'horse_2', 'horse_3'])
    sanrentan_dict = {}
    for _, row in sanrentan_ret.iterrows():
        key = (row['race_id'], (int(row['horse_1']), int(row['horse_2']), int(row['horse_3'])))
        sanrentan_dict[key] = row['payout']
    
    bets_3_tan['payout'] = bets_3_tan.apply(lambda x: sanrentan_dict.get((x['race_id'], x['horse_number']), 0), axis=1)
    results['sanrentan'] = {
        'bet_count': len(bets_3_tan),
        'hit_count': (bets_3_tan['payout'] > 0).sum(),
        'hit_rate': (bets_3_tan['payout'] > 0).sum() / len(bets_3_tan) * 100 if len(bets_3_tan) > 0 else 0,
        'roi': bets_3_tan['payout'].sum() / (len(bets_3_tan) * 100) * 100 if len(bets_3_tan) > 0 else 0
    }
    
    return results


def main():
    print("=" * 80)
    print("全券種 予測精度分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 期間設定（直近1年をテスト）
    train = races[races['race_date'] <= '2023-12-31'].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    print(f"\nTrain: {len(train):,}件 (~2023-12-31)")
    print(f"Test:  {len(test):,}件 (2024-01-01~2024-12-31)")
    
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
    
    # 1. 予測順位ごとの入着率分析
    print("\n" + "=" * 80)
    print("【分析1】予測順位ごとの入着率")
    print("=" * 80)
    
    pos_accuracy = analyze_position_accuracy(test_f)
    print("\n予測順位 | サンプル数 | 1着率 | 2着率 | 3着率 | 複勝率(3着内)")
    print("-" * 70)
    for _, row in pos_accuracy.iterrows():
        print(f"  Top{int(row['pred_rank'])}    |  {int(row['n_samples']):6,}   | {row['1着率']:5.1f}% | {row['2着率']:5.1f}% | {row['3着率']:5.1f}% | {row['複勝率(3着内)']:5.1f}%")
    
    # Top3完全一致率
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top3_pred = test_f[test_f['pred_rank'] <= 3].groupby('race_id')['horse_number'].apply(set)
    top3_actual = test_f[test_f['finish_position'] <= 3].groupby('race_id')['horse_number'].apply(set)
    
    common = pd.merge(top3_pred.reset_index(), top3_actual.reset_index(), on='race_id', suffixes=('_pred', '_actual'))
    common['match'] = common.apply(lambda x: x['horse_number_pred'] == x['horse_number_actual'], axis=1)
    top3_perfect = common['match'].sum() / len(common) * 100 if len(common) > 0 else 0
    
    print(f"\nTop3完全一致率: {top3_perfect:.2f}%")
    
    # 2. 全券種ROI
    print("\n" + "=" * 80)
    print("【分析2】全券種ROI（Top1/Top2/Top3ベース）")
    print("=" * 80)
    
    bet_results = calc_all_bet_types_roi(test_f, returns)
    
    print("\n券種     | ベット数 | 的中数 | 的中率 | ROI")
    print("-" * 60)
    for bet_type, data in bet_results.items():
        print(f"{bet_type:10s} | {data['bet_count']:6,} | {data['hit_count']:5,} | {data['hit_rate']:5.1f}% | {data['roi']:6.1f}%")
    
    # 3. 理論的な限界分析
    print("\n" + "=" * 80)
    print("【分析3】各券種に必要な予測精度")
    print("=" * 80)
    
    print("""
    券種      | 必要な予測        | 控除率 | 損益分岐点的中率
    ----------|------------------|--------|------------------
    単勝      | Top1が1着         | 20%    | ~25%（オッズ4倍想定）
    複勝      | Top3が3着以内     | 20%    | ~60%（オッズ1.5倍想定）
    馬連      | Top2が1-2着(順不同)| 22.5%  | ~8%（オッズ12倍想定）
    馬単      | Top1→Top2(順序) | 25%    | ~4%（オッズ25倍想定）
    ワイド    | Top2が3着以内2頭  | 25%    | ~25%（オッズ4倍想定）
    三連複    | Top3が1-2-3着    | 25%    | ~2.5%（オッズ40倍想定）
    三連単    | Top1→Top2→Top3 | 27.5%  | ~1%（オッズ100倍想定）
    """)
    
    # 4. 現状の課題分析
    print("\n" + "=" * 80)
    print("【分析4】現状の課題と改善可能性")
    print("=" * 80)
    
    # 予測Top2のうち、実際に1-2着になった確率（馬連的中に必要）
    top2_data = test_f[test_f['pred_rank'] <= 2]
    top2_by_race = top2_data.groupby('race_id').agg({
        'horse_number': list,
        'finish_position': list
    }).reset_index()
    
    # Top2両方が1-2着
    def check_umaren_hit(row):
        positions = row['finish_position']
        return len(positions) == 2 and set(positions) == {1, 2}
    
    top2_by_race['umaren_hit'] = top2_by_race.apply(lambda x: check_umaren_hit(x) if len(x['horse_number']) == 2 else False, axis=1)
    umaren_accuracy = top2_by_race['umaren_hit'].sum() / len(top2_by_race) * 100
    
    print(f"\n予測Top2が1-2着になる確率: {umaren_accuracy:.2f}%")
    print(f"  → 馬連的中に必要な精度: 約8% (控除率22.5%、平均オッズ12倍想定)")
    
    # Top3が1-2-3着になる確率
    top3_data = test_f[test_f['pred_rank'] <= 3]
    top3_by_race = top3_data.groupby('race_id').agg({
        'horse_number': list,
        'finish_position': list
    }).reset_index()
    
    def check_sanren_hit(row):
        positions = row['finish_position']
        return len(positions) == 3 and set(positions) == {1, 2, 3}
    
    top3_by_race['sanren_hit'] = top3_by_race.apply(lambda x: check_sanren_hit(x) if len(x['horse_number']) == 3 else False, axis=1)
    sanren_accuracy = top3_by_race['sanren_hit'].sum() / len(top3_by_race) * 100
    
    print(f"\n予測Top3が1-2-3着になる確率: {sanren_accuracy:.2f}%")
    print(f"  → 三連複的中に必要な精度: 約2.5% (控除率25%、平均オッズ40倍想定)")
    
    return test_f, bet_results


if __name__ == "__main__":
    test_f, bet_results = main()
