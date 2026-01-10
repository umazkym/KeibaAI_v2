# -*- coding: utf-8 -*-
"""
大規模パターン分析スクリプト

全レースを対象に、モデルの予測パターンを統計的に深く分析
- 失敗パターンの体系的分類
- オッズレンジ別の詳細分析
- 特徴量値と予測成功率の関係
- 人気逆転パターンの分析
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    """データを読み込む"""
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


def train_v15(train_df, valid_df, feature_cols):
    """V15モデル学習"""
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
    
    model = lgb.train(params, train_ds, num_boost_round=300, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def comprehensive_pattern_analysis(test_df, preds, feature_cols, model):
    """包括的パターン分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 予測Top1馬
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # 勝馬情報を取得
    winners = d[d['finish_position'] == 1][['race_id', 'horse_number', 'win_odds', 'score', 'rank_pred', 'popularity']].copy()
    winners.columns = ['race_id', 'winner_num', 'winner_odds', 'winner_score', 'winner_rank', 'winner_pop']
    
    bet_df = bet_df.merge(winners, on='race_id', how='left')
    
    return bet_df, d


def analyze_failure_patterns(bet_df, all_df):
    """失敗パターンの体系的分析"""
    misses = bet_df[~bet_df['is_hit']].copy()
    
    print("\n" + "=" * 80)
    print("【失敗パターン体系的分析】（全失敗レースを対象）")
    print("=" * 80)
    
    total_misses = len(misses)
    print(f"\n総失敗レース数: {total_misses:,}件")
    
    # パターン1: 勝馬のモデル内順位分布
    print("\n■ 勝馬のモデル内順位分布")
    print("-" * 50)
    for rank in range(1, 11):
        count = (misses['winner_rank'] == rank).sum()
        pct = count / total_misses * 100
        bar = '█' * int(pct / 2)
        print(f"  {rank:>2}位: {count:>5}件 ({pct:>5.1f}%) {bar}")
    
    count_over10 = (misses['winner_rank'] > 10).sum()
    print(f"  11位～: {count_over10:>5}件 ({count_over10/total_misses*100:>5.1f}%)")
    
    # パターン2: 人気との関係
    print("\n■ 失敗パターン分類（予測馬の人気 vs 勝馬の人気）")
    print("-" * 60)
    
    if 'popularity' in misses.columns:
        # 1番人気を選んで外した
        pred_1pop = misses[misses['popularity'] == 1]
        print(f"  予測馬が1番人気だった失敗: {len(pred_1pop):>5}件 ({len(pred_1pop)/total_misses*100:.1f}%)")
        if len(pred_1pop) > 0:
            print(f"    → その時の勝馬の人気分布: 2-3人気{(pred_1pop['winner_pop']<=3).sum()}件, 4-6人気{((pred_1pop['winner_pop']>=4)&(pred_1pop['winner_pop']<=6)).sum()}件, 7人気～{(pred_1pop['winner_pop']>=7).sum()}件")
        
        # 人気馬（1-3番人気）を選んで外した
        pred_top3 = misses[(misses['popularity'] >= 1) & (misses['popularity'] <= 3)]
        print(f"  予測馬が1-3番人気だった失敗: {len(pred_top3):>5}件 ({len(pred_top3)/total_misses*100:.1f}%)")
        
        # 穴馬（4番人気以降）を選んで外した
        pred_ana = misses[misses['popularity'] >= 4]
        print(f"  予測馬が4番人気以降だった失敗: {len(pred_ana):>5}件 ({len(pred_ana)/total_misses*100:.1f}%)")
        
        # 人気逆転パターン（モデルが穴を選んだが本命が勝った）
        upset = misses[(misses['popularity'] >= 4) & (misses['winner_pop'] <= 2)]
        print(f"  穴選択→本命が勝った: {len(upset):>5}件 ({len(upset)/total_misses*100:.1f}%)")
    
    # パターン3: オッズ差による分類
    print("\n■ オッズ差による失敗分類")
    print("-" * 60)
    
    misses['odds_diff'] = misses['winner_odds'] - misses['win_odds']
    
    # 予測馬より低オッズ（人気）の馬が勝った
    lower_odds_wins = misses[misses['odds_diff'] < 0]
    # 予測馬より高オッズ（穴）の馬が勝った
    higher_odds_wins = misses[misses['odds_diff'] > 0]
    
    print(f"  予測馬より人気の馬が勝った: {len(lower_odds_wins):>5}件 ({len(lower_odds_wins)/total_misses*100:.1f}%)")
    print(f"  予測馬より穴の馬が勝った:   {len(higher_odds_wins):>5}件 ({len(higher_odds_wins)/total_misses*100:.1f}%)")
    
    if len(higher_odds_wins) > 0:
        print(f"    → 勝馬の平均オッズ: {higher_odds_wins['winner_odds'].mean():.1f}倍")
    
    return misses


def analyze_success_patterns(bet_df):
    """成功パターンの体系的分析"""
    hits = bet_df[bet_df['is_hit']].copy()
    
    print("\n" + "=" * 80)
    print("【成功パターン体系的分析】（全的中レースを対象）")
    print("=" * 80)
    
    total_hits = len(hits)
    print(f"\n総的中レース数: {total_hits:,}件")
    
    # オッズ分布
    print("\n■ 的中馬のオッズ分布")
    print("-" * 50)
    
    odds_bins = [(1, 2), (2, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 999)]
    for lo, hi in odds_bins:
        count = ((hits['win_odds'] >= lo) & (hits['win_odds'] < hi)).sum()
        odds_sum = hits[(hits['win_odds'] >= lo) & (hits['win_odds'] < hi)]['win_odds'].sum()
        pct = count / total_hits * 100
        contribution = odds_sum / len(bet_df) * 100  # ROIへの貢献
        bar = '█' * int(contribution / 2)
        print(f"  {lo:>2}-{hi:>3}倍: {count:>4}件 ({pct:>5.1f}%) → ROI貢献: {contribution:>5.1f}% {bar}")
    
    # 人気分布
    print("\n■ 的中馬の人気分布")
    print("-" * 50)
    
    if 'popularity' in hits.columns:
        for pop in range(1, 11):
            count = (hits['popularity'] == pop).sum()
            pct = count / total_hits * 100
            bar = '█' * int(pct / 2)
            print(f"  {pop:>2}番人気: {count:>4}件 ({pct:>5.1f}%) {bar}")


def analyze_model_independence(bet_df, all_df):
    """モデルの独自性分析"""
    print("\n" + "=" * 80)
    print("【モデル独自性分析】")
    print("=" * 80)
    
    if 'popularity' not in bet_df.columns:
        print("人気データがありません")
        return
    
    # モデル選択と人気の関係
    print("\n■ モデル選択 vs 人気順位（全レース）")
    print("-" * 60)
    
    # 各人気の馬がモデルTop1に選ばれた割合
    total = len(bet_df)
    for pop in range(1, 11):
        count = (bet_df['popularity'] == pop).sum()
        hits = bet_df[(bet_df['popularity'] == pop) & bet_df['is_hit']]
        hit_rate = len(hits) / count * 100 if count > 0 else 0
        roi = hits['win_odds'].sum() / count * 100 if count > 0 else 0
        print(f"  {pop:>2}番人気を選択: {count:>4}件 ({count/total*100:>5.1f}%) → 的中率 {hit_rate:>5.1f}%, ROI {roi:>6.1f}%")
    
    # 人気順位とモデル順位の一致度
    print("\n■ モデルの独自選択能力")
    print("-" * 60)
    
    agree = (bet_df['popularity'] == 1).sum()
    disagree = (bet_df['popularity'] != 1).sum()
    
    agree_hits = bet_df[(bet_df['popularity'] == 1) & bet_df['is_hit']]
    disagree_hits = bet_df[(bet_df['popularity'] != 1) & bet_df['is_hit']]
    
    print(f"  1番人気を選択した回数: {agree:>5}件 ({agree/total*100:.1f}%)")
    print(f"    → 的中率: {len(agree_hits)/agree*100:.1f}%, ROI: {agree_hits['win_odds'].sum()/agree*100:.1f}%")
    print(f"  1番人気以外を選択した回数: {disagree:>5}件 ({disagree/total*100:.1f}%)")
    print(f"    → 的中率: {len(disagree_hits)/disagree*100:.1f}%, ROI: {disagree_hits['win_odds'].sum()/disagree*100:.1f}%")
    
    # 人気順位別のモデル優位性
    print("\n■ モデルが人気Nより上位と判断した馬の分析")
    print("-" * 60)
    
    # モデルTop1が市場で何番人気か、その人気帯ごとの成績
    for pop_range, label in [((2,3), '2-3番人気'), ((4,6), '4-6番人気'), ((7,10), '7-10番人気'), ((11,99), '11番人気～')]:
        mask = (bet_df['popularity'] >= pop_range[0]) & (bet_df['popularity'] <= pop_range[1])
        subset = bet_df[mask]
        if len(subset) > 0:
            subset_hits = subset[subset['is_hit']]
            roi = subset_hits['win_odds'].sum() / len(subset) * 100
            hr = len(subset_hits) / len(subset) * 100
            avg_odds = subset_hits['win_odds'].mean() if len(subset_hits) > 0 else 0
            print(f"  モデルTop1が{label}: {len(subset):>4}件 → 的中率 {hr:>5.1f}%, ROI {roi:>6.1f}%, 平均オッズ {avg_odds:>5.1f}倍")


def analyze_by_condition_segments(bet_df, all_df):
    """条件セグメント別の大規模分析"""
    print("\n" + "=" * 80)
    print("【条件セグメント別分析】（全レース対象）")
    print("=" * 80)
    
    total = len(bet_df)
    
    # 出走頭数別
    print("\n■ 出走頭数別")
    print("-" * 60)
    
    if 'field_size' in all_df.columns:
        bet_df_fs = bet_df.merge(all_df[['race_id', 'horse_number', 'field_size']].drop_duplicates(['race_id', 'horse_number']), 
                                  on=['race_id', 'horse_number'], how='left', suffixes=('', '_y'))
        if 'field_size_y' in bet_df_fs.columns:
            bet_df_fs['field_size'] = bet_df_fs['field_size_y']
        
        for fs_min, fs_max, label in [(5, 8, '5-8頭'), (9, 12, '9-12頭'), (13, 14, '13-14頭'), (15, 18, '15-18頭')]:
            mask = (bet_df_fs['field_size'] >= fs_min) & (bet_df_fs['field_size'] <= fs_max)
            subset = bet_df_fs[mask]
            if len(subset) > 50:
                hits = subset[subset['is_hit']]
                roi = hits['win_odds'].sum() / len(subset) * 100
                hr = len(hits) / len(subset) * 100
                print(f"  {label}: {len(subset):>5}件 → 的中率 {hr:>5.1f}%, ROI {roi:>6.1f}%")
    
    # 競馬場別
    print("\n■ 競馬場別")
    print("-" * 60)
    
    if 'venue' in bet_df.columns:
        venue_stats = []
        for venue in bet_df['venue'].dropna().unique():
            subset = bet_df[bet_df['venue'] == venue]
            if len(subset) > 100:
                hits = subset[subset['is_hit']]
                roi = hits['win_odds'].sum() / len(subset) * 100
                venue_stats.append({'venue': venue, 'count': len(subset), 'roi': roi})
        
        venue_df = pd.DataFrame(venue_stats).sort_values('roi', ascending=False)
        for _, row in venue_df.iterrows():
            print(f"  {row['venue']:>4}: {row['count']:>4}件 → ROI {row['roi']:>6.1f}%")
    
    # 月別
    print("\n■ 月別")
    print("-" * 60)
    
    bet_df['month'] = bet_df['race_date'].dt.month
    for month in range(1, 13):
        subset = bet_df[bet_df['month'] == month]
        if len(subset) > 50:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100
            hr = len(hits) / len(subset) * 100
            print(f"  {month:>2}月: {len(subset):>4}件 → 的中率 {hr:>5.1f}%, ROI {roi:>6.1f}%")


def main():
    print("=" * 80)
    print("大規模パターン分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件")
    
    # 直近2年を分析
    for year in ['2025', '2024']:
        print(f"\n\n{'#'*80}")
        print(f"# {year}年 大規模パターン分析")
        print(f"{'#'*80}")
        
        if year == '2025':
            train_end, test_start, test_end = '2023-12-31', '2025-01-01', '2025-12-31'
            valid_start, valid_end = '2024-01-01', '2024-12-31'
        else:
            train_end, test_start, test_end = '2022-12-31', '2024-01-01', '2024-12-31'
            valid_start, valid_end = '2023-01-01', '2023-12-31'
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # 特徴量生成
        print("特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # モデル学習
        print("モデル学習中...")
        model = train_v15(train_f, valid_f, feature_cols)
        
        # 予測
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # 包括的パターン分析
        bet_df, all_df = comprehensive_pattern_analysis(test_f, preds, feature_cols, model)
        
        # 各種分析
        analyze_success_patterns(bet_df)
        analyze_failure_patterns(bet_df, all_df)
        analyze_model_independence(bet_df, all_df)
        analyze_by_condition_segments(bet_df, all_df)
    
    print("\n\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
