#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
包括的分析: Top1-5ランクとオッズ・的中の関係

分析内容:
1. Top1-5の予測ランク vs 実際の着順
2. Top1-5の予測ランク vs オッズ分布
3. オッズ帯別の的中率・回収率
4. 「狙い目」の発見
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

def load_base_data():
    """データ読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    return races, pedigrees, corners, race_details, horses, returns


def run_prediction(races, pedigrees, corners, race_details, horses, year):
    """1年分の予測を実行"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_start = pd.to_datetime(f'{year}-01-01')
    test_end = pd.to_datetime(f'{year}-12-31')
    train_end = test_start - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
    
    if len(test) < 5000:
        return None
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start) & (all_data_f['race_date'] <= test_end)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_f = margin_engineer.transform(test_f)
    
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    test_preds = predictor.predict(test_f)
    
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                'race_date', 'venue', 'race_name']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = year
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    return test_preds


def analyze_rank_vs_finish(preds_df):
    """Top1-5ランクと実際の着順の関係"""
    print("\n" + "="*70)
    print("【分析1】予測ランク vs 実着順")
    print("="*70)
    
    results = []
    for rank in range(1, 6):
        rank_df = preds_df[preds_df['rank'] == rank]
        
        first = (rank_df['finish_position'] == 1).sum()
        top2 = (rank_df['finish_position'] <= 2).sum()
        top3 = (rank_df['finish_position'] <= 3).sum()
        total = len(rank_df)
        
        results.append({
            'rank': f'Top{rank}',
            '1着率': first / total * 100,
            '2着以内率': top2 / total * 100,
            '3着以内率': top3 / total * 100,
            '件数': total,
        })
    
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    return df


def analyze_rank_vs_odds(preds_df):
    """Top1-5ランクとオッズ分布の関係"""
    print("\n" + "="*70)
    print("【分析2】予測ランク vs オッズ分布")
    print("="*70)
    
    results = []
    for rank in range(1, 6):
        rank_df = preds_df[preds_df['rank'] == rank]
        
        odds = rank_df['win_odds']
        results.append({
            'rank': f'Top{rank}',
            '平均オッズ': odds.mean(),
            '中央値': odds.median(),
            '10倍未満率': (odds < 10).sum() / len(odds) * 100,
            '10-30倍率': ((odds >= 10) & (odds < 30)).sum() / len(odds) * 100,
            '30倍以上率': (odds >= 30).sum() / len(odds) * 100,
        })
    
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    return df


def analyze_odds_band_roi(preds_df):
    """オッズ帯別のROI分析"""
    print("\n" + "="*70)
    print("【分析3】オッズ帯別ROI（単勝 Top1）")
    print("="*70)
    
    top1 = preds_df[preds_df['rank'] == 1]
    
    bands = [
        ('1-3倍', 1, 3),
        ('3-5倍', 3, 5),
        ('5-10倍', 5, 10),
        ('10-20倍', 10, 20),
        ('20-50倍', 20, 50),
        ('50倍以上', 50, 1000),
    ]
    
    results = []
    for name, low, high in bands:
        band_df = top1[(top1['win_odds'] >= low) & (top1['win_odds'] < high)]
        
        if len(band_df) == 0:
            continue
        
        bets = len(band_df)
        hits = (band_df['finish_position'] == 1).sum()
        returns = band_df[band_df['finish_position'] == 1]['win_odds'].sum()
        roi = returns / bets * 100
        hit_rate = hits / bets * 100
        
        results.append({
            'オッズ帯': name,
            '件数': bets,
            '的中率': hit_rate,
            'ROI': roi,
            '評価': '★' if roi >= 80 else '',
        })
    
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    return df


def analyze_rank_combination_hitrate(preds_df, returns_df):
    """Top1-5の組み合わせ別 馬連的中率とROI"""
    print("\n" + "="*70)
    print("【分析4】Top1-5組み合わせ別 馬連的中率・ROI")
    print("="*70)
    
    # 馬連払戻データ
    umaren = returns_df[returns_df['bet_type'] == 'umaren'].copy()
    umaren_dict = {}
    for _, row in umaren.iterrows():
        if pd.notna(row['horse_1']) and pd.notna(row['horse_2']):
            pair = tuple(sorted([int(row['horse_1']), int(row['horse_2'])]))
            umaren_dict[(row['race_id'], pair)] = row['payout']
    
    results = []
    
    # Top1-Top2 ~ Top4-Top5
    for i in range(1, 5):
        for j in range(i+1, 6):
            total_bets = 0
            total_hits = 0
            total_return = 0
            
            for race_id in preds_df['race_id'].unique():
                race_df = preds_df[preds_df['race_id'] == race_id]
                
                h1 = race_df[race_df['rank'] == i]
                h2 = race_df[race_df['rank'] == j]
                
                if len(h1) == 0 or len(h2) == 0:
                    continue
                
                horse1 = int(h1.iloc[0]['horse_number'])
                horse2 = int(h2.iloc[0]['horse_number'])
                pair = tuple(sorted([horse1, horse2]))
                
                total_bets += 1
                
                payout = umaren_dict.get((race_id, pair), 0)
                if payout > 0:
                    total_hits += 1
                    total_return += payout / 100
            
            if total_bets > 0:
                hit_rate = total_hits / total_bets * 100
                roi = total_return / total_bets * 100
                
                results.append({
                    '組み合わせ': f'Top{i}-Top{j}',
                    '件数': total_bets,
                    '的中率': hit_rate,
                    'ROI': roi,
                    '評価': '★' if roi >= 70 else '',
                })
    
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    return df


def analyze_popularity_vs_prediction(preds_df):
    """人気順 vs 予測順位の乖離分析"""
    print("\n" + "="*70)
    print("【分析5】人気順 vs AI予測ランクの乖離")
    print("="*70)
    
    # Top1がどの人気に分布しているか
    top1 = preds_df[preds_df['rank'] == 1]
    
    pop_dist = top1['popularity'].value_counts().sort_index().head(10)
    print("\n■ Top1（AI予測1位）の人気分布:")
    for pop, count in pop_dist.items():
        pct = count / len(top1) * 100
        bar = '█' * int(pct / 5)
        print(f"  {int(pop):2}番人気: {count:4}件 ({pct:5.1f}%) {bar}")
    
    # 人気とAI予測が一致した場合 vs 乖離した場合のROI
    print("\n■ 人気とAI予測の一致 vs 乖離時のROI（単勝Top1）:")
    
    # 一致: AI Top1 = 1番人気
    match = top1[top1['popularity'] == 1]
    match_bets = len(match)
    match_return = match[match['finish_position'] == 1]['win_odds'].sum()
    match_roi = match_return / match_bets * 100 if match_bets > 0 else 0
    
    # 乖離: AI Top1 ≠ 1番人気
    diff = top1[top1['popularity'] != 1]
    diff_bets = len(diff)
    diff_return = diff[diff['finish_position'] == 1]['win_odds'].sum()
    diff_roi = diff_return / diff_bets * 100 if diff_bets > 0 else 0
    
    print(f"  一致時（AI Top1 = 1番人気）: ROI {match_roi:.1f}% ({match_bets}件)")
    print(f"  乖離時（AI Top1 ≠ 1番人気）: ROI {diff_roi:.1f}% ({diff_bets}件)")
    
    return {'match_roi': match_roi, 'diff_roi': diff_roi}


def main():
    print("="*70)
    print("【包括的分析】Top1-5ランクとオッズ・的中の関係")
    print("="*70)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_base_data()
    print(f"データ件数: {len(races):,}")
    
    # 2024年のみで分析（高速化）
    print("\n[2024年] 予測中...")
    preds = run_prediction(races, pedigrees, corners, race_details, horses, 2024)
    print(f"予測完了: {len(preds):,}件")
    
    # 分析実行
    analyze_rank_vs_finish(preds)
    analyze_rank_vs_odds(preds)
    analyze_odds_band_roi(preds)
    analyze_rank_combination_hitrate(preds, returns)
    divergence = analyze_popularity_vs_prediction(preds)
    
    # 結論
    print("\n" + "="*70)
    print("【結論と改善の方向性】")
    print("="*70)
    print(f"""
■ ROI改善の鍵は「人気との乖離」
  - AI Top1 = 1番人気 → ROI {divergence['match_roi']:.1f}% （オッズ低いので期待薄）
  - AI Top1 ≠ 1番人気 → ROI {divergence['diff_roi']:.1f}% （AIの真価が出る場面）

■ 改善戦略の方向性
  1. 「AIが1番人気と異なる馬をTop1に選んだ」レースに絞る
  2. 中穴帯（10-30倍）で高的中率を維持する
  3. Top3-Top4など人気薄組み合わせで馬連を狙う
""")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    preds.to_csv(output_dir / "comprehensive_analysis_preds.csv", index=False, encoding='utf-8-sig')
    print(f"\n予測結果保存: comprehensive_analysis_preds.csv")


if __name__ == "__main__":
    main()
