# -*- coding: utf-8 -*-
"""
レース単位 多角的詳細分析

【目的】
レースごとにモデル予測×オッズ×実着順の関係を詳細に分析し、
ROI改善に繋がる傾向やパターンを発見する。

【分析項目】
1. 予測順位別の着順分布と的中率
2. オッズ帯別の予測精度とROI
3. 予測順位×オッズ帯のクロス分析
4. 的中レース vs ハズレレースの特徴差
5. 人気馬が負けるパターンの分析
6. 穴馬が来るパターンの分析
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def add_pred_rank(df):
    """予測スコアで順位付け"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    return df


def analyze_pred_rank_vs_finish(df):
    """1. 予測順位別の着順分布"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 予測順位別の着順分布")
    logger.info("=" * 60)
    
    for pred_r in range(1, 6):
        subset = df[df['pred_rank'] == pred_r]
        total = len(subset)
        
        finish_dist = []
        for finish_p in range(1, 6):
            count = len(subset[subset['finish_position'] == finish_p])
            pct = count / total * 100 if total > 0 else 0
            finish_dist.append(f"{finish_p}着:{pct:.1f}%")
        
        win_rate = len(subset[subset['finish_position'] == 1]) / total * 100
        top3_rate = len(subset[subset['finish_position'] <= 3]) / total * 100
        
        logger.info(f"\n  予測{pred_r}位 (n={total:,})")
        logger.info(f"    着順分布: {', '.join(finish_dist)}")
        logger.info(f"    1着率: {win_rate:.1f}%, 3着内率: {top3_rate:.1f}%")


def analyze_odds_bands(df):
    """2. オッズ帯別の予測精度とROI"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. オッズ帯別の予測精度とROI (予測1位馬)")
    logger.info("=" * 60)
    
    top1 = df[df['pred_rank'] == 1].copy()
    
    # オッズ帯を定義
    odds_bands = [
        (1.0, 2.0, "1.0-2.0"),
        (2.0, 3.0, "2.0-3.0"),
        (3.0, 5.0, "3.0-5.0"),
        (5.0, 10.0, "5.0-10.0"),
        (10.0, 20.0, "10.0-20.0"),
        (20.0, 50.0, "20.0-50.0"),
        (50.0, 100.0, "50.0-100.0"),
        (100.0, float('inf'), "100.0+"),
    ]
    
    logger.info(f"\n  {'オッズ帯':12} {'件数':>8} {'1着率':>8} {'ROI':>8} {'Avg払戻':>10}")
    logger.info("  " + "-" * 55)
    
    for low, high, label in odds_bands:
        subset = top1[(top1['win_odds'] >= low) & (top1['win_odds'] < high)]
        n = len(subset)
        if n == 0:
            continue
            
        wins = subset[subset['finish_position'] == 1]
        win_rate = len(wins) / n * 100
        roi = wins['win_odds'].sum() / n * 100
        avg_payout = wins['win_odds'].mean() * 100 if len(wins) > 0 else 0
        
        logger.info(f"  {label:12} {n:>8,} {win_rate:>7.1f}% {roi:>7.1f}% {avg_payout:>9,.0f}円")


def analyze_cross_pred_odds(df):
    """3. 予測順位×オッズ帯のクロス分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. 予測順位×オッズ帯のクロス分析 (1着率)")
    logger.info("=" * 60)
    
    # オッズを4区分
    df = df.copy()
    df['odds_cat'] = pd.cut(
        df['win_odds'],
        bins=[0, 3, 10, 30, float('inf')],
        labels=['低(~3)', '中(3~10)', '高(10~30)', '超高(30~)']
    )
    
    logger.info(f"\n  {'':8} {'低(~3)':>10} {'中(3~10)':>10} {'高(10~30)':>10} {'超高(30~)':>10}")
    logger.info("  " + "-" * 50)
    
    for pred_r in range(1, 6):
        row = [f"  予測{pred_r}位"]
        for cat in ['低(~3)', '中(3~10)', '高(10~30)', '超高(30~)']:
            subset = df[(df['pred_rank'] == pred_r) & (df['odds_cat'] == cat)]
            n = len(subset)
            if n > 0:
                win_rate = len(subset[subset['finish_position'] == 1]) / n * 100
                row.append(f"{win_rate:5.1f}%({n:,})")
            else:
                row.append("-")
        logger.info("  ".join(row))


def analyze_hit_vs_miss(df):
    """4. 的中レース vs ハズレレースの特徴差"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 的中レース vs ハズレレースの特徴差")
    logger.info("=" * 60)
    
    # 各レースでTop1が当たったか
    race_results = df.groupby('race_id').apply(
        lambda x: (x[x['pred_rank'] == 1]['finish_position'].iloc[0] == 1) if len(x[x['pred_rank'] == 1]) > 0 else False
    ).reset_index()
    race_results.columns = ['race_id', 'hit']
    
    df = df.merge(race_results, on='race_id')
    
    # 的中レースの特徴
    hit_races = df[df['hit'] == True]
    miss_races = df[df['hit'] == False]
    
    # Top1とTop2のオッズ差
    def get_top2_odds_gap(group):
        sorted_g = group.sort_values('pred_rank')
        if len(sorted_g) >= 2:
            top1_odds = sorted_g.iloc[0]['win_odds']
            top2_odds = sorted_g.iloc[1]['win_odds']
            return top2_odds / top1_odds if top1_odds > 0 else 1
        return 1
    
    hit_gap = hit_races.groupby('race_id').apply(get_top2_odds_gap).mean()
    miss_gap = miss_races.groupby('race_id').apply(get_top2_odds_gap).mean()
    
    # Top1の平均オッズ
    hit_top1_odds = hit_races[hit_races['pred_rank'] == 1]['win_odds'].mean()
    miss_top1_odds = miss_races[miss_races['pred_rank'] == 1]['win_odds'].mean()
    
    # Top1の人気順位
    hit_top1_pop = hit_races[hit_races['pred_rank'] == 1]['popularity'].mean()
    miss_top1_pop = miss_races[miss_races['pred_rank'] == 1]['popularity'].mean()
    
    logger.info(f"\n  {'指標':25} {'的中':>12} {'ハズレ':>12} {'差':>10}")
    logger.info("  " + "-" * 60)
    logger.info(f"  {'Top1の平均オッズ':25} {hit_top1_odds:>11.1f}x {miss_top1_odds:>11.1f}x {hit_top1_odds - miss_top1_odds:>+9.1f}")
    logger.info(f"  {'Top1の平均人気順':25} {hit_top1_pop:>12.1f} {miss_top1_pop:>12.1f} {hit_top1_pop - miss_top1_pop:>+10.1f}")
    logger.info(f"  {'Top2/Top1オッズ比':25} {hit_gap:>12.2f} {miss_gap:>12.2f} {hit_gap - miss_gap:>+10.2f}")


def analyze_upset_patterns(df):
    """5. 人気馬が負けるパターンの分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("5. 人気馬（予測1位・オッズ低）が負けるパターン")
    logger.info("=" * 60)
    
    # 予測1位でオッズ3倍以下（本命）
    favorites = df[(df['pred_rank'] == 1) & (df['win_odds'] < 3)]
    fav_total = len(favorites)
    fav_miss = favorites[favorites['finish_position'] != 1]
    fav_miss_rate = len(fav_miss) / fav_total * 100 if fav_total > 0 else 0
    
    logger.info(f"\n  本命馬（予測1位・オッズ<3倍）: {fav_total:,}件")
    logger.info(f"  うちハズレ: {len(fav_miss):,}件 ({fav_miss_rate:.1f}%)")
    
    # ハズレた時の勝馬の特徴
    miss_race_ids = fav_miss['race_id'].unique()
    miss_df = df[df['race_id'].isin(miss_race_ids)]
    actual_winners = miss_df[miss_df['finish_position'] == 1]
    
    logger.info(f"\n  本命がハズレた時の勝馬の特徴:")
    logger.info(f"    平均オッズ: {actual_winners['win_odds'].mean():.1f}x")
    logger.info(f"    平均人気: {actual_winners['popularity'].mean():.1f}番人気")
    logger.info(f"    平均予測順位: {actual_winners['pred_rank'].mean():.1f}位")
    
    # 予測順位別の分布
    pred_dist = actual_winners['pred_rank'].value_counts().sort_index()
    logger.info(f"    予測順位分布: {dict(pred_dist.head(5))}")


def analyze_longshot_winners(df):
    """6. 穴馬が来るパターンの分析"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("6. 穴馬（オッズ10倍以上）が勝つパターン")
    logger.info("=" * 60)
    
    # オッズ10倍以上の勝馬
    longshots = df[(df['finish_position'] == 1) & (df['win_odds'] >= 10)]
    total_winners = len(df[df['finish_position'] == 1])
    
    logger.info(f"\n  穴馬勝利: {len(longshots):,}件 / {total_winners:,}件 ({len(longshots)/total_winners*100:.1f}%)")
    logger.info(f"  平均オッズ: {longshots['win_odds'].mean():.1f}x")
    
    # 穴馬の予測順位分布
    logger.info(f"\n  穴馬勝者の予測順位分布:")
    pred_dist = longshots['pred_rank'].value_counts().sort_index()
    cumulative = 0
    for rank in range(1, 8):
        if rank in pred_dist.index:
            count = pred_dist[rank]
            pct = count / len(longshots) * 100
            cumulative += pct
            logger.info(f"    予測{rank}位: {count:>4}件 ({pct:>5.1f}%) 累計{cumulative:.1f}%")
    
    # 穴馬を予測できているオッズ帯
    logger.info(f"\n  穴馬勝者のオッズ分布（予測Top3以内に挙げていた場合）:")
    predicted_longshots = longshots[longshots['pred_rank'] <= 3]
    odds_dist = pd.cut(
        predicted_longshots['win_odds'],
        bins=[10, 20, 30, 50, 100, float('inf')],
        labels=['10-20', '20-30', '30-50', '50-100', '100+']
    ).value_counts().sort_index()
    
    for label, count in odds_dist.items():
        logger.info(f"    オッズ{label}倍: {count:>4}件")


def analyze_confidence_vs_performance(df):
    """7. モデル確信度 vs 実パフォーマンス"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("7. モデル確信度（予測確率）vs 実パフォーマンス")
    logger.info("=" * 60)
    
    # 予測確率の分位で分類
    df = df.copy()
    df['prob_quintile'] = pd.qcut(df['pred'], q=5, labels=['最低20%', '低20%', '中20%', '高20%', '最高20%'])
    
    logger.info(f"\n  {'確信度':10} {'件数':>8} {'1着率':>8} {'平均オッズ':>10} {'ROI':>8}")
    logger.info("  " + "-" * 50)
    
    for q in ['最低20%', '低20%', '中20%', '高20%', '最高20%']:
        subset = df[df['prob_quintile'] == q]
        n = len(subset)
        wins = subset[subset['finish_position'] == 1]
        win_rate = len(wins) / n * 100 if n > 0 else 0
        avg_odds = subset['win_odds'].mean()
        roi = wins['win_odds'].sum() / n * 100 if n > 0 else 0
        
        logger.info(f"  {q:10} {n:>8,} {win_rate:>7.1f}% {avg_odds:>9.1f}x {roi:>7.1f}%")


def analyze_ev_distribution(df):
    """8. EV（期待値）分布と実ROI"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("8. EV（期待値）分布と実ROI")
    logger.info("=" * 60)
    
    df = df.copy()
    df['ev'] = df['pred'] * df['win_odds']
    
    ev_bands = [
        (0, 0.5, "EV 0-0.5"),
        (0.5, 0.8, "EV 0.5-0.8"),
        (0.8, 1.0, "EV 0.8-1.0"),
        (1.0, 1.2, "EV 1.0-1.2"),
        (1.2, 1.5, "EV 1.2-1.5"),
        (1.5, 2.0, "EV 1.5-2.0"),
        (2.0, float('inf'), "EV 2.0+"),
    ]
    
    logger.info(f"\n  {'EV帯':12} {'件数':>8} {'1着率':>8} {'平均オッズ':>10} {'ROI':>8}")
    logger.info("  " + "-" * 55)
    
    for low, high, label in ev_bands:
        subset = df[(df['ev'] >= low) & (df['ev'] < high)]
        n = len(subset)
        if n < 100:  # サンプル少なすぎる帯はスキップ
            continue
            
        wins = subset[subset['finish_position'] == 1]
        win_rate = len(wins) / n * 100
        avg_odds = subset['win_odds'].mean()
        roi = wins['win_odds'].sum() / n * 100
        
        logger.info(f"  {label:12} {n:>8,} {win_rate:>7.1f}% {avg_odds:>9.1f}x {roi:>7.1f}%")


def main():
    logger.info("=" * 60)
    logger.info("レース単位 多角的詳細分析")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
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
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_f['pred'] = model.predict(X_test)
    test_f = add_pred_rank(test_f)
    
    # 各種分析
    analyze_pred_rank_vs_finish(test_f)
    analyze_odds_bands(test_f)
    analyze_cross_pred_odds(test_f)
    analyze_hit_vs_miss(test_f)
    analyze_upset_patterns(test_f)
    analyze_longshot_winners(test_f)
    analyze_confidence_vs_performance(test_f)
    analyze_ev_distribution(test_f)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("分析完了")
    logger.info("=" * 60)
    
    return test_f


if __name__ == "__main__":
    main()
