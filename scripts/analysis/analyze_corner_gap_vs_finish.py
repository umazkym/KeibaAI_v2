"""
コーナー馬身差 vs 着順 深層分析

【分析目的】
1. コーナー馬身差と着順の関係を検証
2. 「追い込み力」（gap_reduction）がROIに貢献するか検証
3. コーナー馬身差と人気の相関を確認（コンセンサストラップ検出）
4. 市場が見落としているパターンを発見

【仮説】
- コーナー位置が良い馬 ≈ 人気馬 → ROI悪化の可能性
- しかし「どれだけ追い込んだか」は市場で織り込まれにくい
- 「C4馬身差 - 着順」の差が大きい馬は undervalued かもしれない

【リーク注意】
- 分析は過去データのみで実施
- 特徴量として使う場合は shift(1) を適用

使用方法:
python scripts/analysis/analyze_corner_gap_vs_finish.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    logger.info("=" * 60)
    logger.info("データ読み込み")
    logger.info("=" * 60)
    
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    
    logger.info(f"  レース: {len(races):,}件")
    logger.info(f"  コーナー: {len(corners):,}件")
    
    return races, corners


def prepare_corner_data(races, corners):
    """コーナーデータと着順を結合"""
    logger.info("\n" + "=" * 60)
    logger.info("データ前処理")
    logger.info("=" * 60)
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # C1, C4データ取得
    c1 = corners[corners['corner'] == 1][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c1.columns = ['race_id', 'horse_number', 'c1_pos', 'c1_gap']
    
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
    
    # 結合
    merged = races.merge(c1, on=['race_id', 'horse_number'], how='left')
    merged = merged.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # C1, C4両方あるデータのみ
    valid = merged[merged['c1_gap'].notna() & merged['c4_gap'].notna()].copy()
    
    logger.info(f"  有効データ: {len(valid):,}件 ({len(valid)/len(races)*100:.1f}%)")
    
    # 出走頭数
    field_size = valid.groupby('race_id').size().reset_index(name='field_size')
    valid = valid.merge(field_size, on='race_id', how='left')
    
    # 相対着順（0-1スケール）
    valid['relative_finish'] = (valid['finish_position'] - 1) / (valid['field_size'] - 1)
    
    # 追い込み力（C1→C4のギャップ改善）
    valid['gap_reduction_c1_c4'] = valid['c1_gap'] - valid['c4_gap']
    
    # 最終直線での追い込み（C4→着順の関係）
    # ※ 着順は馬身差ではないので直接比較は困難
    # → 代わりに「C4位置 - 着順」を見る
    valid['c4_to_finish_improvement'] = valid['c4_pos'] - valid['finish_position']
    
    return valid


def analyze_correlation_with_popularity(df):
    """人気との相関分析"""
    logger.info("\n" + "=" * 60)
    logger.info("1. 人気との相関分析（コンセンサストラップ検出）")
    logger.info("=" * 60)
    
    if 'popularity' not in df.columns:
        logger.warning("  popularityカラムがありません")
        return
    
    valid = df[df['popularity'].notna()].copy()
    
    # 相関係数
    correlations = {
        'c1_gap vs popularity': valid['c1_gap'].corr(valid['popularity']),
        'c4_gap vs popularity': valid['c4_gap'].corr(valid['popularity']),
        'gap_reduction vs popularity': valid['gap_reduction_c1_c4'].corr(valid['popularity']),
        'c4_to_finish_improvement vs popularity': valid['c4_to_finish_improvement'].corr(valid['popularity']),
        'c1_gap vs win_odds': valid['c1_gap'].corr(valid['win_odds']),
        'c4_gap vs win_odds': valid['c4_gap'].corr(valid['win_odds']),
        'gap_reduction vs win_odds': valid['gap_reduction_c1_c4'].corr(valid['win_odds']),
    }
    
    logger.info("\n  相関係数:")
    for name, corr in correlations.items():
        logger.info(f"    {name}: {corr:.3f}")
    
    # 解釈
    logger.info("\n  【解釈】")
    if abs(correlations.get('c4_gap vs popularity', 0)) > 0.3:
        logger.info("  ⚠️ c4_gapは人気と強い相関 → 市場に織り込まれている可能性")
    else:
        logger.info("  ✓ c4_gapと人気の相関は限定的")
    
    if abs(correlations.get('gap_reduction vs popularity', 0)) < 0.2:
        logger.info("  ✓ gap_reductionは人気との相関が低い → 市場が見落としている可能性")


def analyze_gap_vs_finish(df):
    """コーナーギャップ vs 着順の関係分析"""
    logger.info("\n" + "=" * 60)
    logger.info("2. コーナーギャップ vs 着順の関係")
    logger.info("=" * 60)
    
    # C4ギャップカテゴリ
    df['c4_gap_cat'] = pd.cut(
        df['c4_gap'],
        bins=[-0.1, 0.5, 2.0, 5.0, 10.0, np.inf],
        labels=['先頭(0-0.5)', '好位(0.5-2)', '中団(2-5)', '後方(5-10)', '大後方(10+)']
    )
    
    # 各カテゴリの着順分布
    logger.info("\n  C4ギャップ別 着順・勝率・ROI:")
    gap_stats = df.groupby('c4_gap_cat', observed=True).agg(
        count=('finish_position', 'count'),
        avg_finish=('finish_position', 'mean'),
        win_rate=('finish_position', lambda x: (x == 1).mean() * 100),
        avg_odds=('win_odds', 'mean'),
    ).reset_index()
    gap_stats['roi'] = (gap_stats['win_rate'] / 100) * gap_stats['avg_odds'] * 100
    
    print(gap_stats.to_string(index=False))
    
    # 着順との相関
    corr = df['c4_gap'].corr(df['finish_position'])
    logger.info(f"\n  C4ギャップ vs 着順の相関: {corr:.3f}")
    logger.info("  → 相関が高い = C4で前にいるほど最終着順が良い（当然）")


def analyze_gap_improvement_roi(df):
    """追い込み力とROIの関係分析"""
    logger.info("\n" + "=" * 60)
    logger.info("3. 追い込み力（Gap Reduction）とROIの関係")
    logger.info("=" * 60)
    
    # Gap Reductionカテゴリ
    df['reduction_cat'] = pd.cut(
        df['gap_reduction_c1_c4'],
        bins=[-np.inf, -3, -1, 0, 2, 5, np.inf],
        labels=['大後退(<-3)', '後退(-3~-1)', '微後退(-1~0)', '維持(0~2)', '追込(2~5)', '大追込(>5)']
    )
    
    logger.info("\n  Gap Reduction別 勝率・ROI:")
    reduction_stats = df.groupby('reduction_cat', observed=True).agg(
        count=('finish_position', 'count'),
        avg_finish=('finish_position', 'mean'),
        win_rate=('finish_position', lambda x: (x == 1).mean() * 100),
        avg_odds=('win_odds', 'mean'),
    ).reset_index()
    reduction_stats['roi'] = (reduction_stats['win_rate'] / 100) * reduction_stats['avg_odds'] * 100
    
    print(reduction_stats.to_string(index=False))
    
    # 解釈
    logger.info("\n  【解釈】")
    logger.info("  - '追込'カテゴリ（C1→C4で馬身差縮小）のROIを確認")
    logger.info("  - ROI > 80%なら「市場が過小評価」の可能性")


def analyze_c4_finish_gap(df):
    """C4位置→着順の改善度分析"""
    logger.info("\n" + "=" * 60)
    logger.info("4. C4位置→着順の改善度（直線での追い込み）")
    logger.info("=" * 60)
    
    # C4→着順改善度カテゴリ
    df['c4_finish_cat'] = pd.cut(
        df['c4_to_finish_improvement'],
        bins=[-np.inf, -3, -1, 0, 2, 5, np.inf],
        labels=['大失速(<-3)', '失速(-3~-1)', '微失速(-1~0)', '維持(0~2)', '強襲(2~5)', '大強襲(>5)']
    )
    
    logger.info("\n  C4→着順改善度別 勝率・ROI:")
    c4_finish_stats = df.groupby('c4_finish_cat', observed=True).agg(
        count=('finish_position', 'count'),
        avg_finish=('finish_position', 'mean'),
        win_rate=('finish_position', lambda x: (x == 1).mean() * 100),
        avg_odds=('win_odds', 'mean'),
        avg_popularity=('popularity', 'mean'),
    ).reset_index()
    c4_finish_stats['roi'] = (c4_finish_stats['win_rate'] / 100) * c4_finish_stats['avg_odds'] * 100
    
    print(c4_finish_stats.to_string(index=False))
    
    # 「維持」カテゴリの詳細分析
    logger.info("\n  【重要な発見】")
    logger.info("  - '強襲'（C4位置より着順が良い）馬のROIは？")
    logger.info("  - このパターンは前走で「追い込み力がある」馬の指標になる")


def analyze_feature_candidate(df):
    """新特徴量候補の検討"""
    logger.info("\n" + "=" * 60)
    logger.info("5. 特徴量候補の検討")
    logger.info("=" * 60)
    
    # 馬ごとの過去の「C4→着順改善度」平均を計算
    df = df.sort_values(['horse_id', 'race_date', 'race_id'])
    
    df['cum_c4_finish_improvement'] = df.groupby('horse_id')['c4_to_finish_improvement'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    df['cum_count'] = df.groupby('horse_id')['c4_to_finish_improvement'].transform(
        lambda x: x.expanding().count().shift(1)
    )
    
    # 最小レース数でフィルタ
    df.loc[df['cum_count'] < 3, 'cum_c4_finish_improvement'] = np.nan
    
    # 有効データ
    valid = df[df['cum_c4_finish_improvement'].notna()].copy()
    logger.info(f"\n  有効データ: {len(valid):,}件")
    
    # 人気との相関
    corr_pop = valid['cum_c4_finish_improvement'].corr(valid['popularity'])
    corr_odds = valid['cum_c4_finish_improvement'].corr(valid['win_odds'])
    logger.info(f"  累積C4→着順改善度 vs 人気: {corr_pop:.3f}")
    logger.info(f"  累積C4→着順改善度 vs オッズ: {corr_odds:.3f}")
    
    # ROI分析
    valid['improvement_cat'] = pd.cut(
        valid['cum_c4_finish_improvement'],
        bins=[-np.inf, -1, 0, 1, 2, np.inf],
        labels=['大失速傾向(<-1)', '失速傾向(-1~0)', '維持傾向(0~1)', '追込傾向(1~2)', '大追込傾向(>2)']
    )
    
    logger.info("\n  累積C4→着順改善度別 ROI（過去傾向から予測）:")
    improvement_stats = valid.groupby('improvement_cat', observed=True).agg(
        count=('finish_position', 'count'),
        win_rate=('finish_position', lambda x: (x == 1).mean() * 100),
        avg_odds=('win_odds', 'mean'),
        avg_popularity=('popularity', 'mean'),
    ).reset_index()
    improvement_stats['roi'] = (improvement_stats['win_rate'] / 100) * improvement_stats['avg_odds'] * 100
    
    print(improvement_stats.to_string(index=False))
    
    # 結論
    logger.info("\n  【結論】")
    logger.info("  - 累積C4→着順改善度と人気の相関を確認")
    logger.info("  - 相関が低く、ROIが高いカテゴリがあれば特徴量として有効")
    
    return df


def main():
    """メイン処理"""
    logger.info("コーナー馬身差 vs 着順 深層分析開始")
    
    # データ読み込み
    races, corners = load_data()
    
    # データ前処理
    df = prepare_corner_data(races, corners)
    
    # 分析実行
    analyze_correlation_with_popularity(df)
    analyze_gap_vs_finish(df)
    analyze_gap_improvement_roi(df)
    analyze_c4_finish_gap(df)
    df = analyze_feature_candidate(df)
    
    logger.info("\n" + "=" * 60)
    logger.info("分析完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
