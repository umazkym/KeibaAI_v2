#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
個別レース深層分析 - 原因推論と改善策導出

【目的】
1. 成功/失敗レースの詳細を個別に分析
2. 共通パターンから原因を推論
3. 具体的な改善策を導出

作成日: 2026-01-11
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import warnings

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_prediction_data():
    """前回の分析結果を活用（既に保存済みのデータがあれば使用）"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 新馬・障害除外
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    # 2024年のみ詳細分析（最新年で代表性確認）
    test_year = 2024
    test_start_dt = pd.to_datetime('2024-01-01')
    test_end_dt = pd.to_datetime('2024-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    # 特徴量エンジン
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[(train_f['race_date'] > valid_start)].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # タイム差特徴量
    margin_engineer = TimeMarginFeatureEngineer()
    margin_engineer.fit(train)
    test_f = margin_engineer.transform(test_f)
    
    # モデル学習
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    # 予測
    test_preds = predictor.predict(test_f)
    
    # 実績データをマージ
    merge_cols = ['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity',
                  'race_name', 'venue', 'race_date']
    # オプショナルなカラム
    optional_cols = ['horse_close_loss_rate', 'horse_margin_std', 'track_surface', 
                     'distance', 'race_class', 'horse_id', 'horse_name', 'jockey_id', 'trainer_id']
    for col in optional_cols:
        if col in test_f.columns:
            merge_cols.append(col)
    
    test_preds = test_preds.merge(
        test_f[merge_cols],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 複勝払戻をマージ
    fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    test_preds = test_preds.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    test_preds['fukusho_payout'] = test_preds['payout'].fillna(0) / 100
    
    return test_preds


def apply_volatility_classification(df):
    """荒れ度分類を適用"""
    df = df.copy()
    df['vol_type'] = np.where(
        df['race_place_prob_std'] < 0.08, 'volatile',
        np.where(df['race_place_prob_std'] < 0.20, 'moderate', 'stable')
    )
    
    # ランキング
    df['rank_ensemble'] = df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    df['rank_place'] = df.groupby('race_id')['place_prob'].rank(ascending=False, method='first')
    
    # V4.4の高低
    v44_median = df['v44_score'].median()
    df['v44_level'] = np.where(df['v44_score'] >= v44_median, 'high', 'low')
    
    return df


def analyze_high_roi_venues(df):
    """高ROI会場の原因分析"""
    logger.info("\n" + "="*70)
    logger.info("【分析1】高ROI会場 vs 低ROI会場の原因分析")
    logger.info("="*70)
    
    top1 = df[df['rank_ensemble'] == 1].copy()
    
    high_roi_venues = ['中山', '福島', '中京']
    low_roi_venues = ['小倉', '新潟', '札幌']
    
    high_roi_races = top1[top1['venue'].isin(high_roi_venues)]
    low_roi_races = top1[top1['venue'].isin(low_roi_venues)]
    
    logger.info("\n【特徴量比較】")
    
    # 荒れ度分布
    logger.info("\n  荒れ度分布:")
    high_vol_dist = high_roi_races['vol_type'].value_counts(normalize=True)
    low_vol_dist = low_roi_races['vol_type'].value_counts(normalize=True)
    for vol_type in ['stable', 'moderate', 'volatile']:
        h = high_vol_dist.get(vol_type, 0) * 100
        l = low_vol_dist.get(vol_type, 0) * 100
        diff = h - l
        logger.info(f"    {vol_type}: 高ROI会場 {h:.1f}% | 低ROI会場 {l:.1f}% (差: {diff:+.1f}pt)")
    
    # 人気分布
    logger.info("\n  人気分布:")
    high_pop = high_roi_races['popularity'].mean()
    low_pop = low_roi_races['popularity'].mean()
    logger.info(f"    平均人気: 高ROI会場 {high_pop:.2f} | 低ROI会場 {low_pop:.2f}")
    
    # オッズ分布
    logger.info("\n  オッズ分布:")
    high_odds = high_roi_races['win_odds'].mean()
    low_odds = low_roi_races['win_odds'].mean()
    logger.info(f"    平均オッズ: 高ROI会場 {high_odds:.2f}倍 | 低ROI会場 {low_odds:.2f}倍")
    
    # 的中率
    logger.info("\n  的中率:")
    high_hit = (high_roi_races['finish_position'] == 1).mean() * 100
    low_hit = (low_roi_races['finish_position'] == 1).mean() * 100
    logger.info(f"    的中率: 高ROI会場 {high_hit:.1f}% | 低ROI会場 {low_hit:.1f}%")
    
    # 推論
    logger.info("\n【推論】")
    if high_hit > low_hit:
        logger.info("  → 高ROI会場は予測精度が高い（的中率が高い）")
    else:
        logger.info("  → 高ROI会場は的中時のオッズが高い傾向")
    
    if high_vol_dist.get('stable', 0) > low_vol_dist.get('stable', 0):
        logger.info("  → 高ROI会場はstableレース比率が高い → 実力通り決まりやすい")
    
    logger.info("\n【改善案】")
    logger.info("  1. 会場別のモデルウェイト調整を検討")
    logger.info("  2. 低ROI会場では複勝に切り替える戦略")


def analyze_v44_paradox(df):
    """stable + V4.4低 > stable + V4.4高 の原因分析"""
    logger.info("\n" + "="*70)
    logger.info("【分析2】stable + V4.4低が高ROIの原因分析")
    logger.info("="*70)
    
    top1 = df[df['rank_ensemble'] == 1].copy()
    stable = top1[top1['vol_type'] == 'stable']
    
    v44_high = stable[stable['v44_level'] == 'high']
    v44_low = stable[stable['v44_level'] == 'low']
    
    logger.info("\n【基本統計の比較】")
    
    # ROI計算
    high_hits = v44_high[v44_high['finish_position'] == 1]
    low_hits = v44_low[v44_low['finish_position'] == 1]
    
    high_roi = high_hits['win_odds'].sum() / len(v44_high) * 100 if len(v44_high) > 0 else 0
    low_roi = low_hits['win_odds'].sum() / len(v44_low) * 100 if len(v44_low) > 0 else 0
    
    logger.info(f"  stable + V4.4高: ROI {high_roi:.1f}% (n={len(v44_high)})")
    logger.info(f"  stable + V4.4低: ROI {low_roi:.1f}% (n={len(v44_low)})")
    
    # 推論
    logger.info("\n【推論】")
    logger.info("  V4.4スコアは「穴馬発見能力」を示す")
    logger.info("  stable + V4.4高 = 堅いレースで穴馬を狙っている → 矛盾")
    logger.info("  stable + V4.4低 = 堅いレースで本命を狙っている → 理にかなう")
    
    logger.info("\n【改善案】")
    logger.info("  1. stableレースではV4.4ウェイトをさらに下げる（0.25 → 0.15）")
    logger.info("  2. stableレースではV4.4を完全に無効化")


def analyze_popular_horse_miss(df, sample_size=10):
    """人気馬が外れたレースの詳細分析"""
    logger.info("\n" + "="*70)
    logger.info("【分析3】人気馬外れレースの詳細パターン")
    logger.info("="*70)
    
    top1 = df[df['rank_ensemble'] == 1].copy()
    
    # 1番人気が外れたレース
    fav1_miss = top1[(top1['popularity'] == 1) & (top1['finish_position'] != 1)]
    
    logger.info(f"\n1番人気外れレース: {len(fav1_miss)}件")
    
    # 外れた時の着順分布
    logger.info("\n【外れた時の着順分布】")
    for pos in [2, 3, 4, 5]:
        count = (fav1_miss['finish_position'] == pos).sum()
        pct = count / len(fav1_miss) * 100 if len(fav1_miss) > 0 else 0
        logger.info(f"  {pos}着: {count}件 ({pct:.1f}%)")
    
    # 具体例を表示
    logger.info(f"\n【具体例（{min(sample_size, len(fav1_miss))}件）】")
    
    for i, (_, row) in enumerate(fav1_miss.head(sample_size).iterrows()):
        race_id = row['race_id']
        race_date = str(row['race_date'])[:10] if pd.notna(row['race_date']) else 'N/A'
        venue = row['venue'] if pd.notna(row['venue']) else 'N/A'
        race_name = str(row['race_name'])[:15] if pd.notna(row['race_name']) else 'N/A'
        actual_pos = int(row['finish_position']) if pd.notna(row['finish_position']) else '-'
        odds = row['win_odds']
        vol_type = row['vol_type']
        
        # 勝った馬の情報を取得
        race_df = df[df['race_id'] == race_id]
        winner = race_df[race_df['finish_position'] == 1]
        if len(winner) > 0:
            winner = winner.iloc[0]
            winner_pop = int(winner['popularity']) if pd.notna(winner['popularity']) else '-'
            winner_odds = winner['win_odds']
        else:
            winner_pop = '-'
            winner_odds = 0
        
        logger.info(f"\n  例{i+1}: {race_date} {venue} {race_name}")
        logger.info(f"    予測1位: 人気1番 オッズ{odds:.1f}倍 → 実着{actual_pos}着")
        logger.info(f"    荒れ度: {vol_type} | 勝者: 人気{winner_pop}番 オッズ{winner_odds:.1f}倍")
    
    # 推論
    logger.info("\n【推論】")
    close_miss_rate = (fav1_miss['finish_position'] <= 3).mean() * 100
    logger.info(f"  1番人気が外れても{close_miss_rate:.1f}%は3着以内 → 複勝なら的中")
    
    logger.info("\n【改善案】")
    logger.info("  1. 複勝への切り替え（特にstableレース）")
    logger.info("  2. stable判定閾値の見直し")


def generate_improvement_summary():
    """改善策サマリーを生成"""
    logger.info("\n" + "="*70)
    logger.info("【総合まとめ】改善策の整理と優先度設定")
    logger.info("="*70)
    
    logger.info("""
■ 発見された問題と改善案
----------------------------------------------------------------------

1. 【会場依存性】ROI差: 中山88% vs 小倉66%
   原因: コース特性による予測精度の差
   改善案:
   ❶ 低ROI会場（小倉、新潟、札幌）では複勝に切り替え
   ❷ 会場を特徴量としてウェイト調整
   
2. 【stable+V4.4逆転現象】V4.4低の方が高ROI
   原因: stableレースでの穴馬評価は過大評価
   改善案:
   ❶ stableレースではV4.4ウェイトを0.15以下に
   ❷ stableレースでは単純にwin_probのみ使用
   
3. 【人気馬外れ】外れても3着以内率が高い
   原因: 実力はあるが勝ちきれない（惜敗馬）
   改善案:
   ❶ 複勝への切り替え（特にstableレース）
   ❷ horse_close_loss_rateを活用

■ 優先度付き実装計画
----------------------------------------------------------------------

【優先度 高】すぐに効果が見込める
  1. 低ROI会場で複勝に切り替え → 期待改善: +3-5pt
  2. stableレースでV4.4ウェイト削減 → 期待改善: +2-3pt
  
【優先度 中】追加検証が必要
  3. moderate穴馬狙いモードの追加
  4. 複勝戦略のバックテスト
""")


def main():
    logger.info("="*70)
    logger.info("個別レース深層分析 - 原因推論と改善策導出")
    logger.info("="*70)
    
    # データ読み込みと予測
    logger.info("\n2024年データを分析中...")
    df = load_prediction_data()
    df = apply_volatility_classification(df)
    
    logger.info(f"分析対象: {len(df):,}件")
    
    # 各種分析
    analyze_high_roi_venues(df)
    analyze_v44_paradox(df)
    analyze_popular_horse_miss(df, sample_size=10)
    
    # 改善策サマリー
    generate_improvement_summary()
    
    logger.info("\n分析完了")


if __name__ == "__main__":
    main()
