#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
レース単位での深層分析

目的:
1. 予測と実着順の乖離パターンを具体的に可視化
2. なぜそのような予測になったのかを特徴量レベルで分析
3. 改善のための具体的な方向性を導出

作成日: 2026-01-11
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
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )
    races = races[~(new_horse_mask | obstacle_mask)].copy()
    
    return races, pedigrees, corners, race_details, horses


def run_prediction_with_features(races, pedigrees, corners, race_details, horses, year):
    """特徴量を含めた予測を実行"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    from keibaai.src.models.multi_target_predictor import MultiTargetPredictor
    from keibaai.src.features.time_margin_features import TimeMarginFeatureEngineer
    
    test_start = pd.to_datetime(f'{year}-01-01')
    test_end = pd.to_datetime(f'{year}-12-31')
    train_end = test_start - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
    
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
    
    # 基本情報を結合
    base_cols = ['race_id', 'horse_number', 'horse_name', 'finish_position', 
                 'win_odds', 'popularity', 'race_date', 'venue', 'race_name',
                 'track_surface', 'distance', 'horse_weight', 'weight_change',
                 'jockey_name', 'trainer_name']
    base_cols = [c for c in base_cols if c in test_f.columns]
    
    test_preds = test_preds.merge(
        test_f[base_cols],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 主要特徴量も追加
    key_features = [
        'win_rate', 'place_rate', 'horse_place_rate_sire',
        'jockey_win_rate', 'trainer_win_rate',
        'avg_finish_position', 'days_since_last_race',
        'class_level', 'running_style_code',
        'horse_avg_margin_when_loss', 'race_place_prob_std',
        'position_improvement', 'front_runner_rate',
    ]
    key_features = [f for f in key_features if f in test_f.columns]
    
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number'] + key_features],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = year
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    
    return test_preds, feature_cols, predictor


def analyze_specific_races(preds_df, n_examples=10):
    """具体的なレースを詳細分析"""
    print("\n" + "="*80)
    print("【分析1】予測成功・失敗の具体例")
    print("="*80)
    
    # 予測成功例: Top1が1着になったレース
    success = preds_df[(preds_df['rank'] == 1) & (preds_df['finish_position'] == 1)]
    
    # 予測失敗例: Top1が5着以下になったレース
    failure = preds_df[(preds_df['rank'] == 1) & (preds_df['finish_position'] >= 5)]
    
    print(f"\n成功例: {len(success)}件, 失敗例: {len(failure)}件")
    
    # 成功例の詳細
    print("\n【成功例】Top1が1着になったレース（サンプル5件）:")
    print("-"*80)
    for _, row in success.sample(min(5, len(success))).iterrows():
        race_id = row['race_id']
        race_df = preds_df[preds_df['race_id'] == race_id].sort_values('rank')
        
        print(f"\n■ {str(row['race_date'])[:10]} {row['venue']} {str(row['race_name'])[:20]}")
        print(f"  AI Top1: {row['horse_name']} ({row['popularity']:.0f}番人気, オッズ{row['win_odds']:.1f}倍)")
        print(f"  結果: 1着 → 成功!")
        
        # 上位3頭の比較
        print("  [上位3頭の特徴量比較]")
        for i, (_, h) in enumerate(race_df.head(3).iterrows()):
            wr = h.get('win_rate', 0) or 0
            pr = h.get('place_rate', 0) or 0
            print(f"    Top{i+1}: {h['horse_name'][:8]:8} "
                  f"人気{h['popularity']:.0f} オッズ{h['win_odds']:5.1f} "
                  f"勝率{wr*100:.1f}% 複勝率{pr*100:.1f}% "
                  f"着順{h['finish_position']:.0f}")
    
    # 失敗例の詳細
    print("\n\n【失敗例】Top1が5着以下になったレース（サンプル5件）:")
    print("-"*80)
    for _, row in failure.sample(min(5, len(failure))).iterrows():
        race_id = row['race_id']
        race_df = preds_df[preds_df['race_id'] == race_id].sort_values('rank')
        actual_winner = race_df[race_df['finish_position'] == 1].iloc[0] if len(race_df[race_df['finish_position'] == 1]) > 0 else None
        
        print(f"\n■ {str(row['race_date'])[:10]} {row['venue']} {str(row['race_name'])[:20]}")
        print(f"  AI Top1: {row['horse_name']} ({row['popularity']:.0f}番人気, オッズ{row['win_odds']:.1f}倍)")
        print(f"  結果: {row['finish_position']:.0f}着 → 失敗")
        
        if actual_winner is not None:
            print(f"  実際の1着: {actual_winner['horse_name']} (AI予測{actual_winner['rank']:.0f}位, {actual_winner['popularity']:.0f}番人気)")
        
        # Top1と実際の1着の特徴量比較
        print("  [Top1 vs 実際の1着 特徴量比較]")
        top1 = race_df[race_df['rank'] == 1].iloc[0]
        
        for feat in ['win_rate', 'place_rate', 'jockey_win_rate', 'days_since_last_race']:
            if feat in top1.index and actual_winner is not None and feat in actual_winner.index:
                v1 = top1[feat] if pd.notna(top1[feat]) else 0
                v2 = actual_winner[feat] if pd.notna(actual_winner[feat]) else 0
                print(f"    {feat:25}: Top1={v1:7.3f}, 1着={v2:7.3f}")


def analyze_prediction_patterns(preds_df):
    """予測パターンの傾向分析"""
    print("\n" + "="*80)
    print("【分析2】予測と着順のズレパターン")
    print("="*80)
    
    # Top1-5それぞれの着順分布
    for rank in range(1, 6):
        rank_df = preds_df[preds_df['rank'] == rank]
        
        print(f"\n■ Top{rank}の着順分布:")
        finish_dist = rank_df['finish_position'].value_counts().sort_index().head(8)
        for pos, count in finish_dist.items():
            pct = count / len(rank_df) * 100
            bar = '█' * int(pct / 3)
            print(f"  {int(pos):2}着: {count:4}件 ({pct:5.1f}%) {bar}")


def analyze_feature_importance_by_outcome(preds_df, predictor, feature_cols):
    """成功/失敗ケースでの特徴量の違い"""
    print("\n" + "="*80)
    print("【分析3】成功/失敗ケースでの特徴量比較")
    print("="*80)
    
    top1 = preds_df[preds_df['rank'] == 1].copy()
    
    # 成功: 1着になった, 失敗: 5着以下
    success = top1[top1['finish_position'] == 1]
    failure = top1[top1['finish_position'] >= 5]
    
    print(f"\n成功(1着): {len(success)}件, 失敗(5着以下): {len(failure)}件")
    
    # 主要特徴量の平均比較
    key_features = [
        'win_rate', 'place_rate', 'popularity', 'win_odds',
        'jockey_win_rate', 'days_since_last_race', 'ensemble_score',
    ]
    key_features = [f for f in key_features if f in top1.columns]
    
    print("\n■ 主要特徴量の平均値比較:")
    print(f"{'特徴量':25} {'成功':>10} {'失敗':>10} {'差':>10}")
    print("-"*55)
    
    for feat in key_features:
        s_mean = success[feat].mean() if feat in success.columns else 0
        f_mean = failure[feat].mean() if feat in failure.columns else 0
        diff = s_mean - f_mean
        
        # 差が大きい特徴量をハイライト
        highlight = "★" if abs(diff) > 0.05 * max(abs(s_mean), abs(f_mean), 0.01) else ""
        print(f"{feat:25} {s_mean:10.4f} {f_mean:10.4f} {diff:+10.4f} {highlight}")


def analyze_underrated_horses(preds_df):
    """AIが過小評価している馬のパターン"""
    print("\n" + "="*80)
    print("【分析4】AIが過小評価している馬（予測下位→実際上位）")
    print("="*80)
    
    # AI予測で6位以下だが実際に1着になった馬
    underrated = preds_df[(preds_df['rank'] >= 6) & (preds_df['finish_position'] == 1)]
    
    print(f"\n過小評価ケース: {len(underrated)}件（AI予測6位以下→実際1着）")
    
    if len(underrated) > 0:
        # 過小評価された馬の特徴
        print("\n■ 過小評価された馬の特徴量（平均）:")
        key_feats = ['win_rate', 'place_rate', 'popularity', 'win_odds', 'days_since_last_race']
        key_feats = [f for f in key_feats if f in underrated.columns]
        
        for feat in key_feats:
            val = underrated[feat].mean()
            print(f"  {feat:25}: {val:.3f}")
        
        # 具体例
        print("\n■ 具体例（サンプル5件）:")
        for _, row in underrated.sample(min(5, len(underrated))).iterrows():
            print(f"  {str(row['race_date'])[:10]} {row['venue']}: "
                  f"{row['horse_name'][:15]} (AI{row['rank']:.0f}位→実際1着, "
                  f"{row['popularity']:.0f}番人気, オッズ{row['win_odds']:.1f}倍)")


def analyze_overrated_horses(preds_df):
    """AIが過大評価している馬のパターン"""
    print("\n" + "="*80)
    print("【分析5】AIが過大評価している馬（予測上位→実際下位）")
    print("="*80)
    
    # AI予測で1-2位だが実際に8着以下になった馬
    overrated = preds_df[(preds_df['rank'] <= 2) & (preds_df['finish_position'] >= 8)]
    
    print(f"\n過大評価ケース: {len(overrated)}件（AI予測1-2位→実際8着以下）")
    
    if len(overrated) > 0:
        # 過大評価された馬の特徴
        print("\n■ 過大評価された馬の特徴量（平均）:")
        key_feats = ['win_rate', 'place_rate', 'popularity', 'win_odds', 'days_since_last_race']
        key_feats = [f for f in key_feats if f in overrated.columns]
        
        for feat in key_feats:
            val = overrated[feat].mean()
            print(f"  {feat:25}: {val:.3f}")
        
        # 人気との比較
        pop_dist = overrated['popularity'].value_counts().sort_index().head(5)
        print("\n■ 人気順分布:")
        for pop, count in pop_dist.items():
            print(f"  {int(pop)}番人気: {count}件")


def main():
    print("="*80)
    print("【レース単位深層分析】予測と着順の具体的検証")
    print("="*80)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses = load_base_data()
    print(f"データ件数: {len(races):,}")
    
    # 2024年で分析
    print("\n[2024年] 予測中（特徴量付き）...")
    preds, feature_cols, predictor = run_prediction_with_features(
        races, pedigrees, corners, race_details, horses, 2024
    )
    print(f"予測完了: {len(preds):,}件")
    
    # 分析実行
    analyze_specific_races(preds)
    analyze_prediction_patterns(preds)
    analyze_feature_importance_by_outcome(preds, predictor, feature_cols)
    analyze_underrated_horses(preds)
    analyze_overrated_horses(preds)
    
    # 結論
    print("\n" + "="*80)
    print("【深層推論：改善の方向性】")
    print("="*80)
    print("""
■ 仮説1: 過去成績に依存しすぎ
  - win_rate, place_rateが高い馬を過大評価
  - 調子の波や適性変化を捉えていない可能性

■ 仮説2: 休み明けの予測が弱い
  - days_since_last_raceが長い馬の扱いが難しい
  - 休養効果と調整不足を区別できていない

■ 仮説3: 人気馬への過信
  - 1-3番人気をTop1-3に配置しがち
  - 穴馬の「隠れた実力」を見落としている

■ 改善の方向性
  1. 「調子」「適性」を捉える特徴量の強化
  2. 休み明け専用の特徴量or重み付け
  3. オッズと予測の乖離を学習に活用
""")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    preds.to_csv(output_dir / "race_level_deep_analysis.csv", index=False, encoding='utf-8-sig')
    print(f"\n結果保存: race_level_deep_analysis.csv")


if __name__ == "__main__":
    main()
