"""
バックテスト詳細デバッグ - 計算の問題を特定

問題:
1. 払戻額が全EV閾値で同一
2. 的中率が極端に低い
3. 月別分布が異常
"""

import os
import sys
import logging
from pathlib import Path
import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.WARNING)


def load_data(data_dir: Path, start_date: str, end_date: str):
    """データ読み込み"""
    # レース結果
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[(df['race_date'] >= start_date) & (df['race_date'] <= end_date)]
    
    # 特徴量マージ
    features_path = data_dir / "features/parquet"
    year_dirs = sorted(features_path.glob('year=*'))
    if year_dirs:
        feature_dfs = []
        for year_dir in year_dirs:
            try:
                year_df = pd.read_parquet(year_dir)
                feature_dfs.append(year_df)
            except:
                pass
        if feature_dfs:
            features_df = pd.concat(feature_dfs, ignore_index=True)
            df = df.merge(features_df, on=['race_id', 'horse_id', 'horse_number'], 
                         how='left', suffixes=('', '_feat'))
    
    return df


def get_feature_columns(df):
    """特徴量カラム取得"""
    exclude_patterns = [
        'race_id', 'horse_id', 'jockey_id', 'trainer_id',
        'horse_name', 'jockey_name', 'trainer_name', 'owner_name',
        'finish_position', 'finish_time', 'win_odds', 'popularity',
        'payout', 'prize', 'morning_odds', 'morning_popularity',
        'passing_order', 'last_3f', 'margin', 'pace_index',
        'race_date', 'race_name', 'post_time'
    ]
    feature_cols = []
    for col in df.columns:
        exclude = any(pattern in col.lower() for pattern in exclude_patterns)
        if not exclude and df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            feature_cols.append(col)
    return feature_cols


def main():
    print("=" * 70)
    print("バックテスト詳細デバッグ")
    print("=" * 70)
    
    data_dir = Path('keibaai/data')
    df = load_data(data_dir, '2024-01-01', '2024-06-30')  # テスト期間のみ
    
    print(f"\n[1] データ概要")
    print(f"  レコード数: {len(df):,}")
    print(f"  レース数: {df['race_id'].nunique():,}")
    
    # 欠損除去
    df = df.dropna(subset=['finish_position', 'win_odds'])
    
    # 人気カラム作成
    if 'popularity' not in df.columns:
        df['popularity'] = df.groupby('race_id')['win_odds'].rank()
    
    feature_cols = get_feature_columns(df)
    
    # モデル学習（シンプルに）
    print(f"\n[2] モデル学習")
    from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
    from keibaai.src.modules.models.probability_calibrator import Layer2_ProbabilityCalibrator
    
    X = df[feature_cols].fillna(0)
    y = df['finish_position']
    groups = df['race_id']
    
    # 学習データは別途用意が必要だが、ここではテストデータで直接推論
    # → 訓練済みモデルをロード
    
    from keibaai.src.modules.models.hybrid_betting_ai import HybridHorseRacingAI
    
    # 訓練済みモデルをロード
    model_path = Path('keibaai/data/models/hybrid_v1')
    if model_path.exists():
        print(f"  訓練済みモデルをロード: {model_path}")
        ai = HybridHorseRacingAI()
        try:
            ai.load(str(model_path))
        except Exception as e:
            print(f"  ロード失敗: {e}")
            print("  新規学習が必要")
            return
    else:
        print("  モデルが見つかりません")
        return

    # Layer 1 推論
    print(f"\n[3] 推論結果の分析")
    l1_pred = ai.layer1.predict(X, groups)
    l2_pred = ai.layer2.transform(l1_pred['ability_score'].values, groups.values)
    
    df['win_prob'] = l2_pred['win_prob'].values
    
    print(f"\n  [3.1] 確率分布")
    print(f"    win_prob min: {df['win_prob'].min():.4f}")
    print(f"    win_prob max: {df['win_prob'].max():.4f}")
    print(f"    win_prob mean: {df['win_prob'].mean():.4f}")
    print(f"    win_prob median: {df['win_prob'].median():.4f}")
    
    # レース内確率合計
    prob_sums = df.groupby('race_id')['win_prob'].sum()
    print(f"\n  [3.2] レース内確率合計")
    print(f"    平均: {prob_sums.mean():.4f}")
    print(f"    最小: {prob_sums.min():.4f}")
    print(f"    最大: {prob_sums.max():.4f}")
    
    # EV計算
    print(f"\n  [3.3] EV分布")
    df['ev'] = df['win_prob'] * df['win_odds'] * 0.8  # 控除率20%
    print(f"    EV min: {df['ev'].min():.4f}")
    print(f"    EV max: {df['ev'].max():.4f}")
    print(f"    EV mean: {df['ev'].mean():.4f}")
    print(f"    EV >= 1.0 の数: {(df['ev'] >= 1.0).sum():,} ({(df['ev'] >= 1.0).mean()*100:.1f}%)")
    print(f"    EV >= 1.1 の数: {(df['ev'] >= 1.1).sum():,} ({(df['ev'] >= 1.1).mean()*100:.1f}%)")
    
    # 1着馬のEV分布
    print(f"\n  [3.4] 1着馬の特性")
    first_place = df[df['finish_position'] == 1]
    print(f"    1着馬数: {len(first_place):,}")
    print(f"    1着馬 EV mean: {first_place['ev'].mean():.4f}")
    print(f"    1着馬 win_prob mean: {first_place['win_prob'].mean():.4f}")
    print(f"    1着馬 win_odds mean: {first_place['win_odds'].mean():.2f}")
    print(f"    1着馬 popularity mean: {first_place['popularity'].mean():.1f}")
    
    # EV >= 1.1 で1着だった馬
    ev_filtered = df[(df['ev'] >= 1.1)]
    winners_in_ev = ev_filtered[ev_filtered['finish_position'] == 1]
    print(f"\n  [3.5] EV >= 1.1 の馬の的中分析")
    print(f"    EV >= 1.1 の馬数: {len(ev_filtered):,}")
    print(f"    うち1着: {len(winners_in_ev):,}")
    print(f"    的中率: {len(winners_in_ev) / len(ev_filtered) * 100:.2f}%")
    
    if len(winners_in_ev) > 0:
        print(f"\n    的中した馬の詳細:")
        for _, row in winners_in_ev.head(10).iterrows():
            print(f"      race={row['race_id']}, 馬番={row['horse_number']}, "
                  f"odds={row['win_odds']:.1f}, prob={row['win_prob']:.4f}, ev={row['ev']:.3f}")
    
    # 問題特定: 人気との相関
    print(f"\n  [3.6] 確率と人気の相関")
    corr_prob_pop = df['win_prob'].corr(df['popularity'])
    corr_ev_pop = df['ev'].corr(df['popularity'])
    print(f"    win_prob vs popularity 相関: {corr_prob_pop:.4f}")
    print(f"    EV vs popularity 相関: {corr_ev_pop:.4f}")
    
    # 本当の問題: 確率がオッズに引きずられていないか？
    print(f"\n  [3.7] 確率とオッズの関係")
    corr_prob_odds = df['win_prob'].corr(df['win_odds'])
    print(f"    win_prob vs win_odds 相関: {corr_prob_odds:.4f}")
    
    # 1番人気の予測確率
    fav = df[df['popularity'] == 1]
    print(f"\n  [3.8] 1番人気の分析")
    print(f"    1番人気の数: {len(fav):,}")
    print(f"    1番人気の勝率 (実際): {(fav['finish_position'] == 1).mean()*100:.1f}%")
    print(f"    1番人気の予測確率 mean: {fav['win_prob'].mean():.4f}")
    print(f"    1番人気 EV mean: {fav['ev'].mean():.4f}")
    print(f"    1番人気で EV >= 1.1: {(fav['ev'] >= 1.1).sum()}")
    
    # 穴馬（人気10位以下）の分析
    print(f"\n  [3.9] 穴馬（人気10位以下）の分析")
    longshot = df[df['popularity'] >= 10]
    print(f"    穴馬の数: {len(longshot):,}")
    print(f"    穴馬の勝率 (実際): {(longshot['finish_position'] == 1).mean()*100:.2f}%")
    print(f"    穴馬の予測確率 mean: {longshot['win_prob'].mean():.4f}")
    print(f"    穴馬 EV mean: {longshot['ev'].mean():.4f}")
    print(f"    穴馬で EV >= 1.1: {(longshot['ev'] >= 1.1).sum()}")


if __name__ == '__main__':
    main()
