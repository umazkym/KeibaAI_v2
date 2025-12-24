"""データ重複とLayer2キャリブレーションの詳細調査"""

import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def main():
    print("=" * 70)
    print("データ重複・キャリブレーション詳細調査")
    print("=" * 70)
    
    data_dir = Path('keibaai/data')
    
    # レース結果のみ読み込み
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[(df['race_date'] >= '2024-01-01') & (df['race_date'] <= '2024-06-30')]
    
    print(f"\n[1] レースデータ（特徴量マージ前）")
    print(f"  レコード数: {len(df):,}")
    print(f"  レース数: {df['race_id'].nunique():,}")
    print(f"  ユニーク (race_id, horse_number): {df.groupby(['race_id', 'horse_number']).ngroups:,}")
    
    # 重複チェック
    duplicates = df.groupby(['race_id', 'horse_number']).size()
    dup_count = (duplicates > 1).sum()
    print(f"  重複 (race_id, horse_number): {dup_count}")
    
    # 特徴量読み込み
    features_path = data_dir / "features/parquet"
    year_dirs = sorted(features_path.glob('year=*'))
    
    feature_dfs = []
    for year_dir in year_dirs:
        try:
            year_df = pd.read_parquet(year_dir)
            feature_dfs.append(year_df)
        except:
            pass
    
    features_df = pd.concat(feature_dfs, ignore_index=True)
    
    print(f"\n[2] 特徴量データ")
    print(f"  レコード数: {len(features_df):,}")
    print(f"  ユニーク (race_id, horse_number): {features_df.groupby(['race_id', 'horse_number']).ngroups:,}")
    print(f"  カラム: {features_df.columns.tolist()[:10]}...")
    
    # 重複チェック
    feature_dup = features_df.groupby(['race_id', 'horse_number']).size()
    feature_dup_count = (feature_dup > 1).sum()
    print(f"  重複 (race_id, horse_number): {feature_dup_count}")
    
    # 重複例を表示
    if feature_dup_count > 0:
        dup_keys = feature_dup[feature_dup > 1].head(3).index.tolist()
        print(f"\n  重複例:")
        for key in dup_keys:
            race_id, horse_num = key
            dup_data = features_df[(features_df['race_id'] == race_id) & (features_df['horse_number'] == horse_num)]
            print(f"    race_id={race_id}, horse_number={horse_num}: {len(dup_data)}件")
    
    # マージ後
    merge_keys = ['race_id', 'horse_id', 'horse_number']
    available_keys = [k for k in merge_keys if k in features_df.columns]
    print(f"\n[3] マージ")
    print(f"  マージキー: {available_keys}")
    
    merged = df.merge(features_df, on=available_keys, how='left', suffixes=('', '_feat'))
    print(f"  マージ後レコード数: {len(merged):,}")
    print(f"  増加倍率: {len(merged) / len(df):.2f}x")
    
    # Layer2の問題調査
    print(f"\n[4] Layer2キャリブレーション調査")
    
    from keibaai.src.models.ability_estimator import Layer1_AbilityEstimator
    from keibaai.src.models.probability_calibrator import Layer2_ProbabilityCalibrator
    
    # 学習データ（2023年）
    train_df = pd.read_parquet(races_path)
    train_df['race_date'] = pd.to_datetime(train_df['race_date'])
    train_df = train_df[(train_df['race_date'] >= '2023-01-01') & (train_df['race_date'] <= '2023-06-30')]
    train_df = train_df.dropna(subset=['finish_position', 'win_odds'])
    
    print(f"  学習データ: {len(train_df):,} 件")
    print(f"  1着率: {(train_df['finish_position'] == 1).mean()*100:.2f}%")
    
    # 1着ラベルの分布
    print(f"\n  着順分布（学習データ）:")
    for pos in range(1, 6):
        count = (train_df['finish_position'] == pos).sum()
        pct = count / len(train_df) * 100
        print(f"    {pos}着: {count:,} ({pct:.1f}%)")
    
    # Isotonic Regressionの入力と出力を確認
    print(f"\n[5] IsotonicRegressionの動作確認")
    
    # ダミースコアで確認
    np.random.seed(42)
    n_samples = 1000
    scores = np.random.randn(n_samples)  # 正規分布スコア
    labels = (np.random.rand(n_samples) < 0.1).astype(int)  # 10%が1着
    
    from sklearn.isotonic import IsotonicRegression
    iso = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
    iso.fit(scores, labels)
    
    probs = iso.transform(scores)
    print(f"  入力スコア: min={scores.min():.3f}, max={scores.max():.3f}")
    print(f"  出力確率: min={probs.min():.4f}, max={probs.max():.4f}, mean={probs.mean():.4f}")
    print(f"  実際の1着率: {labels.mean()*100:.1f}%")
    print(f"  予測確率平均: {probs.mean()*100:.1f}%")
    
    # グループ内正規化
    race_size = 10
    n_races = n_samples // race_size
    groups = np.repeat(np.arange(n_races), race_size)
    df_test = pd.DataFrame({'group': groups, 'prob': probs[:len(groups)]})
    df_test['norm_prob'] = df_test.groupby('group')['prob'].transform(lambda x: x / x.sum())
    
    print(f"\n  正規化後:")
    print(f"    確率: min={df_test['norm_prob'].min():.4f}, max={df_test['norm_prob'].max():.4f}")
    print(f"    合計: {df_test.groupby('group')['norm_prob'].sum().mean():.4f}")
    
    # 問題の本質
    print(f"\n[6] 問題の本質")
    print(f"  IsotonicRegressionは「スコア → 1着確率」を学習")
    print(f"  しかし1着率は約10%なので、確率も最大10%程度に抑制される")
    print(f"  レース内正規化で合計100%にするが、相対比率は維持される")
    print(f"")
    print(f"  → 解決策: スコアを直接Softmaxで変換すべき")


if __name__ == '__main__':
    main()
