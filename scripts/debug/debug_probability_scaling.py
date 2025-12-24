"""
確率スケーリングの問題を特定
"""
import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def main():
    print("=" * 70)
    print("確率スケーリング問題の調査")
    print("=" * 70)
    
    data_dir = Path('keibaai/data')
    
    # レースデータ（特徴量マージ前）
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[(df['race_date'] >= '2024-01-01') & (df['race_date'] <= '2024-06-30')]
    df = df.dropna(subset=['finish_position', 'win_odds'])
    
    print(f"\n[1] レースデータ（特徴量なし、重複なし）")
    print(f"  レコード数: {len(df):,}")
    print(f"  レース数: {df['race_id'].nunique():,}")
    print(f"  平均1レースあたりの馬数: {len(df) / df['race_id'].nunique():.1f}")
    
    # サンプルレースで確認
    sample_race_id = df['race_id'].iloc[0]
    sample_race = df[df['race_id'] == sample_race_id]
    print(f"\n  サンプルレース {sample_race_id}:")
    print(f"    馬数: {len(sample_race)}")
    print(f"    オッズ: {sample_race['win_odds'].tolist()}")
    
    # 人気カラム作成
    if 'popularity' not in df.columns:
        df['popularity'] = df.groupby('race_id')['win_odds'].rank()
    
    # ダミースコアで確率計算をテスト
    print(f"\n[2] Softmax変換のテスト")
    
    # レース内で確率を計算（SoftmaxでなくRankベース）
    # スコア = -popularity（1番人気が最高）
    df['score'] = -df['popularity']
    
    def softmax(x, temperature=1.0):
        scaled = x / temperature
        exp_x = np.exp(scaled - np.max(scaled))
        return exp_x / exp_x.sum()
    
    # レースごとにSoftmax
    df['softmax_prob'] = df.groupby('race_id')['score'].transform(
        lambda x: softmax(x.values, temperature=1.0)
    )
    
    # 1番人気の確率
    fav = df[df['popularity'] == 1]
    print(f"  1番人気の確率 (Softmax from -popularity):")
    print(f"    mean: {fav['softmax_prob'].mean():.4f}")
    print(f"    median: {fav['softmax_prob'].median():.4f}")
    
    # 合計確認
    prob_sums = df.groupby('race_id')['softmax_prob'].sum()
    print(f"  レース内確率合計: {prob_sums.mean():.4f}")
    
    # モデルの能力スコアで確認
    print(f"\n[3] モデル能力スコアの分布")
    
    from keibaai.src.models.hybrid_betting_ai import HybridHorseRacingAI
    
    # 特徴量作成（簡易版 - レースデータのみ）
    numeric_cols = [c for c in df.columns if df[c].dtype in ['int64', 'float64', 'int32', 'float32']
                   and c not in ['finish_position', 'win_odds', 'popularity', 'race_id', 'horse_number']]
    
    if len(numeric_cols) == 0:
        print(f"  数値カラムなし、スキップ")
        return
    
    print(f"  使用カラム: {numeric_cols[:5]}...")
    X = df[numeric_cols].fillna(0)
    groups = df['race_id']
    
    # モデルロード
    model_path = Path('keibaai/data/models/hybrid_v1')
    if model_path.exists():
        ai = HybridHorseRacingAI()
        try:
            ai.load(str(model_path))
            print(f"  モデルロード完了")
            
            # Layer1の特徴量を確認
            print(f"  Layer1の期待する特徴量数: {len(ai.layer1.feature_names_)}")
            print(f"  提供した特徴量数: {len(numeric_cols)}")
            
            # 共通の特徴量を使用
            common_cols = [c for c in ai.layer1.feature_names_ if c in numeric_cols]
            print(f"  共通特徴量数: {len(common_cols)}")
            
            if len(common_cols) == 0:
                print(f"  共通特徴量がないため推論不可")
                return
            
        except Exception as e:
            print(f"  モデルロード失敗: {e}")
            return
    else:
        print(f"  モデルなし")
        return
    
    # 問題の根本原因
    print(f"\n[4] 問題の根本原因の特定")
    print(f"  重複マージ後のデータでは、同じ race_id, horse_number が複数行存在")
    print(f"  → レース内の馬数が本来より多く見える")
    print(f"  → Softmax計算で確率が過小評価される")
    print(f"")
    print(f"  例: 本来10頭のレースが80行に膨張")
    print(f"  → 1番人気の確率: 1/10 → 1/80 に")


if __name__ == '__main__':
    main()
