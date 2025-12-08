"""
EV閾値感度分析 - 最適な閾値を探索

異なるEV閾値でバックテストを実行し、
ROIと安定性のバランスを評価する
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from tqdm import tqdm

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.modules.models.hybrid_betting_ai import HybridHorseRacingAI

logging.basicConfig(
    level=logging.WARNING,  # INFOログを抑制
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_and_prepare_data(data_dir: Path, start_date: str, end_date: str):
    """データの読み込みと準備"""
    print(f"Loading data from {data_dir}...")
    
    # レース結果
    races_path = data_dir / "parsed/parquet/races/races.parquet"
    df = pd.read_parquet(races_path)
    
    # 日付フィルタ
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df[(df['race_date'] >= start_date) & (df['race_date'] <= end_date)]
    print(f"  Race records: {len(df)}")
    
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
            merge_keys = ['race_id', 'horse_id', 'horse_number']
            df = df.merge(features_df, on=merge_keys, how='left', suffixes=('', '_feat'))
    
    print(f"  Total columns: {len(df.columns)}")
    return df


def get_feature_columns(df: pd.DataFrame) -> list:
    """特徴量カラムを取得"""
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
        if not exclude and df[col].dtype in ['int64', 'float64', 'int32', 'float32', 'Int8', 'Int16']:
            feature_cols.append(col)
    
    return feature_cols


def time_series_split(df: pd.DataFrame):
    """時系列分割"""
    df = df.sort_values('race_date').reset_index(drop=True)
    max_date = df['race_date'].max()
    
    # Test: 最新6ヶ月
    test_start = max_date - pd.DateOffset(months=6)
    test_df = df[df['race_date'] >= test_start]
    
    # Valid: test前6ヶ月（ギャップ1ヶ月）
    val_end = test_start - pd.DateOffset(months=1)
    val_start = val_end - pd.DateOffset(months=6)
    val_df = df[(df['race_date'] >= val_start) & (df['race_date'] < val_end)]
    
    # Train: valid前
    train_end = val_start - pd.DateOffset(months=1)
    train_df = df[df['race_date'] < train_end]
    train_df = train_df[train_df['race_date'] >= train_end - pd.DateOffset(years=3)]
    
    return train_df, val_df, test_df


def run_sensitivity_analysis():
    """EV閾値感度分析を実行"""
    print("=" * 70)
    print("EV閾値感度分析")
    print("=" * 70)
    
    data_dir = Path('keibaai/data')
    
    # データ準備
    df = load_and_prepare_data(data_dir, '2023-01-01', '2024-06-30')
    feature_cols = get_feature_columns(df)
    print(f"  Feature columns: {len(feature_cols)}")
    
    # 欠損除去
    df = df.dropna(subset=['finish_position', 'win_odds'])
    
    # 人気カラム作成
    if 'popularity' not in df.columns:
        df['popularity'] = df.groupby('race_id')['win_odds'].rank()
    
    # 時系列分割
    train_df, val_df, test_df = time_series_split(df)
    print(f"\nData splits:")
    print(f"  Train: {len(train_df):,} records")
    print(f"  Valid: {len(val_df):,} records")
    print(f"  Test: {len(test_df):,} records")
    
    # 準備
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df['finish_position']
    groups_val = val_df['race_id']
    
    # テストするEV閾値
    ev_thresholds = [1.00, 1.02, 1.05, 1.08, 1.10, 1.12, 1.15, 1.20]
    
    results = []
    
    print("\n" + "=" * 70)
    print("Testing different EV thresholds...")
    print("=" * 70)
    
    for ev_threshold in tqdm(ev_thresholds, desc="EV閾値テスト"):
        # モデル初期化・学習（1回だけ）
        ai = HybridHorseRacingAI(
            ev_threshold=ev_threshold,
            kelly_fraction=0.25,
            bankroll=100000
        )
        
        # 学習（ログ抑制）
        import logging
        logging.getLogger('keibaai').setLevel(logging.ERROR)
        ai.train(X_train, y_train, groups_train, X_val, y_val, groups_val)
        
        # バックテスト
        result = ai.backtest(
            test_df,
            feature_cols,
            race_id_col='race_id',
            rank_col='finish_position',
            odds_col='win_odds',
            horse_number_col='horse_number',
            popularity_col='popularity'
        )
        
        # 結果記録
        monthly_values = list(result.monthly_rois.values()) if result.monthly_rois else [0]
        results.append({
            'ev_threshold': ev_threshold,
            'roi': result.roi,
            'hit_rate': result.hit_rate,
            'n_bets': result.n_bets,
            'total_bet': result.total_bet,
            'total_payout': result.total_payout,
            'favorite_rate': result.favorite_selection_rate,
            'monthly_std': np.std(monthly_values),
            'monthly_min': min(monthly_values),
            'monthly_max': max(monthly_values)
        })
    
    # 結果表示
    print("\n" + "=" * 70)
    print("EV閾値感度分析結果")
    print("=" * 70)
    
    results_df = pd.DataFrame(results)
    
    print("\n{:<12} {:>8} {:>8} {:>8} {:>12} {:>12} {:>10}".format(
        "EV閾値", "ROI%", "的中率%", "ベット数", "投資額", "払戻額", "月別Std"
    ))
    print("-" * 80)
    
    for _, row in results_df.iterrows():
        print("{:<12.2f} {:>8.1f} {:>8.1f} {:>8d} {:>12,} {:>12,} {:>10.1f}".format(
            row['ev_threshold'],
            row['roi'],
            row['hit_rate'],
            int(row['n_bets']),
            int(row['total_bet']),
            int(row['total_payout']),
            row['monthly_std']
        ))
    
    # 最適なEV閾値を提案
    print("\n" + "-" * 70)
    print("分析結果:")
    print("-" * 70)
    
    # ROI 100%以上で月別Stdが最小のものを選択
    profitable = results_df[results_df['roi'] >= 100]
    if len(profitable) > 0:
        best = profitable.loc[profitable['monthly_std'].idxmin()]
        print(f"  推奨EV閾値: {best['ev_threshold']:.2f}")
        print(f"  期待ROI: {best['roi']:.1f}%")
        print(f"  月別安定性(Std): {best['monthly_std']:.1f}")
    else:
        best = results_df.loc[results_df['roi'].idxmax()]
        print(f"  最高ROI EV閾値: {best['ev_threshold']:.2f}")
        print(f"  期待ROI: {best['roi']:.1f}%")
        print(f"  ※ROI 100%超過なし")
    
    # 結果保存
    output_path = Path('keibaai/data/evaluation/ev_threshold_analysis.csv')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_path, index=False)
    print(f"\n結果保存: {output_path}")
    
    return results_df


if __name__ == '__main__':
    run_sensitivity_analysis()
