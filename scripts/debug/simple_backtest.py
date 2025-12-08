"""
バックテストの詳細ログを出力して問題を特定
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.hybrid_features import HybridFeatureEngineerV3, FeatureConfig


def simple_backtest_detailed():
    """シンプルなバックテストを詳細ログ付きで実行"""
    
    print("=" * 70)
    print("詳細バックテスト")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[(races['race_date'] >= '2020-01-01') & (races['race_date'] <= '2024-06-30')]
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # 特徴量生成
    print("\n特徴量生成中...")
    
    # finish_positionとwin_oddsを変換前に保持
    preserved = races[['race_id', 'horse_id', 'finish_position', 'win_odds']].copy()
    
    fe = HybridFeatureEngineerV3()
    fe.fit(races, pedigrees_df=pedigrees)
    df = fe.transform(races)
    
    # 保持した列をマージ
    df = df.merge(preserved, on=['race_id', 'horse_id'], how='left')
    
    # 時系列分割
    test_start = pd.Timestamp('2024-01-06')
    train_df = df[df['race_date'] < test_start].copy()
    test_df = df[df['race_date'] >= test_start].copy()
    
    print(f"\nTrain: {len(train_df):,}")
    print(f"Test: {len(test_df):,}")
    
    # 単純な予測：調教師勝率のランクで予測
    # (実際のモデルは複雑だが、単純化して問題を特定)
    
    # テスト期間の各レースで最も予測確率が高い馬を選ぶ
    # 仮の確率として調教師勝率ランクを使用
    
    test_df['pred_rank'] = test_df.groupby('race_id')['trainer_overall_win_rate'].rank(ascending=False, method='first')
    
    # 上位N頭にベット
    n_bet_per_race = 3
    bet_df = test_df[test_df['pred_rank'] <= n_bet_per_race].copy()
    
    print(f"\nベット対象馬: {len(bet_df):,}")
    
    # 的中数
    hits = bet_df[bet_df['finish_position'] == 1]
    
    print(f"的中数: {len(hits)}")
    print(f"的中率: {100 * len(hits) / len(bet_df):.2f}%")
    
    # 払戻
    total_bet = len(bet_df) * 100  # 100円ずつ
    total_payout = (hits['win_odds'] * 100).sum()
    
    print(f"\n総投資: ¥{total_bet:,}")
    print(f"総払戻: ¥{total_payout:,.0f}")
    print(f"ROI: {100 * total_payout / total_bet:.1f}%")
    
    # 的中馬のオッズ分布
    print("\n的中馬のオッズ分布:")
    print(f"  平均: {hits['win_odds'].mean():.1f}倍")
    print(f"  中央値: {hits['win_odds'].median():.1f}倍")
    print(f"  最大: {hits['win_odds'].max():.1f}倍")
    
    # 高配当的中の詳細
    high_odds_hits = hits[hits['win_odds'] >= 50]
    print(f"\n50倍以上の的中: {len(high_odds_hits)}件")
    print(f"  合計払戻: ¥{(high_odds_hits['win_odds'] * 100).sum():,.0f}")
    
    # === 正しいROI計算 ===
    print("\n" + "=" * 70)
    print("現実的なROI試算")
    print("=" * 70)
    
    # 勝馬全体の期待値
    all_winners = test_df[test_df['finish_position'] == 1]
    avg_win_odds = all_winners['win_odds'].mean()
    
    print(f"\n勝馬平均オッズ: {avg_win_odds:.1f}倍")
    print(f"JRA控除率: 20%")
    print(f"期待回収率（ランダム選択）: {avg_win_odds * 0.8 / (len(test_df) / len(all_winners)):.1%}")
    
    # 調教師勝率で選ぶとどうなるか
    # 上位10%の調教師の馬だけ選んだ場合
    top_trainers = train_df.groupby('trainer_id').apply(
        lambda x: (x['finish_position'] == 1).sum() / len(x)
    ).nlargest(int(0.1 * len(train_df['trainer_id'].unique())))
    
    top_trainer_horses = test_df[test_df['trainer_id'].isin(top_trainers.index)]
    top_trainer_winners = top_trainer_horses[top_trainer_horses['finish_position'] == 1]
    
    if len(top_trainer_horses) > 0:
        win_rate = len(top_trainer_winners) / len(top_trainer_horses)
        avg_odds = top_trainer_winners['win_odds'].mean() if len(top_trainer_winners) > 0 else 0
        
        print(f"\n上位10%調教師の馬:")
        print(f"  出走数: {len(top_trainer_horses)}")
        print(f"  勝利数: {len(top_trainer_winners)}")
        print(f"  勝率: {100 * win_rate:.2f}%")
        print(f"  勝馬平均オッズ: {avg_odds:.1f}倍")
        print(f"  期待ROI: {100 * win_rate * avg_odds * 0.8:.1f}%")


if __name__ == '__main__':
    simple_backtest_detailed()
