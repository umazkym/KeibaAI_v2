"""
バックテスト結果の妥当性検証

ROI 91.8%が現実的かどうか詳細分析
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.hybrid_features import HybridFeatureEngineerV3, FeatureConfig


def main():
    print("=" * 70)
    print("バックテスト結果の妥当性検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[(races['race_date'] >= '2020-01-01') & (races['race_date'] <= '2024-06-30')]
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # テスト期間の抽出
    test_start = pd.Timestamp('2024-01-06')
    test_df = races[races['race_date'] >= test_start].copy()
    
    print(f"\nテスト期間: {test_df['race_date'].min()} - {test_df['race_date'].max()}")
    print(f"レース数: {test_df['race_id'].nunique()}")
    print(f"出走馬数: {len(test_df)}")
    
    # === 基本統計 ===
    print("\n" + "=" * 50)
    print("[1] テスト期間の基本統計")
    print("=" * 50)
    
    # 勝馬のオッズ分布
    winners = test_df[test_df['finish_position'] == 1]
    print(f"\n勝馬数: {len(winners)}")
    print(f"勝馬のオッズ分布:")
    print(f"  平均: {winners['win_odds'].mean():.1f}倍")
    print(f"  中央値: {winners['win_odds'].median():.1f}倍")
    print(f"  最大: {winners['win_odds'].max():.1f}倍")
    
    # 高配当の勝馬
    high_odds_winners = winners[winners['win_odds'] >= 20]
    print(f"\n20倍以上の勝馬: {len(high_odds_winners)}頭 ({100*len(high_odds_winners)/len(winners):.1f}%)")
    
    very_high = winners[winners['win_odds'] >= 50]
    print(f"50倍以上の勝馬: {len(very_high)}頭 ({100*len(very_high)/len(winners):.1f}%)")
    
    # === ROI 91.8%の検証 ===
    print("\n" + "=" * 50)
    print("[2] ROI 91.8%の妥当性")
    print("=" * 50)
    
    # 報告された数値
    total_bet = 3_385_900  # 円
    total_payout = 3_109_157  # 円
    bet_count = 12_852
    hit_count = int(12_852 * 0.019)  # 約244件
    
    print(f"\n報告された数値:")
    print(f"  ベット数: {bet_count:,}")
    print(f"  総投資: ¥{total_bet:,}")
    print(f"  1ベットあたり: ¥{total_bet/bet_count:.0f}")
    print(f"  的中数（推定）: {hit_count}")
    print(f"  総払戻: ¥{total_payout:,}")
    print(f"  平均払戻（的中時）: ¥{total_payout/hit_count:,.0f}")
    
    # 必要な平均オッズ
    avg_bet = total_bet / bet_count  # 約263円/ベット
    avg_payout_per_hit = total_payout / hit_count  # 12,742円
    required_avg_odds = avg_payout_per_hit / 100  # 127倍!?
    
    print(f"\n[計算検証]")
    print(f"  平均ベット額: ¥{avg_bet:.0f}")
    print(f"  的中あたり払戻: ¥{avg_payout_per_hit:,.0f}")
    print(f"  必要平均オッズ: {required_avg_odds:.1f}倍")
    
    # === 高オッズ馬の的中可能性 ===
    print("\n" + "=" * 50)
    print("[3] 高オッズ馬の的中可能性")
    print("=" * 50)
    
    # オッズ帯別の勝率
    test_df['odds_range'] = pd.cut(
        test_df['win_odds'],
        bins=[0, 3, 5, 10, 20, 50, 100, 1000],
        labels=['~3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50-100倍', '100倍~']
    )
    
    odds_stats = test_df.groupby('odds_range').apply(
        lambda x: pd.Series({
            '出走数': len(x),
            '勝利数': (x['finish_position'] == 1).sum(),
            '勝率': (x['finish_position'] == 1).sum() / len(x) * 100,
            '平均オッズ': x['win_odds'].mean(),
        })
    ).round(2)
    
    print("\nオッズ帯別の勝率:")
    print(odds_stats.to_string())
    
    # === 結論 ===
    print("\n" + "=" * 70)
    print("結論")
    print("=" * 70)
    
    # 100倍以上の勝馬数
    ultra_high = winners[winners['win_odds'] >= 100]
    
    print(f"""
[分析結果]

1. 報告されたROI 91.8%が正しい場合:
   - 的中数: 約{hit_count}件
   - 平均払戻オッズ: {required_avg_odds:.1f}倍（非常に高い）

2. テスト期間の高配当勝馬:
   - 100倍以上の勝馬: {len(ultra_high)}頭
   - 50-100倍の勝馬: {len(winners[(winners['win_odds'] >= 50) & (winners['win_odds'] < 100)])}頭

3. 必要な的中パターン:
   - {hit_count}件の的中で平均{required_avg_odds:.1f}倍のオッズが必要
   - これは実現可能か？

[疑問点]
- ベット額が非常に小さい（¥263/ベット）
- Kelly分数0.15でこの金額は、予測確率が低い馬に多くベットしている
- 高オッズ馬への少額分散ベットなら可能性あり

[推奨確認事項]
1. 実際のベット対象馬のオッズ分布を確認
2. 的中した馬の具体的なオッズを確認
3. 特定の高配当的中がROIを押し上げていないか
""")


if __name__ == '__main__':
    main()
