"""
V7 リーク・過学習の徹底検証

検証項目:
1. shift(1)が正しく動作し、当日のレース結果を使用していないか
2. Test期間の特定の馬の累積統計を手動で再計算し、V7の計算と一致するか
3. 「未来の情報」が混入していないか
4. 高ROIが正当かどうか
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

print("="*80)
print("V7 リーク・過学習 徹底検証")
print("="*80)

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

print(f"\nTrain期間: {train_df['race_date'].min()} - {train_df['race_date'].max()}")
print(f"Test期間:  {test_df['race_date'].min()} - {test_df['race_date'].max()}")

# V7 fit & transform
fe = LeakFreeFeatureEngineerV7()
fe.fit(races_df=train_df)
test_features = fe.transform(test_df)

# ================================================================
# 検証1: 特定の馬の累積統計を手動計算と比較
# ================================================================
print("\n" + "="*80)
print("検証1: 手動計算との突合（特定の馬を詳細に検証）")
print("="*80)

# Test期間で複数回出走した馬を探す
test_df_sorted = test_df.sort_values('race_date')
horse_counts = test_df_sorted.groupby('horse_id').size()
repeat_horses = horse_counts[horse_counts >= 3].index.tolist()

if repeat_horses:
    # 最初の馬で詳細検証
    target_horse = repeat_horses[0]
    
    # 全レース履歴（Train + Test）
    all_races = pd.concat([train_df, test_df]).sort_values('race_date')
    horse_all = all_races[all_races['horse_id'] == target_horse].sort_values('race_date')
    
    print(f"\n馬ID: {target_horse}")
    print(f"全レース数: {len(horse_all)}")
    
    # Train期間のレース
    horse_train = horse_all[horse_all['race_date'] < '2024-07-01']
    print(f"Train期間レース数: {len(horse_train)}")
    
    # Test期間のレース
    horse_test = horse_all[horse_all['race_date'] >= '2025-01-01']
    print(f"Test期間レース数: {len(horse_test)}")
    
    # V7が計算した特徴量
    v7_horse = test_features[test_features['horse_id'] == target_horse].sort_values('race_date')
    
    print("\n【手動計算 vs V7計算】")
    cum_races = 0
    cum_wins = 0
    
    # Train期間の累積を計算
    for idx, row in horse_train.iterrows():
        cum_races += 1
        if row['finish_position'] == 1:
            cum_wins += 1
    
    print(f"Train期間終了時: レース数={cum_races}, 勝利数={cum_wins}")
    
    # Test期間の各レースについて検証
    errors = []
    for i, (idx, row) in enumerate(horse_test.iterrows()):
        race_date = row['race_date']
        race_id = row['race_id']
        horse_num = row['horse_number']
        actual_finish = row['finish_position']
        
        # V7の計算結果
        v7_row = v7_horse[v7_horse['race_id'] == race_id]
        if len(v7_row) == 0:
            print(f"  [WARN] race_id={race_id}がV7結果に見つからない")
            continue
        v7_row = v7_row.iloc[0]
        v7_total_races = v7_row.get('horse_total_races', np.nan)
        v7_win_rate = v7_row.get('horse_win_rate', np.nan)
        
        # 手動計算（このレースの前までの累積）
        expected_races = cum_races
        expected_win_rate = cum_wins / cum_races if cum_races >= 3 else np.nan
        
        # 比較
        races_match = abs(v7_total_races - expected_races) < 0.01 if pd.notna(v7_total_races) else expected_races < 3
        rate_match = (pd.isna(v7_win_rate) and pd.isna(expected_win_rate)) or \
                     (pd.notna(v7_win_rate) and pd.notna(expected_win_rate) and abs(v7_win_rate - expected_win_rate) < 0.001)
        
        status = "✓" if races_match and rate_match else "✗"
        if not (races_match and rate_match):
            errors.append((race_date, race_id))
        
        print(f"\n  {race_date.date()} (race_id={race_id}):")
        print(f"    このレースの着順: {int(actual_finish)}着")
        expected_win_rate_str = f"{expected_win_rate:.3f}" if pd.notna(expected_win_rate) else "N/A"
        v7_win_rate_str = f"{v7_win_rate:.3f}" if pd.notna(v7_win_rate) else "N/A"
        print(f"    手動計算: total_races={expected_races}, win_rate={expected_win_rate_str}")
        print(f"    V7計算:   total_races={v7_total_races}, win_rate={v7_win_rate_str}")
        print(f"    結果: {status}")
        
        # このレースの結果を累積に追加（次のレースで使用）
        cum_races += 1
        if actual_finish == 1:
            cum_wins += 1
    
    if errors:
        print(f"\n⚠️ 不一致が{len(errors)}件あります!")
    else:
        print("\n✓ 全件一致!")

# ================================================================
# 検証2: 当日のレース結果がリークしていないか
# ================================================================
print("\n" + "="*80)
print("検証2: 当日結果のリークチェック（勝った馬の統計に自身の勝利が含まれていないか）")
print("="*80)

# テスト期間で勝った馬を抽出
test_winners = test_features[test_features['finish_position'] == 1]
print(f"\nTest期間の勝利数: {len(test_winners)}")

# それらの馬のhorse_win_rateが「勝利分を含んで高くなっている」かチェック
leak_detected = False
sample_winners = test_winners.head(10)

for idx, row in sample_winners.iterrows():
    horse_id = row['horse_id']
    race_date = row['race_date']
    v7_total_races = row.get('horse_total_races', np.nan)
    v7_win_rate = row.get('horse_win_rate', np.nan)
    
    # このレースより前の全勝利数を手動で計算
    all_races = pd.concat([train_df, test_df]).sort_values('race_date')
    prior_races = all_races[(all_races['horse_id'] == horse_id) & (all_races['race_date'] < race_date)]
    prior_wins = (prior_races['finish_position'] == 1).sum()
    prior_total = len(prior_races)
    
    expected_rate = prior_wins / prior_total if prior_total >= 3 else np.nan
    
    if pd.notna(v7_win_rate) and pd.notna(expected_rate):
        if abs(v7_win_rate - expected_rate) > 0.001:
            print(f"\n⚠️ リーク疑い: horse_id={horse_id}, date={race_date.date()}")
            print(f"   V7 win_rate={v7_win_rate:.3f}, 期待値={expected_rate:.3f}")
            leak_detected = True

if not leak_detected:
    print("✓ 当日結果のリークは検出されませんでした")

# ================================================================
# 検証3: ROIが異常に高い原因の調査
# ================================================================
print("\n" + "="*80)
print("検証3: 高ROI(148%)の妥当性検証")
print("="*80)

# 的中率と平均オッズの関係
test_top1 = test_features.copy()
test_top1['score'] = np.random.randn(len(test_top1))  # ダミースコア
test_top1['rank'] = test_top1.groupby('race_id')['score'].rank(ascending=False)

# 実際のTop1予測で的中した馬のオッズ分布
print("\n【基礎統計】")
print(f"Test期間の全行数: {len(test_features)}")
print(f"Test期間のユニークレース数: {test_features['race_id'].nunique()}")
print(f"Test期間の平均オッズ: {test_features['win_odds'].mean():.1f}")
print(f"Test期間の中央値オッズ: {test_features['win_odds'].median():.1f}")

# 「1着の馬」のオッズ分布
winners_df = test_features[test_features['finish_position'] == 1]
print(f"\n【1着になった馬のオッズ分布】")
print(f"平均: {winners_df['win_odds'].mean():.1f}")
print(f"中央値: {winners_df['win_odds'].median():.1f}")
print(f"最大: {winners_df['win_odds'].max():.1f}")

# オッズ帯別の1着率
print(f"\n【オッズ帯別の実際の1着率（全体、モデルなし）】")
for odds_min, odds_max in [(1,3), (3,5), (5,10), (10,20), (20,50), (50,80)]:
    subset = test_features[(test_features['win_odds'] >= odds_min) & (test_features['win_odds'] < odds_max)]
    if len(subset) > 0:
        win_rate = (subset['finish_position'] == 1).mean() * 100
        fair_roi = win_rate * (odds_min + odds_max) / 2  # 概算
        print(f"  {odds_min:>2}-{odds_max:<2}倍: 1着率={win_rate:>5.1f}%, 概算ROI={fair_roi:>5.0f}%")

print("\n" + "="*80)
print("検証完了")
print("="*80)
