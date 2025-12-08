"""
ROI 153.7%が本当か徹底検証

疑問点：
1. finish_positionが重複していないか？
2. win_oddsが重複していないか？
3. EVの計算が正しいか？
4. 実際に的中した馬のオッズは妥当か？
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.comprehensive_features import ComprehensiveFeatureEngineer
from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.modules.models.probability_calibrator import Layer2_ProbabilityCalibrator


def main():
    print("=" * 70)
    print("🔍 ROI 153.7%の徹底検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\n1. データ読み込み...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    print(f"   全レコード数: {len(races):,}")
    
    # 時系列分割
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    # ========================================
    # 重要: finish_positionとwin_oddsを特徴量変換前に保持
    # ========================================
    print("\n2. 結果情報を保持...")
    preserved = races[['race_id', 'horse_id', 'finish_position', 'win_odds']].copy()
    print(f"   preserved.finish_position のユニーク値: {sorted(preserved['finish_position'].unique()[:10])}")
    print(f"   preserved.win_odds の範囲: {preserved['win_odds'].min():.1f} - {preserved['win_odds'].max():.1f}")
    
    # 訓練データのみで特徴量エンジニアをfit
    train_races = races[races['race_date'] <= train_end]
    
    print("\n3. 特徴量生成...")
    fe = ComprehensiveFeatureEngineer()
    fe.fit(train_races, pedigrees_df=pedigrees)
    df = fe.transform(races)
    
    print(f"   変換後のカラム数: {len(df.columns)}")
    print(f"   'finish_position' in df.columns: {'finish_position' in df.columns}")
    print(f"   'win_odds' in df.columns: {'win_odds' in df.columns}")
    
    # 保持した列をマージ
    print("\n4. 保持した列をマージ...")
    df = df.merge(preserved, on=['race_id', 'horse_id'], how='left', suffixes=('_feat', '_preserved'))
    
    # 重複チェック
    finish_cols = [c for c in df.columns if 'finish_position' in c]
    odds_cols = [c for c in df.columns if 'win_odds' in c]
    print(f"   finish_position関連カラム: {finish_cols}")
    print(f"   win_odds関連カラム: {odds_cols}")
    
    # 使用するカラムを明確に
    if 'finish_position_preserved' in df.columns:
        df['finish_position'] = df['finish_position_preserved']
    if 'win_odds_preserved' in df.columns:
        df['win_odds'] = df['win_odds_preserved']
    
    # Test2期間を取得
    test2_df = df[df['race_date'] > test1_end].copy()
    print(f"\n5. Test2期間: {len(test2_df):,} レコード")
    
    # ========================================
    # モデル訓練
    # ========================================
    train_df = df[df['race_date'] <= train_end].copy()
    valid_df = df[(df['race_date'] > train_end) & (df['race_date'] <= valid_end)].copy()
    
    exclude_cols = {'race_id', 'horse_id', 'race_date', 'race_name', 'venue', 'horse_name',
                    'jockey_id', 'jockey_name', 'trainer_id', 'trainer_name', 'owner_name',
                    'finish_position', 'finish_position_feat', 'finish_position_preserved',
                    'win_odds', 'win_odds_feat', 'win_odds_preserved',
                    'popularity', 'horse_number', 'track_surface', 'track_condition', 
                    'distance_category', 'sire_id'}
    
    feature_cols = [c for c in train_df.columns if c not in exclude_cols 
                    and train_df[c].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    print(f"\n6. 使用特徴量数: {len(feature_cols)}")
    
    # NaN処理
    for col in feature_cols:
        median_val = train_df[col].median()
        for d in [train_df, valid_df, test2_df]:
            d[col] = d[col].fillna(median_val if pd.notna(median_val) else 0)
    
    X_train, y_train = train_df[feature_cols], train_df['finish_position']
    groups_train = train_df['race_id']
    X_valid, y_valid = valid_df[feature_cols], valid_df['finish_position']
    groups_valid = valid_df['race_id']
    
    print("\n7. モデル訓練...")
    config = {'name': 'shallow', 'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    layer1 = Layer1_AbilityEstimator(config=config)
    layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
    
    train_pred = layer1.predict(X_train, groups_train)
    train_scores = train_pred['ability_score'].values
    layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
    layer2.fit(train_scores, y_train.values, groups_train.values)
    
    # Test2で予測
    X_test2 = test2_df[feature_cols]
    groups_test2 = test2_df['race_id']
    test2_pred = layer1.predict(X_test2, groups_test2)
    test2_scores = test2_pred['ability_score'].values
    test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
    test2_probs = test2_probs_df['win_prob'].values
    
    # ========================================
    # ROI計算の詳細検証
    # ========================================
    print("\n" + "=" * 70)
    print("8. ROI計算の詳細検証")
    print("=" * 70)
    
    test2_df = test2_df.copy()
    test2_df['pred_prob'] = test2_probs
    test2_df['ev'] = test2_df['pred_prob'] * test2_df['win_odds'] * 0.8
    
    print(f"\n予測確率の統計:")
    print(f"  最小: {test2_df['pred_prob'].min():.4f}")
    print(f"  最大: {test2_df['pred_prob'].max():.4f}")
    print(f"  平均: {test2_df['pred_prob'].mean():.4f}")
    
    print(f"\nEVの統計:")
    print(f"  最小: {test2_df['ev'].min():.2f}")
    print(f"  最大: {test2_df['ev'].max():.2f}")
    print(f"  平均: {test2_df['ev'].mean():.2f}")
    
    # EV >= 1.0でフィルタ
    ev_threshold = 1.0
    bet_mask = test2_df['ev'] >= ev_threshold
    bet_df = test2_df[bet_mask].copy()
    
    print(f"\nEV >= {ev_threshold} でフィルタ後:")
    print(f"  ベット対象: {len(bet_df):,} / {len(test2_df):,} ({len(bet_df)/len(test2_df)*100:.1f}%)")
    
    # 的中馬を確認
    hits = bet_df[bet_df['finish_position'] == 1]
    print(f"  的中数: {len(hits)}")
    print(f"  的中率: {len(hits)/len(bet_df)*100:.2f}%")
    
    # 的中馬のオッズを確認
    print(f"\n的中馬のオッズ統計:")
    print(f"  最小: {hits['win_odds'].min():.1f}")
    print(f"  最大: {hits['win_odds'].max():.1f}")
    print(f"  平均: {hits['win_odds'].mean():.1f}")
    print(f"  中央値: {hits['win_odds'].median():.1f}")
    
    # オッズ分布
    print(f"\n的中馬のオッズ分布:")
    for low, high in [(1, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 500)]:
        count = len(hits[(hits['win_odds'] >= low) & (hits['win_odds'] < high)])
        if count > 0:
            print(f"  {low}-{high}倍: {count}頭")
    
    # ROI計算
    total_bet = len(bet_df) * 100
    total_payout = (hits['win_odds'] * 100).sum()
    roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
    
    print(f"\nROI計算:")
    print(f"  総ベット額: {total_bet:,}円")
    print(f"  総払戻額: {total_payout:,.0f}円")
    print(f"  ROI: {roi:.1f}%")
    
    # ========================================
    # ランダムベースラインとの比較
    # ========================================
    print("\n" + "=" * 70)
    print("9. ランダムベースラインとの比較")
    print("=" * 70)
    
    all_winners = test2_df[test2_df['finish_position'] == 1]
    random_total_bet = len(test2_df) * 100
    random_total_payout = (all_winners['win_odds'] * 100).sum()
    random_roi = (random_total_payout / random_total_bet * 100) if random_total_bet > 0 else 0
    
    print(f"\nランダム選択（全馬に均等ベット）:")
    print(f"  総ベット額: {random_total_bet:,}円")
    print(f"  総払戻額: {random_total_payout:,.0f}円")
    print(f"  ROI: {random_roi:.1f}%")
    
    # ========================================
    # 問題の可能性を検証
    # ========================================
    print("\n" + "=" * 70)
    print("10. 問題の可能性を検証")
    print("=" * 70)
    
    # 予測確率が高い馬の勝率が本当に高いか？
    print("\n予測確率別の実際の勝率:")
    test2_df['prob_bin'] = pd.cut(test2_df['pred_prob'], bins=[0, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0])
    prob_analysis = test2_df.groupby('prob_bin', observed=True).apply(
        lambda x: pd.Series({
            'count': len(x),
            'wins': (x['finish_position'] == 1).sum(),
            'actual_win_rate': (x['finish_position'] == 1).mean() * 100,
            'avg_odds': x['win_odds'].mean()
        }), include_groups=False
    )
    print(prob_analysis)
    
    # 予測が正しければ、予測確率と実際の勝率が近いはず
    print("\n予測確率 vs 実際の勝率 の乖離（キャリブレーション）:")
    for low, high in [(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 1.0)]:
        subset = test2_df[(test2_df['pred_prob'] >= low) & (test2_df['pred_prob'] < high)]
        if len(subset) > 0:
            actual_rate = (subset['finish_position'] == 1).mean()
            expected_rate = subset['pred_prob'].mean()
            diff = actual_rate - expected_rate
            print(f"  予測{low:.0%}-{high:.0%}: 実際={actual_rate:.1%}, 予測={expected_rate:.1%}, 差={diff:+.1%}")


if __name__ == '__main__':
    main()
