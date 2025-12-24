"""
モデル予測の妥当性を検証

予測確率が異常に高くないか確認
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.hybrid_features import HybridFeatureEngineerV3, FeatureConfig
from keibaai.src.models.hybrid_betting_ai import HybridHorseRacingAI


def verify_model_predictions():
    """モデルの予測確率を検証"""
    
    print("=" * 70)
    print("モデル予測の妥当性検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[(races['race_date'] >= '2020-01-01') & (races['race_date'] <= '2024-06-30')]
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # finish_positionとwin_oddsを保持
    preserved = races[['race_id', 'horse_id', 'finish_position', 'win_odds', 'popularity']].copy()
    
    # 特徴量生成
    print("\n特徴量生成中...")
    fe = HybridFeatureEngineerV3()
    fe.fit(races, pedigrees_df=pedigrees)
    df = fe.transform(races)
    
    # 保持した列をマージ
    df = df.merge(preserved, on=['race_id', 'horse_id'], how='left')
    
    # 時系列分割
    test_start = pd.Timestamp('2024-01-06')
    val_start = test_start - pd.DateOffset(months=6)
    train_end = val_start - pd.DateOffset(months=1)
    
    train_df = df[df['race_date'] <= train_end].copy()
    valid_df = df[(df['race_date'] > train_end) & (df['race_date'] < test_start)].copy()
    test_df = df[df['race_date'] >= test_start].copy()
    
    print(f"\nTrain: {len(train_df):,}")
    print(f"Valid: {len(valid_df):,}")
    print(f"Test: {len(test_df):,}")
    
    # 特徴量列の選択
    exclude_cols = {'race_id', 'horse_id', 'race_date', 'race_name', 'venue', 'horse_name',
                    'jockey_id', 'jockey_name', 'trainer_id', 'trainer_name', 'owner_name',
                    'finish_position', 'win_odds', 'popularity', 'horse_number',
                    'track_surface', 'track_condition', 'distance_category', 'sire_id'}
    
    feature_cols = [c for c in train_df.columns if c not in exclude_cols 
                    and train_df[c].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    # NaN処理
    for col in feature_cols:
        for d in [train_df, valid_df, test_df]:
            d[col] = d[col].fillna(d[col].median() if len(d) > 0 else 0)
    
    # モデル訓練
    print("\nモデル訓練中...")
    model = HybridHorseRacingAI(ev_threshold=1.0, kelly_fraction=0.15)
    
    X_train = train_df[feature_cols]
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_valid = valid_df[feature_cols]
    y_valid = valid_df['finish_position']
    groups_valid = valid_df['race_id']
    
    model.train(
        X_train, y_train, groups_train,
        X_valid, y_valid, groups_valid
    )
    
    # テストデータで予測
    print("\n予測中...")
    X_test = test_df[feature_cols]
    groups_test = test_df['race_id']
    
    predictions = model.layer1.predict(X_test, groups_test)
    
    # キャリブレーション
    calibrated_probs = model.layer2.transform(predictions, groups_test)
    
    # 予測確率の分布を確認
    print("\n" + "=" * 50)
    print("予測確率の分布")
    print("=" * 50)
    
    print(f"\n全馬の予測確率:")
    print(f"  平均: {calibrated_probs.mean():.4f}")
    print(f"  中央値: {np.median(calibrated_probs):.4f}")
    print(f"  最大: {calibrated_probs.max():.4f}")
    print(f"  最小: {calibrated_probs.min():.4f}")
    print(f"  標準偏差: {calibrated_probs.std():.4f}")
    
    # 予測確率の分布
    print("\n予測確率の分布:")
    percentiles = [10, 25, 50, 75, 90, 95, 99]
    for p in percentiles:
        print(f"  {p}%ile: {np.percentile(calibrated_probs, p):.4f}")
    
    # 予測確率が高い馬のオッズ
    test_df['pred_prob'] = calibrated_probs
    
    high_prob = test_df[test_df['pred_prob'] >= 0.15]
    print(f"\n予測確率15%以上の馬: {len(high_prob)}")
    if len(high_prob) > 0:
        print(f"  平均オッズ: {high_prob['win_odds'].mean():.1f}倍")
        winners_high = high_prob[high_prob['finish_position'] == 1]
        print(f"  勝利数: {len(winners_high)}")
        print(f"  勝率: {100 * len(winners_high) / len(high_prob):.2f}%")
    
    # EV > 1.0 の馬を確認
    test_df['ev'] = test_df['pred_prob'] * test_df['win_odds'] * 0.8
    
    ev_threshold = 1.0
    high_ev = test_df[test_df['ev'] >= ev_threshold]
    
    print(f"\n" + "=" * 50)
    print(f"EV >= {ev_threshold} の馬")
    print("=" * 50)
    
    print(f"\n該当馬数: {len(high_ev)}")
    if len(high_ev) > 0:
        print(f"予測確率:")
        print(f"  平均: {high_ev['pred_prob'].mean():.4f}")
        print(f"  中央値: {high_ev['pred_prob'].median():.4f}")
        
        print(f"オッズ:")
        print(f"  平均: {high_ev['win_odds'].mean():.1f}倍")
        print(f"  中央値: {high_ev['win_odds'].median():.1f}倍")
        
        winners_ev = high_ev[high_ev['finish_position'] == 1]
        print(f"\n勝利数: {len(winners_ev)}")
        print(f"勝率: {100 * len(winners_ev) / len(high_ev):.2f}%")
        
        # 期待ROI
        total_bet = len(high_ev) * 100
        total_payout = (winners_ev['win_odds'] * 100).sum()
        print(f"\n総投資（均等ベット）: ¥{total_bet:,}")
        print(f"総払戻: ¥{total_payout:,.0f}")
        print(f"ROI: {100 * total_payout / total_bet:.1f}%")
        
        # 勝馬の平均オッズ
        print(f"\n勝馬の平均オッズ: {winners_ev['win_odds'].mean():.1f}倍")


if __name__ == '__main__':
    verify_model_predictions()
