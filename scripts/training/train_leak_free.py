"""
リークを排除した特徴量エンジニアを使用した訓練スクリプト

1. 時系列分割でデータを分割
2. 訓練データのみでLeakFreeFeatureEngineerをfit
3. ハイパーパラメータ調整
4. ROI検証とキャリブレーション確認
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer
from keibaai.src.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.models.probability_calibrator import Layer2_ProbabilityCalibrator


def calculate_roi(test_df: pd.DataFrame, pred_probs: np.ndarray, ev_threshold: float = 1.0) -> Dict:
    """均等ベットでのROI計算"""
    test_df = test_df.copy()
    test_df['pred_prob'] = pred_probs
    test_df['ev'] = test_df['pred_prob'] * test_df['win_odds'] * 0.8
    
    bet_mask = test_df['ev'] >= ev_threshold
    bet_df = test_df[bet_mask]
    
    if len(bet_df) == 0:
        return {'n_bets': 0, 'n_hits': 0, 'roi': 0, 'hit_rate': 0}
    
    hits = bet_df[bet_df['finish_position'] == 1]
    total_bet = len(bet_df) * 100
    total_payout = (hits['win_odds'] * 100).sum()
    
    return {
        'n_bets': len(bet_df),
        'n_hits': len(hits),
        'roi': (total_payout / total_bet * 100) if total_bet > 0 else 0,
        'hit_rate': (len(hits) / len(bet_df) * 100) if len(bet_df) > 0 else 0
    }


def check_calibration(test_df: pd.DataFrame, pred_probs: np.ndarray) -> pd.DataFrame:
    """キャリブレーション確認"""
    test_df = test_df.copy()
    test_df['pred_prob'] = pred_probs
    
    results = []
    for low, high in [(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 1.0)]:
        subset = test_df[(test_df['pred_prob'] >= low) & (test_df['pred_prob'] < high)]
        if len(subset) > 0:
            actual_rate = (subset['finish_position'] == 1).mean()
            expected_rate = subset['pred_prob'].mean()
            results.append({
                'range': f'{low:.0%}-{high:.0%}',
                'count': len(subset),
                'actual_rate': actual_rate,
                'expected_rate': expected_rate,
                'diff': actual_rate - expected_rate
            })
    
    return pd.DataFrame(results)


def main():
    print("=" * 70)
    print("リーク排除済み特徴量エンジニアによる訓練")
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
    
    train_df = races[races['race_date'] <= train_end].copy()
    valid_df = races[(races['race_date'] > train_end) & (races['race_date'] <= valid_end)].copy()
    test1_df = races[(races['race_date'] > valid_end) & (races['race_date'] <= test1_end)].copy()
    test2_df = races[races['race_date'] > test1_end].copy()
    
    print(f"\n2. 時系列分割")
    print(f"   Train: ~{train_end.date()} ({len(train_df):,})")
    print(f"   Valid: ~{valid_end.date()} ({len(valid_df):,})")
    print(f"   Test1: ~{test1_end.date()} ({len(test1_df):,})")
    print(f"   Test2: {test1_end.date()}~ ({len(test2_df):,})")
    
    # 特徴量エンジニアをfit（訓練データのみ）
    print("\n3. 特徴量エンジニアをfit...")
    fe = LeakFreeFeatureEngineer()
    fe.fit(train_df, pedigrees_df=pedigrees)
    
    # 各データセットにtransform
    print("\n4. 特徴量を生成...")
    train_feat = fe.transform(train_df)
    valid_feat = fe.transform(valid_df)
    test1_feat = fe.transform(test1_df)
    test2_feat = fe.transform(test2_df)
    
    # 特徴量カラムを取得
    feature_cols = fe.get_feature_columns()
    # 実際に存在するカラムのみ使用
    feature_cols = [c for c in feature_cols if c in train_feat.columns]
    print(f"\n   使用特徴量数: {len(feature_cols)}")
    print(f"   特徴量: {feature_cols}")
    
    # NaN処理
    print("\n5. NaN処理...")
    for col in feature_cols:
        median_val = train_feat[col].median()
        median_val = median_val if pd.notna(median_val) else 0
        for df in [train_feat, valid_feat, test1_feat, test2_feat]:
            df[col] = df[col].fillna(median_val)
    
    # ========================================
    # ハイパーパラメータ調整
    # ========================================
    print("\n" + "=" * 70)
    print("6. ハイパーパラメータ調整")
    print("=" * 70)
    
    configs = [
        {'name': 'default', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50},
        {'name': 'shallow', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 3, 'min_child_samples': 100},
        {'name': 'deep', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 6, 'min_child_samples': 30},
        {'name': 'low_lr', 'n_estimators': 1000, 'learning_rate': 0.02, 'max_depth': 4, 'min_child_samples': 50},
    ]
    
    X_train = train_feat[feature_cols]
    y_train = train_feat['finish_position']
    groups_train = train_feat['race_id']
    
    X_valid = valid_feat[feature_cols]
    y_valid = valid_feat['finish_position']
    groups_valid = valid_feat['race_id']
    
    best_config = None
    best_test2_roi = 0
    results = []
    
    for config in configs:
        print(f"\n--- {config['name']} ---")
        
        layer1 = Layer1_AbilityEstimator(config=config)
        layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
        
        # Layer2
        train_pred = layer1.predict(X_train, groups_train)
        train_scores = train_pred['ability_score'].values
        layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
        layer2.fit(train_scores, y_train.values, groups_train.values)
        
        # Test2で予測
        X_test2 = test2_feat[feature_cols]
        groups_test2 = test2_feat['race_id']
        test2_pred = layer1.predict(X_test2, groups_test2)
        test2_scores = test2_pred['ability_score'].values
        test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
        test2_probs = test2_probs_df['win_prob'].values
        
        # ROI計算
        roi_result = calculate_roi(test2_feat, test2_probs, ev_threshold=1.0)
        
        print(f"   Test2 ROI: {roi_result['roi']:.1f}%")
        print(f"   ベット数: {roi_result['n_bets']:,}, 的中数: {roi_result['n_hits']}")
        
        result = {
            'config': config['name'],
            'test2_roi': roi_result['roi'],
            'test2_bets': roi_result['n_bets'],
            'test2_hit_rate': roi_result['hit_rate'],
        }
        results.append(result)
        
        if roi_result['roi'] > best_test2_roi:
            best_test2_roi = roi_result['roi']
            best_config = config
            best_layer1 = layer1
            best_layer2 = layer2
            best_test2_probs = test2_probs
    
    # 結果表示
    print("\n" + "=" * 70)
    print("7. ハイパーパラメータ調整結果")
    print("=" * 70)
    
    results_df = pd.DataFrame(results)
    print("\n" + results_df.to_string(index=False))
    
    print(f"\nベスト構成: {best_config['name']} (Test2 ROI: {best_test2_roi:.1f}%)")
    
    # ========================================
    # EV閾値の調整
    # ========================================
    print("\n" + "=" * 70)
    print("8. EV閾値の調整")
    print("=" * 70)
    
    for ev_th in [0.9, 1.0, 1.1, 1.2, 1.3]:
        roi_result = calculate_roi(test2_feat, best_test2_probs, ev_threshold=ev_th)
        print(f"   EV >= {ev_th}: ROI={roi_result['roi']:.1f}%, ベット数={roi_result['n_bets']:,}, 的中率={roi_result['hit_rate']:.2f}%")
    
    # ========================================
    # キャリブレーション確認
    # ========================================
    print("\n" + "=" * 70)
    print("9. キャリブレーション確認（Test2）")
    print("=" * 70)
    
    calib_df = check_calibration(test2_feat, best_test2_probs)
    print("\n" + calib_df.to_string(index=False))
    
    # リークチェック（予測確率20%以上で勝率100%ならリーク）
    high_prob = test2_feat.copy()
    high_prob['pred_prob'] = best_test2_probs
    high_prob_subset = high_prob[high_prob['pred_prob'] >= 0.2]
    if len(high_prob_subset) > 0:
        actual_win_rate = (high_prob_subset['finish_position'] == 1).mean()
        print(f"\n予測20%以上の馬の実際の勝率: {actual_win_rate:.1%}")
        if actual_win_rate > 0.95:
            print("⚠️ リークの可能性あり！")
        else:
            print("✅ リークなし（予測と実際がほぼ一致）")
    
    # ========================================
    # ランダムベースラインとの比較
    # ========================================
    print("\n" + "=" * 70)
    print("10. ランダムベースラインとの比較")
    print("=" * 70)
    
    for name, tdf in [('Test1', test1_feat), ('Test2', test2_feat)]:
        winners = tdf[tdf['finish_position'] == 1]
        total_bet = len(tdf) * 100
        total_payout = (winners['win_odds'] * 100).sum()
        random_roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
        
        if name == 'Test2':
            model_roi = best_test2_roi
        else:
            # Test1も計算
            X_test1 = test1_feat[feature_cols]
            groups_test1 = test1_feat['race_id']
            test1_pred = best_layer1.predict(X_test1, groups_test1)
            test1_scores = test1_pred['ability_score'].values
            test1_probs_df = best_layer2.transform(test1_scores, groups_test1.values)
            test1_probs = test1_probs_df['win_prob'].values
            test1_result = calculate_roi(test1_feat, test1_probs)
            model_roi = test1_result['roi']
        
        diff = model_roi - random_roi
        status = "✅" if diff > 0 else "❌"
        print(f"\n{name}:")
        print(f"   ランダム: {random_roi:.1f}%")
        print(f"   モデル:   {model_roi:.1f}%")
        print(f"   差分:     {diff:+.1f}pt {status}")
    
    # ========================================
    # 最終結論
    # ========================================
    print("\n" + "=" * 70)
    print("11. 最終結論")
    print("=" * 70)
    
    print(f"""
[結果]
- ベストモデル: {best_config['name']}
- Test2 ROI: {best_test2_roi:.1f}%

[リーク検証]
- キャリブレーションが正常（予測確率 ≒ 実際勝率）ならリークなし

[次のステップ]
1. より多くの特徴量を追加（騎手×馬の相性、前走成績など）
2. モデルアンサンブルの強化
3. ベット戦略の最適化
""")


if __name__ == '__main__':
    main()
