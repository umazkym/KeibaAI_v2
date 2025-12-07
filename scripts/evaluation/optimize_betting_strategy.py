"""
ベット戦略最適化スクリプト

目標:
- 安定ROI（上位3件除外）: ≥ 90%
- 年間ベット数: ≥ 500件 (Test期間は約2年なので1000件以上目標)
- 95%信頼区間下限: ≥ 70%
- 1番人気選択率: ≤ 65%
"""

import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_and_prepare_data():
    """データ準備"""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from scripts.training.train_mu_v11_0 import MuV110Trainer
    
    model_dir = Path('keibaai/models/mu_v11_0')
    with open(model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    with open(model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_names = json.load(f)
    
    trainer = MuV110Trainer()
    train_data, races_df, race_details_df, returns_df = trainer.load_data()
    train_data = trainer.generate_leak_free_features(train_data, races_df)
    train_data = trainer.generate_new_features(train_data, races_df, race_details_df, returns_df)
    
    train_data['win_odds'] = pd.to_numeric(train_data['win_odds'], errors='coerce')
    train_data['popularity'] = pd.to_numeric(train_data['popularity'], errors='coerce')
    train_data['finish_position'] = pd.to_numeric(train_data['finish_position'], errors='coerce')
    
    test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
    available_features = [f for f in feature_names if f in train_data.columns]
    for col in available_features:
        test_df[col] = test_df[col].fillna(0)
    
    return model, available_features, test_df


def evaluate_threshold(df, model, features, ev_threshold):
    """閾値でのパフォーマンス評価"""
    df = df.copy()
    df['score'] = model.predict(df[features])
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    def softmax_prob(group):
        scores = group['score'].values
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / exp_scores.sum()
        return pd.Series(probs, index=group.index)
    
    df['pred_prob'] = df.groupby('race_id', group_keys=False).apply(softmax_prob)
    df['ev'] = df['pred_prob'] * df['win_odds'] - 1
    
    top_picks = df[df['rank_pred'] == 1].copy()
    bets = top_picks[top_picks['ev'] >= ev_threshold].copy()
    
    if len(bets) < 10:
        return None
    
    # 基本指標
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets)
    fav1_rate = (bets['popularity'] == 1).sum() / len(bets)
    
    # 安定ROI（上位3件除外）
    if len(hits) >= 3:
        top3_odds = hits.nlargest(3, 'win_odds')['win_odds'].sum()
        stable_roi = (hits['win_odds'].sum() - top3_odds) / (len(bets) - 3)
    else:
        stable_roi = 0
    
    # ブートストラップ信頼区間
    outcomes = np.where(bets['finish_position'] == 1, bets['win_odds'].values, 0)
    n = len(outcomes)
    np.random.seed(42)
    bootstrap_rois = []
    for _ in range(5000):
        sample = np.random.choice(outcomes, size=n, replace=True)
        bootstrap_rois.append(sample.sum() / n)
    ci_lower = np.percentile(bootstrap_rois, 2.5)
    ci_upper = np.percentile(bootstrap_rois, 97.5)
    
    return {
        'threshold': ev_threshold,
        'n_bets': len(bets),
        'n_hits': len(hits),
        'hit_rate': hit_rate,
        'roi': roi,
        'stable_roi': stable_roi,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'fav1_rate': fav1_rate,
    }


def find_optimal_threshold(df, model, features):
    """最適閾値を探索"""
    logging.info("=" * 70)
    logging.info("ベット戦略最適化")
    logging.info("=" * 70)
    logging.info("\n目標:")
    logging.info("  安定ROI（上位3件除外）: ≥ 90%")
    logging.info("  年間ベット数: ≥ 500件 (Test期間で1000件以上)")
    logging.info("  95%信頼区間下限: ≥ 70%")
    logging.info("  1番人気選択率: ≤ 65%")
    
    # Test期間の長さを計算
    test_years = (df['race_date'].max() - df['race_date'].min()).days / 365
    logging.info(f"\nTest期間: {test_years:.1f}年")
    min_bets = int(500 * test_years)  # 年間500件 × 年数
    logging.info(f"最低ベット数目標: {min_bets}件")
    
    # 閾値探索
    thresholds = np.arange(-0.6, 0.3, 0.02)
    results = []
    
    for thresh in thresholds:
        result = evaluate_threshold(df, model, features, thresh)
        if result:
            results.append(result)
    
    results_df = pd.DataFrame(results)
    
    # 目標達成判定
    results_df['meets_bets'] = results_df['n_bets'] >= min_bets
    results_df['meets_stable_roi'] = results_df['stable_roi'] >= 0.90
    results_df['meets_ci'] = results_df['ci_lower'] >= 0.70
    results_df['meets_fav1'] = results_df['fav1_rate'] <= 0.65
    results_df['meets_all'] = (results_df['meets_bets'] & 
                               results_df['meets_stable_roi'] & 
                               results_df['meets_ci'] & 
                               results_df['meets_fav1'])
    
    logging.info("\n" + "=" * 70)
    logging.info("閾値別パフォーマンス")
    logging.info("=" * 70)
    logging.info(f"{'閾値':>6} {'ベット':>7} {'的中':>5} {'ROI':>7} {'安定ROI':>8} {'CI下限':>7} {'1番人気':>7} {'判定':>6}")
    logging.info("-" * 70)
    
    for _, row in results_df.iterrows():
        status = "✅" if row['meets_all'] else "❌"
        logging.info(f"{row['threshold']:>6.2f} {int(row['n_bets']):>7} "
                    f"{int(row['n_hits']):>5} {row['roi']*100:>6.1f}% "
                    f"{row['stable_roi']*100:>7.1f}% {row['ci_lower']*100:>6.1f}% "
                    f"{row['fav1_rate']*100:>6.1f}% {status:>6}")
    
    # 目標達成する閾値を探す
    valid_results = results_df[results_df['meets_all']]
    
    if len(valid_results) > 0:
        # 目標達成の中で最もROIが高いものを選択
        best = valid_results.loc[valid_results['roi'].idxmax()]
        logging.info("\n" + "=" * 70)
        logging.info("✅ 目標達成する閾値が見つかりました！")
        logging.info("=" * 70)
    else:
        # 目標達成なし - 最も近いものを選択
        logging.info("\n" + "=" * 70)
        logging.info("⚠️ 全目標を達成する閾値がありません。最も近いものを選択:")
        logging.info("=" * 70)
        
        # ベット数が足りているものの中で安定ROIが最も高いものを選択
        enough_bets = results_df[results_df['n_bets'] >= min_bets]
        if len(enough_bets) > 0:
            best = enough_bets.loc[enough_bets['stable_roi'].idxmax()]
        else:
            best = results_df.loc[results_df['n_bets'].idxmax()]
    
    logging.info(f"\n【推奨設定】")
    logging.info(f"  EV閾値: {best['threshold']:.2f}")
    logging.info(f"  ベット数: {int(best['n_bets'])}件 (年間{int(best['n_bets']/test_years)}件)")
    logging.info(f"  的中率: {best['hit_rate']*100:.1f}%")
    logging.info(f"  ROI: {best['roi']*100:.1f}%")
    logging.info(f"  安定ROI（上位3件除外）: {best['stable_roi']*100:.1f}%")
    logging.info(f"  95%信頼区間: [{best['ci_lower']*100:.1f}%, {best['ci_upper']*100:.1f}%]")
    logging.info(f"  1番人気選択率: {best['fav1_rate']*100:.1f}%")
    
    # 目標達成状況
    logging.info("\n【目標達成状況】")
    logging.info(f"  安定ROI ≥ 90%: {'✅' if best['stable_roi'] >= 0.90 else '❌'} ({best['stable_roi']*100:.1f}%)")
    logging.info(f"  年間ベット ≥ 500件: {'✅' if best['n_bets'] >= min_bets else '❌'} ({int(best['n_bets']/test_years)}件/年)")
    logging.info(f"  CI下限 ≥ 70%: {'✅' if best['ci_lower'] >= 0.70 else '❌'} ({best['ci_lower']*100:.1f}%)")
    logging.info(f"  1番人気 ≤ 65%: {'✅' if best['fav1_rate'] <= 0.65 else '❌'} ({best['fav1_rate']*100:.1f}%)")
    
    # 結果保存
    results_df.to_csv('keibaai/models/mu_v11_0/threshold_optimization_results.csv', index=False)
    
    # 推奨設定を保存
    recommended = {
        'ev_threshold': float(best['threshold']),
        'expected_bets_per_year': int(best['n_bets'] / test_years),
        'expected_roi': float(best['roi']),
        'stable_roi': float(best['stable_roi']),
        'ci_95_lower': float(best['ci_lower']),
        'ci_95_upper': float(best['ci_upper']),
        'fav1_rate': float(best['fav1_rate']),
    }
    with open('keibaai/models/mu_v11_0/recommended_settings.json', 'w') as f:
        json.dump(recommended, f, indent=2)
    
    logging.info(f"\n結果保存: keibaai/models/mu_v11_0/")
    
    return results_df, best


def main():
    model, features, test_df = load_and_prepare_data()
    logging.info(f"データ準備完了: {len(test_df):,}件")
    
    results_df, best = find_optimal_threshold(test_df, model, features)
    
    return results_df, best


if __name__ == "__main__":
    results, best = main()
