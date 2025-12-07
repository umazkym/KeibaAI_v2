"""
EV閾値結果の統計的検証

目的: 165.88% ROI (119件) が外れ値かどうかを検証
1. 的中分布の確認（高配当の外れ値チェック）
2. 月別安定性の確認
3. ブートストラップによる信頼区間
4. ランダムベット比較
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


def get_ev_filtered_bets(df, model, features, ev_threshold=0.10):
    """EV閾値適用後のベットを取得"""
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
    
    return bets


def analyze_win_distribution(bets):
    """的中分布の分析"""
    print("=" * 60)
    print("【検証1】的中分布の分析")
    print("=" * 60)
    
    hits = bets[bets['finish_position'] == 1].copy()
    misses = bets[bets['finish_position'] != 1].copy()
    
    print(f"\n総ベット数: {len(bets)}")
    print(f"的中数: {len(hits)}")
    print(f"的中率: {len(hits)/len(bets)*100:.1f}%")
    print(f"総回収: {hits['win_odds'].sum():.1f}倍")
    print(f"ROI: {hits['win_odds'].sum()/len(bets)*100:.1f}%")
    
    if len(hits) > 0:
        print(f"\n【的中配当の詳細】")
        print(f"最小: {hits['win_odds'].min():.1f}倍")
        print(f"最大: {hits['win_odds'].max():.1f}倍")
        print(f"平均: {hits['win_odds'].mean():.1f}倍")
        print(f"中央値: {hits['win_odds'].median():.1f}倍")
        
        print(f"\n的中配当一覧（降順）:")
        for i, (_, row) in enumerate(hits.sort_values('win_odds', ascending=False).iterrows(), 1):
            print(f"  {i}. {row['win_odds']:.1f}倍 ({int(row['popularity'])}番人気)")
        
        # 上位の的中が全体に与える影響
        top_hit = hits['win_odds'].max()
        roi_without_top = (hits['win_odds'].sum() - top_hit) / (len(bets) - 1) if len(bets) > 1 else 0
        print(f"\n【外れ値の影響】")
        print(f"最高配当 {top_hit:.1f}倍 を除いた場合:")
        print(f"  ROI: {roi_without_top*100:.1f}% ({len(bets)-1}件)")
        
        # 上位2件を除いた場合
        if len(hits) >= 2:
            top2 = hits.nlargest(2, 'win_odds')['win_odds'].sum()
            roi_without_top2 = (hits['win_odds'].sum() - top2) / (len(bets) - 2)
            print(f"上位2件を除いた場合:")
            print(f"  ROI: {roi_without_top2*100:.1f}% ({len(bets)-2}件)")


def analyze_monthly_stability(bets):
    """月別安定性の分析"""
    print("\n" + "=" * 60)
    print("【検証2】月別安定性")
    print("=" * 60)
    
    bets = bets.copy()
    bets['month'] = bets['race_date'].dt.to_period('M')
    
    print(f"\n{'月':>10} {'ベット数':>8} {'的中数':>8} {'ROI':>8}")
    print("-" * 40)
    
    monthly_roi = []
    for month in sorted(bets['month'].unique()):
        m_bets = bets[bets['month'] == month]
        m_hits = m_bets[m_bets['finish_position'] == 1]
        if len(m_bets) > 0:
            roi = m_hits['win_odds'].sum() / len(m_bets)
            monthly_roi.append(roi)
            print(f"{str(month):>10} {len(m_bets):>8} {len(m_hits):>8} {roi*100:>7.1f}%")
    
    if len(monthly_roi) > 1:
        print(f"\n月別ROIの統計:")
        print(f"  平均: {np.mean(monthly_roi)*100:.1f}%")
        print(f"  標準偏差: {np.std(monthly_roi)*100:.1f}%")
        print(f"  最小: {np.min(monthly_roi)*100:.1f}%")
        print(f"  最大: {np.max(monthly_roi)*100:.1f}%")
        
        # 100%を超えた月の数
        over_100 = sum(1 for r in monthly_roi if r >= 1.0)
        print(f"  ROI 100%超の月: {over_100}/{len(monthly_roi)}")


def bootstrap_confidence_interval(bets, n_bootstrap=10000):
    """ブートストラップによる信頼区間"""
    print("\n" + "=" * 60)
    print("【検証3】ブートストラップ信頼区間")
    print("=" * 60)
    
    np.random.seed(42)
    
    # 各ベットの結果（的中なら配当、外れなら0）
    outcomes = np.where(bets['finish_position'] == 1, bets['win_odds'].values, 0)
    n = len(outcomes)
    
    bootstrap_rois = []
    for _ in range(n_bootstrap):
        sample = np.random.choice(outcomes, size=n, replace=True)
        roi = sample.sum() / n
        bootstrap_rois.append(roi)
    
    bootstrap_rois = np.array(bootstrap_rois)
    
    print(f"\nブートストラップサンプル: {n_bootstrap}回")
    print(f"元のROI: {outcomes.sum()/n*100:.1f}%")
    print(f"ブートストラップ中央値: {np.median(bootstrap_rois)*100:.1f}%")
    print(f"95%信頼区間: [{np.percentile(bootstrap_rois, 2.5)*100:.1f}%, {np.percentile(bootstrap_rois, 97.5)*100:.1f}%]")
    print(f"90%信頼区間: [{np.percentile(bootstrap_rois, 5)*100:.1f}%, {np.percentile(bootstrap_rois, 95)*100:.1f}%]")
    
    # ROIが100%以下になる確率
    prob_under_100 = (bootstrap_rois < 1.0).sum() / n_bootstrap
    print(f"\nROI 100%未満になる確率: {prob_under_100*100:.1f}%")
    print(f"ROI 100%以上になる確率: {(1-prob_under_100)*100:.1f}%")
    
    return bootstrap_rois


def compare_with_random(test_df, n_simulations=1000):
    """ランダムベットとの比較"""
    print("\n" + "=" * 60)
    print("【検証4】ランダムベット比較")
    print("=" * 60)
    
    np.random.seed(42)
    
    # 同じ件数（119件）をランダムに選んだ場合
    n_bets = 119
    random_rois = []
    
    race_ids = test_df['race_id'].unique()
    
    for _ in range(n_simulations):
        # ランダムにレースを選択
        selected_races = np.random.choice(race_ids, size=n_bets, replace=False)
        random_bets = []
        
        for race_id in selected_races:
            race = test_df[test_df['race_id'] == race_id]
            if len(race) > 0:
                # ランダムに1頭選択
                selected = race.sample(1).iloc[0]
                random_bets.append(selected)
        
        if len(random_bets) > 0:
            random_df = pd.DataFrame(random_bets)
            hits = random_df[random_df['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(random_df)
            random_rois.append(roi)
    
    random_rois = np.array(random_rois)
    
    print(f"\nランダムベット ({n_simulations}回シミュレーション):")
    print(f"  平均ROI: {np.mean(random_rois)*100:.1f}%")
    print(f"  標準偏差: {np.std(random_rois)*100:.1f}%")
    print(f"  95%範囲: [{np.percentile(random_rois, 2.5)*100:.1f}%, {np.percentile(random_rois, 97.5)*100:.1f}%]")
    
    # EV閾値ベットがランダムより優れている確率
    ev_roi = 1.6588  # 165.88%
    prob_random_higher = (random_rois >= ev_roi).sum() / n_simulations
    print(f"\nランダムでROI {ev_roi*100:.1f}%以上になる確率: {prob_random_higher*100:.2f}%")
    
    return random_rois


def main():
    print("=" * 60)
    print("EV閾値結果の統計的検証")
    print("=" * 60)
    
    model, features, test_df = load_and_prepare_data()
    bets = get_ev_filtered_bets(test_df, model, features, ev_threshold=0.10)
    
    logging.info(f"Test期間: {test_df['race_date'].min()} ~ {test_df['race_date'].max()}")
    logging.info(f"EV閾値0.10で選択されたベット: {len(bets)}件")
    
    # 各検証
    analyze_win_distribution(bets)
    analyze_monthly_stability(bets)
    bootstrap_rois = bootstrap_confidence_interval(bets)
    random_rois = compare_with_random(test_df)
    
    print("\n" + "=" * 60)
    print("【総合判定】")
    print("=" * 60)
    
    # 判定
    original_roi = bets[bets['finish_position'] == 1]['win_odds'].sum() / len(bets)
    ci_lower = np.percentile(bootstrap_rois, 5)
    
    if ci_lower >= 1.0:
        print("✅ 90%信頼区間の下限が100%を超えている → 統計的に有意")
    elif ci_lower >= 0.8:
        print("△ 90%信頼区間の下限が80-100% → やや不確実だが期待できる")
    else:
        print("⚠️ 90%信頼区間の下限が80%未満 → 外れ値の影響が大きい可能性")


if __name__ == "__main__":
    main()
