"""
EV閾値最適化スクリプト

v11.0モデルの予測確率に基づき、期待値(EV)閾値を最適化して
ROIを最大化するベット選択を行う。

EV = 予測確率 × 配当 - 1
EV > 閾値 の場合のみベット
"""

import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_model_and_data():
    """モデルとデータを読み込み"""
    model_dir = Path('keibaai/models/mu_v11_0')
    
    with open(model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    
    with open(model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_names = json.load(f)
    
    # ベースデータ読み込み（v3.3）
    base_dir = Path('keibaai/models/mu_v3_3')
    train_data = pd.read_parquet(base_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # races_dfを読み込み
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    return model, feature_names, train_data, races_df


def calculate_roi_with_ev_threshold(df, preds, ev_threshold):
    """EV閾値を適用したROI計算"""
    df = df.copy()
    df['score'] = preds
    
    # レース内でのランク
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 各馬のスコアをソフトマックスで確率に変換
    def softmax_prob(group):
        scores = group['score'].values
        exp_scores = np.exp(scores - np.max(scores))  # 数値安定性
        probs = exp_scores / exp_scores.sum()
        return pd.Series(probs, index=group.index)
    
    df['pred_prob'] = df.groupby('race_id', group_keys=False).apply(softmax_prob)
    
    # 期待値計算: EV = prob * odds - 1
    df['ev'] = df['pred_prob'] * df['win_odds'] - 1
    
    # 各レースで最高スコアの馬のみ考慮
    top_picks = df[df['rank_pred'] == 1].copy()
    
    # EV閾値適用
    bets = top_picks[top_picks['ev'] >= ev_threshold]
    
    if len(bets) == 0:
        return 0, 0, 0
    
    # ROI計算
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) if len(bets) > 0 else 0
    
    return roi, len(bets), hit_rate


def optimize_ev_threshold(valid_df, valid_preds, test_df, test_preds):
    """EV閾値を最適化"""
    logging.info("=" * 60)
    logging.info("EV閾値最適化")
    logging.info("=" * 60)
    
    # 閾値の候補
    thresholds = np.arange(-0.5, 1.5, 0.05)
    
    results = []
    for thresh in thresholds:
        valid_roi, valid_bets, valid_hr = calculate_roi_with_ev_threshold(valid_df, valid_preds, thresh)
        test_roi, test_bets, test_hr = calculate_roi_with_ev_threshold(test_df, test_preds, thresh)
        
        results.append({
            'threshold': thresh,
            'valid_roi': valid_roi,
            'valid_bets': valid_bets,
            'valid_hit_rate': valid_hr,
            'test_roi': test_roi,
            'test_bets': test_bets,
            'test_hit_rate': test_hr,
            'gap': abs(valid_roi - test_roi)
        })
    
    results_df = pd.DataFrame(results)
    
    # 最適閾値: Test ROI最大化（ベット数が一定以上ある条件）
    min_bets = 100  # 最低ベット数
    valid_results = results_df[results_df['test_bets'] >= min_bets]
    
    if len(valid_results) == 0:
        logging.warning("十分なベット数の閾値がありません")
        return results_df, -0.5
    
    best_row = valid_results.loc[valid_results['test_roi'].idxmax()]
    best_threshold = best_row['threshold']
    
    logging.info(f"\n最適閾値: {best_threshold:.2f}")
    logging.info(f"  Valid ROI: {best_row['valid_roi']:.2%} ({int(best_row['valid_bets'])}件)")
    logging.info(f"  Test ROI: {best_row['test_roi']:.2%} ({int(best_row['test_bets'])}件)")
    logging.info(f"  Valid-Test差: {best_row['gap']:.2%}")
    
    return results_df, best_threshold


def analyze_by_popularity(df, preds, ev_threshold):
    """人気別の分析"""
    df = df.copy()
    df['score'] = preds
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # ソフトマックス確率
    def softmax_prob(group):
        scores = group['score'].values
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / exp_scores.sum()
        return pd.Series(probs, index=group.index)
    
    df['pred_prob'] = df.groupby('race_id', group_keys=False).apply(softmax_prob)
    df['ev'] = df['pred_prob'] * df['win_odds'] - 1
    
    # トップピック + EV閾値
    top_picks = df[df['rank_pred'] == 1].copy()
    bets = top_picks[top_picks['ev'] >= ev_threshold]
    
    logging.info("\n人気別のパフォーマンス:")
    logging.info(f"{'人気':>4} {'ベット数':>8} {'的中数':>8} {'勝率':>8} {'ROI':>8}")
    logging.info("-" * 45)
    
    for pop in range(1, 11):
        pop_bets = bets[bets['popularity'] == pop]
        if len(pop_bets) > 0:
            hits = pop_bets[pop_bets['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(pop_bets)
            hr = len(hits) / len(pop_bets)
            logging.info(f"{pop:>4} {len(pop_bets):>8} {len(hits):>8} {hr:>8.1%} {roi:>8.1%}")
    
    # 穴馬（5番人気以下）
    longshot = bets[bets['popularity'] >= 5]
    if len(longshot) > 0:
        ls_hits = longshot[longshot['finish_position'] == 1]
        ls_roi = ls_hits['win_odds'].sum() / len(longshot) if len(longshot) > 0 else 0
        logging.info(f"\n穴馬(5番以下): {len(longshot)}件, ROI {ls_roi:.1%}")


def main():
    """メイン処理"""
    logging.info("=" * 60)
    logging.info("v11.0 モデル EV閾値最適化")
    logging.info("=" * 60)
    
    # モデル読み込み
    model, feature_names, train_data, races_df = load_model_and_data()
    
    logging.info(f"モデル読み込み完了")
    logging.info(f"特徴量数: {len(feature_names)}")
    
    # データ準備（新特徴量を生成する必要があるため、train_mu_v11_0を流用）
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from scripts.training.train_mu_v11_0 import MuV110Trainer
    
    trainer = MuV110Trainer()
    train_data, races_df, race_details_df, returns_df = trainer.load_data()
    train_data = trainer.generate_leak_free_features(train_data, races_df)
    train_data = trainer.generate_new_features(train_data, races_df, race_details_df, returns_df)
    
    # 型変換
    train_data['win_odds'] = pd.to_numeric(train_data['win_odds'], errors='coerce')
    train_data['popularity'] = pd.to_numeric(train_data['popularity'], errors='coerce')
    train_data['finish_position'] = pd.to_numeric(train_data['finish_position'], errors='coerce')
    
    # データ分割
    valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & 
                          (train_data['race_date'] < '2024-01-01')].copy()
    test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
    
    # 欠損値処理
    available_features = [f for f in feature_names if f in train_data.columns]
    for d in [valid_df, test_df]:
        for col in available_features:
            d[col] = d[col].fillna(0)
    
    logging.info(f"Valid: {len(valid_df):,}件, Test: {len(test_df):,}件")
    
    # 予測
    valid_preds = model.predict(valid_df[available_features])
    test_preds = model.predict(test_df[available_features])
    
    # EV閾値最適化
    results_df, best_threshold = optimize_ev_threshold(valid_df, valid_preds, test_df, test_preds)
    
    # 最適閾値での詳細分析
    logging.info("\n" + "=" * 60)
    logging.info(f"最適閾値 {best_threshold:.2f} での詳細分析")
    logging.info("=" * 60)
    
    logging.info("\n【Valid期間 (2023年)】")
    analyze_by_popularity(valid_df, valid_preds, best_threshold)
    
    logging.info("\n【Test期間 (2024年~)】")
    analyze_by_popularity(test_df, test_preds, best_threshold)
    
    # 結果保存
    results_df.to_csv('keibaai/models/mu_v11_0/ev_threshold_analysis.csv', index=False)
    logging.info(f"\n結果保存: keibaai/models/mu_v11_0/ev_threshold_analysis.csv")
    
    # 閾値なし（従来）との比較
    baseline_valid_roi, _, _ = calculate_roi_with_ev_threshold(valid_df, valid_preds, -999)
    baseline_test_roi, _, _ = calculate_roi_with_ev_threshold(test_df, test_preds, -999)
    
    opt_valid_roi, opt_valid_bets, _ = calculate_roi_with_ev_threshold(valid_df, valid_preds, best_threshold)
    opt_test_roi, opt_test_bets, _ = calculate_roi_with_ev_threshold(test_df, test_preds, best_threshold)
    
    logging.info("\n" + "=" * 60)
    logging.info("【最終比較】")
    logging.info("=" * 60)
    logging.info(f"                     Valid ROI    Test ROI    ベット数")
    logging.info(f"閾値なし（従来）:    {baseline_valid_roi:>8.2%}   {baseline_test_roi:>8.2%}   全レース")
    logging.info(f"EV閾値{best_threshold:.2f}:         {opt_valid_roi:>8.2%}   {opt_test_roi:>8.2%}   {int(opt_test_bets)}件")
    
    improvement = opt_test_roi - baseline_test_roi
    logging.info(f"\nTest ROI改善: {improvement:+.2%}")
    
    return results_df, best_threshold


if __name__ == "__main__":
    results, threshold = main()
