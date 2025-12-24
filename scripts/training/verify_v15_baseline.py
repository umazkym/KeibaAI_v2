# -*- coding: utf-8 -*-
"""
V15相当の正しい期間設定でのベースライン検証

【期間設定】
- Train: 2020-01-01 ~ 2024-01-01（学習・特徴量fit）
- Test: 2024-01-01 ~ （評価のみ）

これがV15公式値91.8%の再現を目指す
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    # ドキュメント記載のデータ期間に合わせる (2020-01-05 〜 2025-10-26)
    races_df = races_df[
        (races_df['race_date'] >= '2020-01-01') & 
        (races_df['race_date'] <= '2025-10-26')  # ★ ドキュメント記載の終了日
    ].dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def create_relevance_label(finish_pos):
    """関連度ラベル"""
    if finish_pos == 1: return 5
    elif finish_pos == 2: return 4
    elif finish_pos == 3: return 3
    elif finish_pos <= 5: return 1
    else: return 0


def calculate_roi(df, pred_col='score'):
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    
    hits = top1[top1['finish_position'] == 1]
    
    total_bets = len(top1) * 100
    total_payout = hits['win_odds'].sum() * 100
    
    hit_rate = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def calculate_place_roi(df, pred_col, returns_df):
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1][['race_id', 'horse_number', 'finish_position']].copy()
    top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    
    total_bets = len(top1) * 100
    total_payout = top1['payout'].fillna(0).sum()
    
    hit_rate = top1['payout'].notna().sum() / len(top1) * 100 if len(top1) > 0 else 0
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    
    return hit_rate, roi


def main():
    logger.info("=" * 60)
    logger.info("V15ベースライン再現テスト（正しい期間設定）")
    logger.info("=" * 60)
    
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    # 時系列分割 (2025-01-01で分割)
    # v15レポートによると、Test期間は2025-01-01〜
    train_end_date = '2025-01-01'
    test_start_date = '2025-01-01'
    
    train_df = races_df[races_df['race_date'] < train_end_date].copy()
    test_df = races_df[races_df['race_date'] >= test_start_date].copy()
    
    logger.info(f"  Train: {len(train_df):,}件 (~{train_end_date})")
    logger.info(f"  Test:  {len(test_df):,}件 ({test_start_date}~)")
    logger.info(f"  Trainレース数: {train_df['race_id'].nunique():,}")
    logger.info(f"  Testレース数: {test_df['race_id'].nunique():,}")
    
    # 特徴量生成（Train全体でfit）
    logger.info("")
    logger.info("特徴量生成中...")
    fe = LeakFreeFeatureEngineerV15()
    fe.fit(train_df, pedigrees_df, corners_df, race_details_df, returns_df)
    
    train_features = fe.transform(train_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    # ★ Binary Classification用ターゲット (V15公式: objective='binary')
    train_features['is_win'] = (train_features['finish_position'] == 1).astype(int)
    test_features['is_win'] = (test_features['finish_position'] == 1).astype(int)
    
    # ソート
    train_sorted = train_features.sort_values('race_id')
    test_sorted = test_features.sort_values('race_id')
    
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['is_win']  # ★ Binary Classification: 0 or 1
    X_test = test_sorted[feature_cols].fillna(0)
    y_test = test_sorted['is_win']
    
    # Binary Classification（V15公式設定）
    logger.info("")
    logger.info("=" * 60)
    logger.info("Binary Classification 単勝モデル（V15公式）")
    logger.info("=" * 60)
    
    # V15公式ハイパーパラメータ (from 23_包括的検証レポート_v3.md)
    lgbm_params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,      # 0.05 -> 0.03
        'num_leaves': 20,           # 31 -> 20
        'max_depth': 3,             # -1 -> 3 (重要: 浅い木で過学習を防ぐ)
        'min_child_samples': 100,   # 20 -> 100
        'reg_alpha': 3.0,           # 0.0 -> 3.0
        'reg_lambda': 5.0,          # 0.0 -> 5.0
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
        'random_state': 42
    }
    num_boost_round = 200 # レポート記載の値
    
    # ★ Binary Classification: groupパラメータは不要
    train_ds = lgb.Dataset(X_train, y_train)
    test_ds = lgb.Dataset(X_test, y_test)
    
    model = lgb.train(
        lgbm_params,
        train_ds,
        num_boost_round=num_boost_round,  # ★ ドキュメント通りフル200ラウンド (Early Stoppingなし)
        valid_sets=[test_ds],
        callbacks=[
            lgb.log_evaluation(period=50)  # ログ用 (Early Stoppingは削除)
        ]
    )
    train_sorted['score'] = model.predict(X_train)
    test_sorted['score'] = model.predict(X_test)
    
    train_hit, train_roi = calculate_roi(train_sorted, 'score')
    test_hit, test_roi = calculate_roi(test_sorted, 'score')
    
    logger.info(f"  Train: 的中率={train_hit:.1f}%, ROI={train_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_hit:.1f}%, ROI={test_roi:.1f}%")
    logger.info(f"  Train-Test Gap: {train_roi - test_roi:.1f}%")
    
    # 複勝ROIも計算
    logger.info("")
    logger.info("=" * 60)
    logger.info("複勝ROI（同じモデルで評価）")
    logger.info("=" * 60)
    
    train_place_hit, train_place_roi = calculate_place_roi(train_sorted, 'score', returns_df)
    test_place_hit, test_place_roi = calculate_place_roi(test_sorted, 'score', returns_df)
    
    logger.info(f"  Train: 的中率={train_place_hit:.1f}%, ROI={train_place_roi:.1f}%")
    logger.info(f"  Test:  的中率={test_place_hit:.1f}%, ROI={test_place_roi:.1f}%")
    
    # 結果サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    logger.info(f"  【単勝】Test ROI: {test_roi:.1f}% (目標: 91.8%)")
    logger.info(f"  【複勝】Test ROI: {test_place_roi:.1f}%")
    
    target = 91.8
    if test_roi >= target:
        logger.info(f"  ✅ V15ベースライン再現成功")
    else:
        logger.info(f"  ❌ 差分: {target - test_roi:.1f}%")

    logger.info("")
    logger.info("=" * 60)
    logger.info("詳細ブレークダウン分析")
    logger.info("=" * 60)

    # 1. トラック条件別
    logger.info("【トラック種別 ROI】")
    # Noneを除外し、文字列に変換
    surfaces = [s for s in test_sorted['track_surface'].unique() if s is not None]
    for surface in surfaces:
        subset = test_sorted[test_sorted['track_surface'] == surface]
        _, roi = calculate_roi(subset, 'score')
        count = len(subset.groupby('race_id'))
        logger.info(f"  {surface:5}: ROI={roi:6.1f}% (レース数={count})")

    # 2. クラス別
    logger.info("\n【クラス別 ROI】")
    class_map = {
        'Shogai': '障害', 'G1': 'G1', 'G2': 'G2', 'G3': 'G3', 'OP': 'オープン',
        '3Win': '3勝', '2Win': '2勝', '1Win': '1勝', 'Maiden': '未勝利', 'New': '新馬'
    }
    # 簡単なマッピングでグルーピング
    test_sorted['simple_class'] = test_sorted['race_class'].astype(str)
    for c in sorted(test_sorted['simple_class'].unique()):
        subset = test_sorted[test_sorted['simple_class'] == c]
        if len(subset) < 100: continue
        _, roi = calculate_roi(subset, 'score')
        count = len(subset['race_id'].unique())
        logger.info(f"  {c:10}: ROI={roi:6.1f}% (レース数={count})")

    # 3. オッズ帯別（Top1選出時）
    logger.info("\n【単勝オッズ帯別 ROI (Top1 Only)】")
    # Top1のみ抽出
    test_sorted['pred_rank'] = test_sorted.groupby('race_id')['score'].rank(ascending=False, method='first')
    top1 = test_sorted[test_sorted['pred_rank'] == 1].copy()
    
    odds_bins = [1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 999.0]
    labels = ['1-5', '5-10', '10-20', '20-50', '50-100', '100+']
    top1['odds_bin'] = pd.cut(top1['win_odds'], bins=odds_bins, labels=labels, right=False)
    
    for label in labels:
        subset = top1[top1['odds_bin'] == label]
        if len(subset) == 0: continue
        hits = subset[subset['finish_position'] == 1]
        bets = len(subset)
        ret = hits['win_odds'].sum()
        roi = ret / bets * 100 if bets > 0 else 0
        logger.info(f"  {label:7}: ROI={roi:6.1f}% (件数={bets}, 的中={len(hits)})")
    logger.info("")
    logger.info("Top 10 特徴量:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.1f}")
    
    return model, fe, test_roi


if __name__ == "__main__":
    main()
