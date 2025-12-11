# -*- coding: utf-8 -*-
"""
Phase 4: EV閾値戦略

18_improvement_roadmap.md Phase 4より:
- EV (期待値) = 予測確率 × オッズ × 払戻率(0.80)
- EV > 閾値 の馬のみにベット

【実験設計】
1. V15モデルで予測確率を取得
2. EV計算: predict_prob * win_odds * 0.80
3. 様々な閾値でROI計算: 1.0, 1.1, 1.2, 1.3

【リスク】
- 購入数減少 → 収束性低下（ばらつき増加）
- 高すぎる閾値 → ベット数ゼロ
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
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def calc_ev(df, preds, odds_col='win_odds', payout_rate=0.80):
    """
    期待値計算
    EV = 予測確率 × オッズ × 払戻率
    """
    df = df.copy()
    df['pred_prob'] = preds
    df['expected_value'] = df['pred_prob'] * df[odds_col] * payout_rate
    return df


def calc_roi_with_ev_threshold(df, threshold, use_pred_rank=True):
    """
    EV閾値でフィルタしてROI計算
    
    use_pred_rank=True: 予測確率Top1馬を選択し、そのEVが閾値を超えるかチェック
    use_pred_rank=False: 最高EV馬を選択
    """
    df = df.copy()
    
    if use_pred_rank:
        # 予測確率でランク付け（V15と同じ方式）
        df['pred_rank'] = df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        top1 = df[df['pred_rank'] == 1]
    else:
        # EVでランク付け
        df['ev_rank'] = df.groupby('race_id')['expected_value'].rank(ascending=False, method='first')
        top1 = df[df['ev_rank'] == 1]
    
    # 閾値フィルタ（Top1馬のEVが閾値を超えるか）
    bets = top1[top1['expected_value'] > threshold]
    
    if len(bets) == 0:
        return {
            'threshold': threshold,
            'n_races': len(df['race_id'].unique()),
            'n_bets': 0,
            'bet_rate': 0,
            'hit_rate': 0,
            'roi': 0,
            'avg_ev': 0,
            'avg_odds': 0,
        }
    
    # 的中
    hits = bets[bets['finish_position'] == 1]
    
    # ROI
    total_bets = len(bets) * 100
    total_payout = hits['win_odds'].sum() * 100
    roi = total_payout / total_bets * 100
    
    return {
        'threshold': threshold,
        'n_races': len(df['race_id'].unique()),
        'n_bets': len(bets),
        'bet_rate': len(bets) / len(df['race_id'].unique()) * 100,
        'hit_rate': len(hits) / len(bets) * 100,
        'roi': roi,
        'avg_ev': bets['expected_value'].mean(),
        'avg_odds': bets['win_odds'].mean(),
    }


def main():
    logger.info("=" * 60)
    logger.info("Phase 4: EV閾値戦略")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    logger.info(f"  Testレース数: {test['race_id'].nunique():,}")
    
    # 特徴量生成
    logger.info("")
    logger.info("特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # モデル学習
    logger.info("")
    logger.info("V15モデル学習中...")
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # 予測
    test_f['pred_prob'] = model.predict(X_test)
    
    # EV計算
    test_f['expected_value'] = test_f['pred_prob'] * test_f['win_odds'] * 0.80
    
    # EV分布の確認
    logger.info("")
    logger.info("=" * 60)
    logger.info("EV分布")
    logger.info("=" * 60)
    
    logger.info(f"  EV平均: {test_f['expected_value'].mean():.3f}")
    logger.info(f"  EV中央値: {test_f['expected_value'].median():.3f}")
    logger.info(f"  EV標準偏差: {test_f['expected_value'].std():.3f}")
    logger.info(f"  EV > 1.0 割合: {(test_f['expected_value'] > 1.0).mean()*100:.1f}%")
    logger.info(f"  EV > 1.1 割合: {(test_f['expected_value'] > 1.1).mean()*100:.1f}%")
    logger.info(f"  EV > 1.2 割合: {(test_f['expected_value'] > 1.2).mean()*100:.1f}%")
    
    # ベースライン (閾値なし = Top1選択)
    logger.info("")
    logger.info("=" * 60)
    logger.info("ベースライン (Top1選択、閾値なし)")
    logger.info("=" * 60)
    
    baseline = calc_roi_with_ev_threshold(test_f, threshold=0)
    logger.info(f"  ベット数: {baseline['n_bets']:,}/{baseline['n_races']:,}レース ({baseline['bet_rate']:.1f}%)")
    logger.info(f"  的中率: {baseline['hit_rate']:.1f}%")
    logger.info(f"  ROI: {baseline['roi']:.1f}%")
    
    # EV閾値実験
    thresholds = [0.8, 0.9, 1.0, 1.1, 1.2, 1.3]
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("EV閾値別ROI")
    logger.info("=" * 60)
    
    results = []
    for th in thresholds:
        result = calc_roi_with_ev_threshold(test_f, threshold=th)
        results.append(result)
        
        if result['n_bets'] > 0:
            logger.info(f"  EV > {th:.1f}: ベット数={result['n_bets']:,}件 ({result['bet_rate']:.1f}%), 的中率={result['hit_rate']:.1f}%, ROI={result['roi']:.1f}%")
        else:
            logger.info(f"  EV > {th:.1f}: ベット数=0 (閾値高すぎ)")
    
    # 結果サマリー
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果サマリー")
    logger.info("=" * 60)
    
    logger.info(f"  {'閾値':>6} {'ベット数':>10} {'ベット率':>8} {'的中率':>8} {'ROI':>8}")
    logger.info("-" * 50)
    
    best_roi = 0
    best_threshold = None
    
    for r in results:
        if r['roi'] > best_roi and r['n_bets'] >= 100:  # 最低100ベット必要
            best_roi = r['roi']
            best_threshold = r['threshold']
        
        logger.info(f"  {r['threshold']:>6.1f} {r['n_bets']:>10,} {r['bet_rate']:>7.1f}% {r['hit_rate']:>7.1f}% {r['roi']:>7.1f}%")
    
    if best_threshold:
        logger.info("")
        logger.info(f"  推奨閾値: EV > {best_threshold:.1f} (ROI={best_roi:.1f}%)")
    
    # 月別安定性確認
    logger.info("")
    logger.info("=" * 60)
    logger.info("月別ROI安定性 (EV > 1.0)")
    logger.info("=" * 60)
    
    test_f['month'] = test_f['race_date'].dt.to_period('M')
    
    for month in sorted(test_f['month'].unique()):
        month_data = test_f[test_f['month'] == month]
        result = calc_roi_with_ev_threshold(month_data, threshold=1.0)
        if result['n_bets'] > 0:
            logger.info(f"  {month}: ベット数={result['n_bets']:,}, ROI={result['roi']:.1f}%")
    
    return results


if __name__ == "__main__":
    main()
