# -*- coding: utf-8 -*-
"""
Novel ML Approaches - V15超えを目指す新アプローチ検証

【3つのアプローチ】
1. TabNet Neural Network
2. Multi-task Learning (1着・2着・3着同時予測)
3. Odds予測モデル (市場過小評価馬の直接予測)

【リーク防止】
- Train期間: ~2024-12-31
- Test期間: 2025-01-01~
- オッズ・着順は特徴量に使用しない
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


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def main():
    logger.info("=" * 70)
    logger.info("Novel ML Approaches - V15超えを目指す")
    logger.info("=" * 70)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # 特徴量
    logger.info("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  特徴量数: {len(feature_cols)}")
    
    X_train = train_f[feature_cols].fillna(0).values
    X_test = test_f[feature_cols].fillna(0).values
    y_train = (train_f['finish_position'] == 1).astype(int).values
    
    # ========== Baseline V15 ==========
    logger.info("\n" + "=" * 70)
    logger.info("Baseline: V15 LightGBM")
    logger.info("=" * 70)
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    model_v15 = lgb.train(params, lgb.Dataset(X_train, y_train), num_boost_round=200)
    pred_v15 = model_v15.predict(X_test)
    
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_v15)
    logger.info(f"  Test ROI: {test_roi_v15:.1f}%, Hit: {test_hit_v15:.1f}%")
    
    # ========== Approach 1: TabNet ==========
    logger.info("\n" + "=" * 70)
    logger.info("Approach 1: TabNet Neural Network")
    logger.info("=" * 70)
    
    try:
        from pytorch_tabnet.tab_model import TabNetClassifier
        
        # TabNet用にデータ準備
        X_train_np = X_train.astype(np.float32)
        X_test_np = X_test.astype(np.float32)
        y_train_np = y_train.astype(np.int64)
        
        # 強い正則化でTabNetを学習
        tabnet = TabNetClassifier(
            n_d=8, n_a=8,  # 小さめの次元
            n_steps=3,
            gamma=1.5,
            lambda_sparse=1e-3,
            optimizer_params={'lr': 0.02},
            scheduler_params={'step_size': 10, 'gamma': 0.9},
            verbose=0
        )
        
        tabnet.fit(
            X_train_np, y_train_np,
            max_epochs=50,
            patience=10,
            batch_size=1024
        )
        
        pred_tabnet = tabnet.predict_proba(X_test_np)[:, 1]
        test_roi_tabnet, test_hit_tabnet = calc_roi(test_f, pred_tabnet)
        
        logger.info(f"  Test ROI: {test_roi_tabnet:.1f}%, Hit: {test_hit_tabnet:.1f}%")
        logger.info(f"  改善: {test_roi_tabnet - test_roi_v15:+.1f}%")
        
    except Exception as e:
        logger.info(f"  TabNet失敗: {e}")
        test_roi_tabnet = 0
        test_hit_tabnet = 0
    
    # ========== Approach 2: Multi-task Learning ==========
    logger.info("\n" + "=" * 70)
    logger.info("Approach 2: Multi-task Learning (1着・2着・3着)")
    logger.info("=" * 70)
    
    # 各着順のラベル作成
    y_train_1st = (train_f['finish_position'] == 1).astype(int)
    y_train_2nd = (train_f['finish_position'] == 2).astype(int)
    y_train_3rd = (train_f['finish_position'] == 3).astype(int)
    
    # 各着順のモデルを学習
    model_1st = lgb.train(params, lgb.Dataset(X_train, y_train_1st), num_boost_round=200)
    model_2nd = lgb.train(params, lgb.Dataset(X_train, y_train_2nd), num_boost_round=200)
    model_3rd = lgb.train(params, lgb.Dataset(X_train, y_train_3rd), num_boost_round=200)
    
    pred_1st = model_1st.predict(X_test)
    pred_2nd = model_2nd.predict(X_test)
    pred_3rd = model_3rd.predict(X_test)
    
    # 1着予測でROI計算
    test_roi_mt1, test_hit_mt1 = calc_roi(test_f, pred_1st)
    logger.info(f"  1着Model Test ROI: {test_roi_mt1:.1f}%, Hit: {test_hit_mt1:.1f}%")
    
    # 1+2+3着の合成スコア
    pred_top3 = pred_1st + pred_2nd * 0.5 + pred_3rd * 0.25
    test_roi_mtc, test_hit_mtc = calc_roi(test_f, pred_top3)
    logger.info(f"  Combined Test ROI: {test_roi_mtc:.1f}%, Hit: {test_hit_mtc:.1f}%")
    logger.info(f"  改善: {test_roi_mtc - test_roi_v15:+.1f}%")
    
    # ========== Approach 3: Odds予測モデル ==========
    logger.info("\n" + "=" * 70)
    logger.info("Approach 3: Odds予測 (市場過小評価馬)")
    logger.info("=" * 70)
    
    # 「オッズ対比での価値」をターゲットにする
    # target = (1着なら1, それ以外は0) / log(オッズ) のような指標
    # ただし、オッズを直接使うとリークになるので、
    # 「勝った場合の価値」= finish_position==1 として、
    # モデルに「穴馬を重視」させる工夫をする
    
    # 穴馬重視: sample_weight = log1p(win_odds) で学習
    # （前回効果なしだったが、別のアプローチを試す）
    
    # EV基準: 1着馬のオッズが高いほど高いターゲット
    # → これはリークになるので使えない
    
    # 代替アプローチ: 人気との乖離予測
    # 実際の着順と人気の差を予測する
    train_f['popularity_gap'] = train_f['finish_position'] - train_f['popularity']
    # 1着馬で人気より上の順位なら正の価値
    y_train_value = ((train_f['finish_position'] == 1) & (train_f['popularity'] > 3)).astype(int)
    
    model_value = lgb.train(params, lgb.Dataset(X_train, y_train_value), num_boost_round=200)
    pred_value = model_value.predict(X_test)
    
    test_roi_value, test_hit_value = calc_roi(test_f, pred_value)
    logger.info(f"  穴馬1着予測 Test ROI: {test_roi_value:.1f}%, Hit: {test_hit_value:.1f}%")
    logger.info(f"  改善: {test_roi_value - test_roi_v15:+.1f}%")
    
    # V15との組み合わせ
    pred_combined = pred_v15 * 0.7 + pred_value * 0.3
    test_roi_comb, test_hit_comb = calc_roi(test_f, pred_combined)
    logger.info(f"  V15+穴馬 Combined ROI: {test_roi_comb:.1f}%, Hit: {test_hit_comb:.1f}%")
    
    # ========== 結果比較 ==========
    logger.info("\n" + "=" * 70)
    logger.info("結果比較")
    logger.info("=" * 70)
    
    results = [
        ('V15 Baseline', test_roi_v15, test_hit_v15),
        ('TabNet', test_roi_tabnet, test_hit_tabnet),
        ('Multi-task (1着)', test_roi_mt1, test_hit_mt1),
        ('Multi-task (Combined)', test_roi_mtc, test_hit_mtc),
        ('穴馬1着予測', test_roi_value, test_hit_value),
        ('V15+穴馬 Combined', test_roi_comb, test_hit_comb),
    ]
    
    logger.info(f"\n{'Method':<25} {'Test ROI':>10} {'改善幅':>10} {'的中率':>10}")
    logger.info("-" * 55)
    for name, roi, hit in results:
        if roi > 0:
            diff = roi - test_roi_v15
            logger.info(f"{name:<25} {roi:>9.1f}% {diff:>+9.1f}% {hit:>9.1f}%")
    
    best = max(results, key=lambda x: x[1])
    logger.info(f"\nBest: {best[0]} (ROI={best[1]:.1f}%)")
    
    if best[1] > test_roi_v15:
        logger.info(f"✅ V15を{best[1] - test_roi_v15:.1f}%上回りました！")
    else:
        logger.info(f"❌ V15を超えられませんでした")
    
    return {r[0]: r[1] for r in results}


if __name__ == "__main__":
    main()
