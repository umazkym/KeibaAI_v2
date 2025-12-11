# -*- coding: utf-8 -*-
"""
Phase 3 #10: コーナー特徴量拡張（リークフリー版）

18_improvement_roadmap.md 6.2.2より:
- horse_position_std: コーナーポジション安定性（過去5走のstd）
- horse_front_tendency: 先行力スコア（過去平均ポジション）

【リークフリー実装】
- cumsum + shift(1) パターン
- 当該レース以前のデータのみ使用
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


def add_corner_features_leakfree(df, corners_df, races_full):
    """
    コーナー特徴量をリークフリーで追加
    
    【新規特徴量】
    1. horse_c4_position_std: 過去5走のC4ポジション標準偏差（安定性）
    2. horse_front_tendency_lf: 過去平均C4ポジション（リークフリー版）
    3. horse_position_improvement_trend: ポジション改善傾向
    """
    logger.info("コーナー特徴量を計算中...")
    
    # C4ポジションを取得
    c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_position']
    
    # レース情報をマージ
    race_info = races_full[['race_id', 'horse_id', 'horse_number', 'race_date']].drop_duplicates()
    c4 = c4.merge(race_info, on=['race_id', 'horse_number'], how='inner')
    
    # 馬×日付でソート
    c4 = c4.sort_values(['horse_id', 'race_date'])
    
    # === 1. horse_front_tendency_lf: 累積平均（リークフリー） ===
    c4['cumsum_pos'] = c4.groupby('horse_id')['c4_position'].cumsum()
    c4['cumcount'] = c4.groupby('horse_id').cumcount() + 1
    c4['cumavg_pos'] = c4['cumsum_pos'] / c4['cumcount']
    # shift(1)で当該レースを除外
    c4['horse_front_tendency_lf'] = c4.groupby('horse_id')['cumavg_pos'].shift(1)
    
    # === 2. horse_c4_position_std: 過去5走の標準偏差 ===
    # rolling(5).std() + shift(1)
    c4['rolling_std'] = c4.groupby('horse_id')['c4_position'].transform(
        lambda x: x.rolling(5, min_periods=2).std()
    )
    c4['horse_c4_position_std'] = c4.groupby('horse_id')['rolling_std'].shift(1)
    
    # === 3. horse_position_improvement_trend: ポジション改善傾向 ===
    # 過去3走でポジションが改善しているか（傾きを計算）
    def calc_trend(series):
        if len(series) < 2:
            return np.nan
        x = np.arange(len(series))
        slope = np.polyfit(x, series, 1)[0]
        return -slope  # マイナスほど後方に（悪化）、プラスほど前方に（改善）
    
    c4['trend'] = c4.groupby('horse_id')['c4_position'].transform(
        lambda x: x.rolling(3, min_periods=2).apply(calc_trend, raw=True)
    )
    c4['horse_position_improvement_trend'] = c4.groupby('horse_id')['trend'].shift(1)
    
    # dfにマージ
    features_to_add = ['race_id', 'horse_id', 'horse_front_tendency_lf', 
                       'horse_c4_position_std', 'horse_position_improvement_trend']
    feat_df = c4[features_to_add].drop_duplicates()
    
    df = df.merge(feat_df, on=['race_id', 'horse_id'], how='left')
    
    for col in ['horse_front_tendency_lf', 'horse_c4_position_std', 'horse_position_improvement_trend']:
        valid = df[col].notna().sum()
        logger.info(f"  {col}非NaN: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
    
    return df


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
    logger.info("=" * 60)
    logger.info("Phase 3 #10: コーナー特徴量拡張（リークフリー）")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件")
    logger.info(f"  Test:  {len(test):,}件")
    
    # V15特徴量
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    v15_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # コーナー特徴量追加
    logger.info("")
    logger.info("=" * 60)
    logger.info("コーナー特徴量追加")
    logger.info("=" * 60)
    
    train_f = add_corner_features_leakfree(train_f, corners, races)
    test_f = add_corner_features_leakfree(test_f, corners, races)
    
    new_features = ['horse_front_tendency_lf', 'horse_c4_position_std', 'horse_position_improvement_trend']
    v16_cols = v15_cols + [f for f in new_features if f in train_f.columns]
    
    logger.info(f"  V15特徴量: {len(v15_cols)}")
    logger.info(f"  V16特徴量: {len(v16_cols)} (+{len(v16_cols) - len(v15_cols)})")
    
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
    
    # ========== ベースライン (V15) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("ベースライン (V15)")
    logger.info("=" * 60)
    
    X_train_v15 = train_f[v15_cols].fillna(0)
    X_test_v15 = test_f[v15_cols].fillna(0)
    
    train_ds_v15 = lgb.Dataset(X_train_v15, y_train)
    model_v15 = lgb.train(params, train_ds_v15, num_boost_round=200)
    
    pred_train_v15 = model_v15.predict(X_train_v15)
    pred_test_v15 = model_v15.predict(X_test_v15)
    
    train_roi_v15, train_hit_v15 = calc_roi(train_f, pred_train_v15)
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_test_v15)
    
    logger.info(f"  Train: ROI={train_roi_v15:.1f}%, 的中率={train_hit_v15:.1f}%")
    logger.info(f"  Test:  ROI={test_roi_v15:.1f}%, 的中率={test_hit_v15:.1f}%")
    logger.info(f"  Gap:   {train_roi_v15 - test_roi_v15:.1f}%")
    
    # ========== V16 コーナー特徴量追加 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("V16 (コーナー特徴量追加)")
    logger.info("=" * 60)
    
    X_train_v16 = train_f[v16_cols].fillna(0)
    X_test_v16 = test_f[v16_cols].fillna(0)
    
    train_ds_v16 = lgb.Dataset(X_train_v16, y_train)
    model_v16 = lgb.train(params, train_ds_v16, num_boost_round=200)
    
    pred_train_v16 = model_v16.predict(X_train_v16)
    pred_test_v16 = model_v16.predict(X_test_v16)
    
    train_roi_v16, train_hit_v16 = calc_roi(train_f, pred_train_v16)
    test_roi_v16, test_hit_v16 = calc_roi(test_f, pred_test_v16)
    
    logger.info(f"  Train: ROI={train_roi_v16:.1f}%, 的中率={train_hit_v16:.1f}%")
    logger.info(f"  Test:  ROI={test_roi_v16:.1f}%, 的中率={test_hit_v16:.1f}%")
    logger.info(f"  Gap:   {train_roi_v16 - test_roi_v16:.1f}%")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    improvement = test_roi_v16 - test_roi_v15
    logger.info(f"                  V15        V16        改善幅")
    logger.info(f"  Test ROI:      {test_roi_v15:7.1f}%    {test_roi_v16:7.1f}%    {improvement:+.1f}%")
    logger.info(f"  Gap:           {train_roi_v15-test_roi_v15:7.1f}%    {train_roi_v16-test_roi_v16:7.1f}%")
    
    if improvement > 0:
        logger.info(f"\n  ✅ V16が改善!")
    else:
        logger.info(f"\n  ❌ V16は改善なし")
    
    # 新特徴量の重要度
    logger.info("")
    logger.info("新特徴量の重要度:")
    importance = pd.DataFrame({
        'feature': v16_cols,
        'importance': model_v16.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for feat in new_features:
        if feat in importance['feature'].values:
            idx = importance[importance['feature'] == feat].index[0]
            rank = list(importance.index).index(idx) + 1
            imp = importance.loc[idx, 'importance']
            logger.info(f"  {feat}: importance={imp:.1f}, rank={rank}/{len(v16_cols)}")
    
    return model_v15, model_v16, test_roi_v15, test_roi_v16


if __name__ == "__main__":
    main()
