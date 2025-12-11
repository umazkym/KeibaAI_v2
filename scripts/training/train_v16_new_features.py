# -*- coding: utf-8 -*-
"""
新特徴量V16実験

18_improvement_roadmapで提案された未実装特徴量を追加しテスト

【追加候補】
1. horse_age_months: レース時点での馬の月齢
   - 若い馬は成長途上で変動が大きい
   - 4歳以上で安定する傾向

2. horse_c4_position_std: C4ポジションの安定性
   - 脚質が一定な馬はレース展開が予測しやすい

【リーク防止】
- 月齢: birth_date (固定情報) と race_date から計算 → リークなし
- std: 過去レースの累積std + shift(1) → リークなし
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
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def add_age_months(df, horses):
    """馬の月齢を追加（リークなし）"""
    logger.info("  馬の月齢を計算中...")
    
    horses = horses[['horse_id', 'birth_date']].copy()
    horses['birth_date'] = pd.to_datetime(horses['birth_date'], errors='coerce')
    
    df = df.merge(horses, on='horse_id', how='left')
    
    # 月齢計算
    df['horse_age_months'] = (df['race_date'] - df['birth_date']).dt.days / 30.44
    
    # birth_date不明はNaN
    valid_count = df['horse_age_months'].notna().sum()
    logger.info(f"    月齢計算済み: {valid_count:,}/{len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
    # birth_dateカラム削除（元データ汚染防止）
    df = df.drop(columns=['birth_date'])
    
    return df


def add_c4_position_std(df, corners):
    """C4ポジション安定性を追加（リークフリー: shift(1)）"""
    logger.info("  C4ポジション安定性を計算中...")
    
    # C4のみ抽出（カラム名は 'corner' not 'corner_number'）
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
    c4 = c4.rename(columns={'position': 'c4_position'})
    
    # レースごとの相対C4ポジション（horse_numberでマージ）
    df = df.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # 馬ごとの累積std (shift(1)でリーク防止)
    df = df.sort_values(['horse_id', 'race_date'])
    
    # expanding std + shift
    df['horse_c4_position_std'] = df.groupby('horse_id')['c4_position'].transform(
        lambda x: x.expanding().std().shift(1)
    )
    
    valid_count = df['horse_c4_position_std'].notna().sum()
    logger.info(f"    C4 std計算済み: {valid_count:,}/{len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
    # 中間カラム削除
    df = df.drop(columns=['c4_position'], errors='ignore')
    
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
    logger.info("新特徴量V16実験 (月齢 + C4ポジション安定性)")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # V15公式期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"  Train: {len(train):,}件 (~2024-12-31)")
    logger.info(f"  Test:  {len(test):,}件 (2025-01-01~)")
    
    # V15特徴量生成
    logger.info("")
    logger.info("V15特徴量生成中...")
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    v15_feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    logger.info(f"  V15特徴量数: {len(v15_feature_cols)}")
    
    # 新特徴量追加
    logger.info("")
    logger.info("=" * 60)
    logger.info("新特徴量追加")
    logger.info("=" * 60)
    
    # 月齢
    train_f = add_age_months(train_f, horses)
    test_f = add_age_months(test_f, horses)
    
    # C4ポジション安定性
    train_f = add_c4_position_std(train_f, corners)
    test_f = add_c4_position_std(test_f, corners)
    
    # V16特徴量リスト
    new_features = ['horse_age_months', 'horse_c4_position_std']
    v16_feature_cols = v15_feature_cols + new_features
    
    logger.info(f"  V16特徴量数: {len(v16_feature_cols)} (+{len(new_features)})")
    
    # ========== ベースライン (V15) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. ベースライン (V15特徴量のみ)")
    logger.info("=" * 60)
    
    X_train_v15 = train_f[v15_feature_cols].fillna(0)
    X_test_v15 = test_f[v15_feature_cols].fillna(0)
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
    
    train_ds_v15 = lgb.Dataset(X_train_v15, y_train)
    model_v15 = lgb.train(params, train_ds_v15, num_boost_round=200)
    
    pred_train_v15 = model_v15.predict(X_train_v15)
    pred_test_v15 = model_v15.predict(X_test_v15)
    
    train_roi_v15, train_hit_v15 = calc_roi(train_f, pred_train_v15)
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_test_v15)
    
    logger.info(f"  Train: 的中率={train_hit_v15:.1f}%, ROI={train_roi_v15:.1f}%")
    logger.info(f"  Test:  的中率={test_hit_v15:.1f}%, ROI={test_roi_v15:.1f}%")
    logger.info(f"  Gap: {train_roi_v15 - test_roi_v15:.1f}%")
    
    # ========== V16 (新特徴量追加) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. V16 (新特徴量: 月齢 + C4 std)")
    logger.info("=" * 60)
    
    X_train_v16 = train_f[v16_feature_cols].fillna(0)
    X_test_v16 = test_f[v16_feature_cols].fillna(0)
    
    train_ds_v16 = lgb.Dataset(X_train_v16, y_train)
    model_v16 = lgb.train(params, train_ds_v16, num_boost_round=200)
    
    pred_train_v16 = model_v16.predict(X_train_v16)
    pred_test_v16 = model_v16.predict(X_test_v16)
    
    train_roi_v16, train_hit_v16 = calc_roi(train_f, pred_train_v16)
    test_roi_v16, test_hit_v16 = calc_roi(test_f, pred_test_v16)
    
    logger.info(f"  Train: 的中率={train_hit_v16:.1f}%, ROI={train_roi_v16:.1f}%")
    logger.info(f"  Test:  的中率={test_hit_v16:.1f}%, ROI={test_roi_v16:.1f}%")
    logger.info(f"  Gap: {train_roi_v16 - test_roi_v16:.1f}%")
    
    # ========== 比較 ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("結果比較")
    logger.info("=" * 60)
    
    improvement = test_roi_v16 - test_roi_v15
    logger.info(f"                    V15        V16        改善幅")
    logger.info(f"  Test ROI:        {test_roi_v15:7.1f}%     {test_roi_v16:7.1f}%     {improvement:+.1f}%")
    logger.info(f"  Test 的中率:     {test_hit_v15:7.1f}%     {test_hit_v16:7.1f}%")
    logger.info(f"  Gap:             {train_roi_v15-test_roi_v15:7.1f}%     {train_roi_v16-test_roi_v16:7.1f}%")
    
    if improvement > 0:
        logger.info(f"\n  ✅ V16が改善!")
    else:
        logger.info(f"\n  ❌ V16は改善なし (or 悪化)")
    
    # 新特徴量の重要度
    logger.info("")
    logger.info("新特徴量の重要度:")
    importance = pd.DataFrame({
        'feature': v16_feature_cols,
        'importance': model_v16.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for feat in new_features:
        rank = importance[importance['feature'] == feat].index[0] + 1
        imp = importance[importance['feature'] == feat]['importance'].values[0]
        logger.info(f"  {feat}: importance={imp:.1f}, rank={rank}/{len(v16_feature_cols)}")
    
    return model_v15, model_v16, test_roi_v15, test_roi_v16


if __name__ == "__main__":
    main()
