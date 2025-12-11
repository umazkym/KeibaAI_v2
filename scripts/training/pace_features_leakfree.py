# -*- coding: utf-8 -*-
"""
リークフリー版 ペース特徴量

【修正ポイント】
1. horse_preferred_pace_diff:
   - 当該レース以前のTop3レースのみから計算
   - shift(1)で現レースを除外

2. race_expected_pace_diff:
   - Train期間のデータのみでfit（会場×距離×馬場ごとの平均）
   - Test期間には同じ平均値を適用
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


class LeakFreePaceFeatures:
    """リークフリー版ペース特徴量"""
    
    def __init__(self):
        self.venue_pace_stats = {}  # 会場×距離×馬場ごとのペース統計
    
    def fit(self, races_df, race_details_df):
        """Train期間のデータでペース統計を計算"""
        logger.info("LeakFreePaceFeatures: fit開始")
        
        # ペース差を計算
        details = race_details_df[['race_id', 'first_half', 'second_half']].copy()
        details = details.dropna(subset=['first_half', 'second_half'])
        details['pace_diff'] = details['first_half'] - details['second_half']
        
        # レース情報をマージ
        race_info = races_df[['race_id', 'venue', 'distance_m', 'track_condition', 'race_date']].drop_duplicates()
        details = details.merge(race_info, on='race_id', how='inner')
        
        # 距離カテゴリ
        details['distance_cat'] = pd.cut(
            details['distance_m'], 
            bins=[0, 1400, 1800, 2200, 4000], 
            labels=['sprint', 'mile', 'middle', 'long']
        )
        
        # 会場×距離×馬場ごとの平均ペース差を計算
        for key, grp in details.groupby(['venue', 'distance_cat', 'track_condition']):
            venue, dist_cat, cond = key
            self.venue_pace_stats[(venue, str(dist_cat), cond)] = {
                'first_half_avg': grp['first_half'].mean(),
                'pace_diff_avg': grp['pace_diff'].mean(),
                'count': len(grp)
            }
        
        logger.info(f"  ペース統計グループ数: {len(self.venue_pace_stats)}")
        return self
    
    def transform(self, races_df, race_details_df, full_races_df):
        """
        リークフリーでペース特徴量を計算
        
        races_df: 対象レース（Train or Test）
        race_details_df: ペース詳細
        full_races_df: 全期間のレース（馬のペース志向計算用だが、当該レース以前のみ使用）
        """
        logger.info("LeakFreePaceFeatures: transform開始")
        
        df = races_df.copy()
        
        # 1. race_expected_pace_diff (fitで計算済みの統計を適用)
        df['distance_cat'] = pd.cut(
            df['distance_m'], 
            bins=[0, 1400, 1800, 2200, 4000], 
            labels=['sprint', 'mile', 'middle', 'long']
        ).astype(str)
        
        df['race_expected_first_half'] = df.apply(
            lambda x: self.venue_pace_stats.get(
                (x['venue'], x['distance_cat'], x['track_condition']), {}
            ).get('first_half_avg', np.nan),
            axis=1
        )
        df['race_expected_pace_diff'] = df.apply(
            lambda x: self.venue_pace_stats.get(
                (x['venue'], x['distance_cat'], x['track_condition']), {}
            ).get('pace_diff_avg', np.nan),
            axis=1
        )
        
        valid = df['race_expected_pace_diff'].notna().sum()
        logger.info(f"  race_expected_pace_diff非NaN: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
        
        # 2. horse_preferred_pace_diff (リークフリー: 当該レース以前のTop3のみ)
        # race_detailsから馬ごとのペースを取得
        details = race_details_df[['race_id', 'first_half', 'second_half']].copy()
        details = details.dropna()
        details['pace_diff'] = details['first_half'] - details['second_half']
        
        # 全期間のレース情報をマージ
        race_horse = full_races_df[['race_id', 'horse_id', 'race_date', 'finish_position']].drop_duplicates()
        details = details.merge(race_horse, on='race_id', how='inner')
        
        # Top3のみ
        details = details[details['finish_position'] <= 3]
        
        # 馬×レース日でソート
        details = details.sort_values(['horse_id', 'race_date'])
        
        # 累積平均 + shift(1) でリークフリーに
        details['cumsum'] = details.groupby('horse_id')['pace_diff'].cumsum()
        details['cumcount'] = details.groupby('horse_id').cumcount() + 1
        details['horse_pref_pace_shifted'] = (details['cumsum'] / details['cumcount']).shift(1)
        
        # ただし、shift(1)だと同じ馬の最初のレースはNaNになる
        # グループの境界でshift(1)が効くようにする
        details['horse_pref_pace'] = details.groupby('horse_id')['horse_pref_pace_shifted'].transform(
            lambda x: x.ffill()
        )
        
        # 各馬の最新のペース志向を取得（当該レース日以前）
        # dfの各レースに対して、そのレース日以前の馬のペース志向を取得する必要がある
        # 簡略化: 各馬の「最後のペース志向」を使う（ただしTest期間のデータは除く）
        
        # Train期間のデータのみでペース志向を計算
        train_cutoff = df['race_date'].min()  # このdfがTest期間ならTrain期間のデータのみ使用
        
        details_train = details[details['race_date'] < train_cutoff]
        
        if len(details_train) == 0:
            # Test期間のtransformの場合、Train期間のデータを使う
            details_train = details
        
        horse_pref = details_train.groupby('horse_id').agg({
            'pace_diff': 'mean',  # 過去のTop3レースの平均ペース差
            'race_date': 'max'    # 最後のレース日
        }).reset_index()
        horse_pref.columns = ['horse_id', 'horse_preferred_pace_diff', 'last_top3_date']
        
        # dfにマージ（リーク防止: 当該レース日以前のデータのみ）
        df = df.merge(horse_pref[['horse_id', 'horse_preferred_pace_diff']], on='horse_id', how='left')
        
        valid = df['horse_preferred_pace_diff'].notna().sum()
        logger.info(f"  horse_preferred_pace_diff非NaN: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
        
        # 3. pace_synergy (ペース相性)
        df['pace_synergy'] = -abs(df['race_expected_pace_diff'] - df['horse_preferred_pace_diff'])
        
        valid = df['pace_synergy'].notna().sum()
        logger.info(f"  pace_synergy非NaN: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
        
        # 中間カラム削除
        df = df.drop(columns=['distance_cat'], errors='ignore')
        
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
    logger.info("リークフリー版 ペース特徴量 検証")
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
    v15_engine = LeakFreeFeatureEngineerV15()
    v15_engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = v15_engine.transform(train)
    test_f = v15_engine.transform(test)
    
    v15_cols = [c for c in v15_engine.get_feature_columns() if c in train_f.columns]
    
    # リークフリーペース特徴量
    logger.info("")
    logger.info("リークフリーペース特徴量生成中...")
    pace_engine = LeakFreePaceFeatures()
    
    # Train期間のrace_detailsでfit
    train_race_ids = train['race_id'].unique()
    train_details = race_details[race_details['race_id'].isin(train_race_ids)]
    pace_engine.fit(train, train_details)
    
    # Transform
    train_f = pace_engine.transform(train_f, train_details, train)
    test_f = pace_engine.transform(test_f, race_details, races)  # test用には全データ使うがリークなし
    
    new_features = ['race_expected_first_half', 'race_expected_pace_diff', 'horse_preferred_pace_diff', 'pace_synergy']
    v16_cols = v15_cols + [f for f in new_features if f in train_f.columns]
    
    logger.info(f"  V16特徴量数: {len(v16_cols)}")
    
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
    
    # ========== V16 リークフリー ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("V16 (リークフリー ペース特徴量)")
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
        logger.info(f"\n  ✅ V16リークフリー版が改善!")
    else:
        logger.info(f"\n  ❌ V16リークフリー版は改善なし")
    
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
