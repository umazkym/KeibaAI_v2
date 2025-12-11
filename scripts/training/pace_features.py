# -*- coding: utf-8 -*-
"""
Phase 3 #9: 予測ペース特徴量の開発

18_improvement_roadmap.md Phase 3より:
- race_details.parquetのfirst_half/second_halfを活用
- 会場×距離×馬場ごとの平均ペースを計算
- 馬の脚質とペース相性を特徴量化

【特徴量案】
1. race_expected_first_half: 予測前半3Fタイム
2. race_expected_pace_type: ハイ/スロー/ミドルペース予測
3. horse_pace_preference: 馬のペース志向（速いペース適性）
4. pace_style_synergy: ペース × 脚質のマッチ度
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


def add_pace_features(df, race_details, races_full):
    """
    ペース特徴量を追加（リークフリー）
    
    【新規特徴量】
    1. race_expected_first_half: 会場×距離×馬場の平均前半3F
    2. race_expected_pace_diff: 前半-後半の予想差（ハイ/スロー指標）
    3. horse_avg_pace_diff_preference: 馬の好むペース差
    4. pace_synergy: 予想ペース × 馬のペース志向
    """
    logger.info("ペース特徴量を計算中...")
    
    # レース詳細とマージ
    details = race_details[['race_id', 'first_half', 'second_half']].copy()
    details = details.dropna(subset=['first_half', 'second_half'])
    
    # ペース差（前半-後半）: マイナス=スロー、プラス=ハイ
    details['pace_diff'] = details['first_half'] - details['second_half']
    
    # 会場×距離×馬場ごとの平均ペースを計算
    race_info = races_full[['race_id', 'venue', 'distance_m', 'track_condition', 'race_date']].drop_duplicates()
    details = details.merge(race_info, on='race_id', how='left')
    
    # 距離カテゴリ
    details['distance_cat'] = pd.cut(
        details['distance_m'], 
        bins=[0, 1400, 1800, 2200, 4000], 
        labels=['sprint', 'mile', 'middle', 'long']
    )
    
    # グループごとの累積平均（リークフリー: 当日より前のデータのみ使用）
    details = details.sort_values('race_date')
    
    # グローバルペース統計（会場×距離×馬場）
    pace_stats = {}
    for _, row in details.iterrows():
        key = (row['venue'], row['distance_cat'], row['track_condition'])
        if key not in pace_stats:
            pace_stats[key] = {'first_half_sum': 0, 'count': 0, 'pace_diff_sum': 0}
        
        # 現レースの予測値 = これまでの平均
        pace_stats[key]['first_half_sum'] += row['first_half']
        pace_stats[key]['pace_diff_sum'] += row['pace_diff']
        pace_stats[key]['count'] += 1
    
    # 各グループの最終平均を取得
    final_pace = {}
    for key, vals in pace_stats.items():
        if vals['count'] > 0:
            final_pace[key] = {
                'first_half_avg': vals['first_half_sum'] / vals['count'],
                'pace_diff_avg': vals['pace_diff_sum'] / vals['count']
            }
    
    # dfにマージするための準備
    df = df.copy()
    df['distance_cat'] = pd.cut(
        df['distance_m'], 
        bins=[0, 1400, 1800, 2200, 4000], 
        labels=['sprint', 'mile', 'middle', 'long']
    )
    
    # 予想ペースを付与
    df['race_expected_first_half'] = df.apply(
        lambda x: final_pace.get((x['venue'], x['distance_cat'], x['track_condition']), {}).get('first_half_avg', np.nan),
        axis=1
    )
    df['race_expected_pace_diff'] = df.apply(
        lambda x: final_pace.get((x['venue'], x['distance_cat'], x['track_condition']), {}).get('pace_diff_avg', np.nan),
        axis=1
    )
    
    valid_count = df['race_expected_first_half'].notna().sum()
    logger.info(f"  race_expected_first_half非NaN: {valid_count:,}/{len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
    # 馬のペース志向（過去レースのペース差で好成績だった時のペース）
    details_with_result = details.merge(
        races_full[['race_id', 'horse_id', 'finish_position']].drop_duplicates(),
        on='race_id',
        how='left'
    )
    
    # 馬ごとの平均ペース差（Top3入りしたレースのペース）
    top3_races = details_with_result[details_with_result['finish_position'] <= 3]
    horse_pace_pref = top3_races.groupby('horse_id')['pace_diff'].mean().reset_index()
    horse_pace_pref.columns = ['horse_id', 'horse_preferred_pace_diff']
    
    df = df.merge(horse_pace_pref, on='horse_id', how='left')
    
    valid_count = df['horse_preferred_pace_diff'].notna().sum()
    logger.info(f"  horse_preferred_pace_diff非NaN: {valid_count:,}/{len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
    # ペース相性スコア = 予想ペースと馬の好みの差（絶対値が小さいほど相性が良い）
    df['pace_synergy'] = -abs(df['race_expected_pace_diff'] - df['horse_preferred_pace_diff'])
    
    valid_count = df['pace_synergy'].notna().sum()
    logger.info(f"  pace_synergy非NaN: {valid_count:,}/{len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
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
    logger.info("Phase 3 #9: 予測ペース特徴量")
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
    
    v15_feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # 新ペース特徴量追加
    logger.info("")
    logger.info("=" * 60)
    logger.info("ペース特徴量追加")
    logger.info("=" * 60)
    
    train_f = add_pace_features(train_f, race_details, races)
    test_f = add_pace_features(test_f, race_details, races)
    
    new_features = ['race_expected_first_half', 'race_expected_pace_diff', 'horse_preferred_pace_diff', 'pace_synergy']
    v16_feature_cols = v15_feature_cols + [f for f in new_features if f in train_f.columns]
    
    logger.info(f"  V15特徴量: {len(v15_feature_cols)}")
    logger.info(f"  V16特徴量: {len(v16_feature_cols)} (+{len(v16_feature_cols) - len(v15_feature_cols)})")
    
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
    
    X_train_v15 = train_f[v15_feature_cols].fillna(0)
    X_test_v15 = test_f[v15_feature_cols].fillna(0)
    
    train_ds_v15 = lgb.Dataset(X_train_v15, y_train)
    model_v15 = lgb.train(params, train_ds_v15, num_boost_round=200)
    
    pred_train_v15 = model_v15.predict(X_train_v15)
    pred_test_v15 = model_v15.predict(X_test_v15)
    
    train_roi_v15, train_hit_v15 = calc_roi(train_f, pred_train_v15)
    test_roi_v15, test_hit_v15 = calc_roi(test_f, pred_test_v15)
    
    logger.info(f"  Train: ROI={train_roi_v15:.1f}%, 的中率={train_hit_v15:.1f}%")
    logger.info(f"  Test:  ROI={test_roi_v15:.1f}%, 的中率={test_hit_v15:.1f}%")
    logger.info(f"  Gap:   {train_roi_v15 - test_roi_v15:.1f}%")
    
    # ========== V16 (ペース特徴量追加) ==========
    logger.info("")
    logger.info("=" * 60)
    logger.info("V16 (ペース特徴量追加)")
    logger.info("=" * 60)
    
    X_train_v16 = train_f[v16_feature_cols].fillna(0)
    X_test_v16 = test_f[v16_feature_cols].fillna(0)
    
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
        'feature': v16_feature_cols,
        'importance': model_v16.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    for feat in new_features:
        if feat in importance['feature'].values:
            idx = importance[importance['feature'] == feat].index[0]
            rank = list(importance.index).index(idx) + 1
            imp = importance.loc[idx, 'importance']
            logger.info(f"  {feat}: importance={imp:.1f}, rank={rank}/{len(v16_feature_cols)}")
    
    return model_v15, model_v16


if __name__ == "__main__":
    main()
