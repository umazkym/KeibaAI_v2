"""
V9特徴量エンジニアのテストとV8.1との比較

【実行方法】
python scripts/training/test_v9_features.py

【評価項目】
1. リークチェック: Train-Test差分 < 10%
2. ROI比較: V8.1 vs V9
3. 特徴量重要度分析
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging
import lightgbm as lgb

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_roi(features_df, returns_df, top_n=1):
    """ROI計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho.columns = ['race_id', 'horse_number', 'tansho_payout']
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    top_picks = features_df[features_df['rank'] == top_n].copy()
    top_picks['race_id'] = top_picks['race_id'].astype(str)
    top_picks['horse_number'] = pd.to_numeric(top_picks['horse_number'], errors='coerce')
    
    merged = top_picks.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    total_bet = len(merged) * 100
    total_payout = merged['tansho_payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    
    hits = (merged['tansho_payout'] > 0).sum()
    hit_rate = hits / len(merged) * 100 if len(merged) > 0 else 0
    
    return roi, hit_rate, len(merged)


def train_and_evaluate(fe_class, fe_name, train_df, valid_df, test_df, 
                       pedigrees_df, corners_df, race_details_df, returns_df,
                       v8_feature_cols=None):
    """モデル学習と評価"""
    logger.info(f"\n{'='*70}")
    logger.info(f"{fe_name} 評価")
    logger.info(f"{'='*70}")
    
    # fit
    fe = fe_class()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    # transform
    logger.info(f"{fe_name} transform...")
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    logger.info(f"使用特徴量数: {len(feature_cols)}")
    
    # 新特徴量のリスト
    if v8_feature_cols:
        new_features = [c for c in feature_cols if c not in v8_feature_cols]
        logger.info(f"新特徴量: {new_features}")
    
    # モデル学習（強力な正則化）
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    valid_sorted = valid_features.sort_values('race_id')
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    # Relevanceスコア
    y_rel_train = np.zeros(len(y_train))
    y_rel_train[y_train.values == 1] = 5
    y_rel_train[y_train.values == 2] = 4
    y_rel_train[y_train.values == 3] = 3
    y_rel_train[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    y_rel_valid = np.zeros(len(y_valid))
    y_rel_valid[y_valid.values == 1] = 5
    y_rel_valid[y_valid.values == 2] = 4
    y_rel_valid[y_valid.values == 3] = 3
    y_rel_valid[(y_valid.values >= 4) & (y_valid.values <= 5)] = 1
    
    # 強力な正則化（V7レポートと同じ設定）
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=3000,
        learning_rate=0.005,      # より低い学習率
        max_depth=2,              # より浅い木
        num_leaves=4,             # より少ない葉
        min_child_samples=200,    # より大きなサンプル要件
        reg_alpha=3.0,            # 強力なL1正則化
        reg_lambda=5.0,           # 強力なL2正則化
        subsample=0.5,            # より強いサンプリング
        colsample_bytree=0.5,
        max_bin=63,
        min_split_gain=0.1,
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_rel_train, 
        group=groups_train,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    logger.info(f"学習完了 (best iteration: {model.best_iteration_})")
    
    # 予測
    for df, name in [(train_features, 'Train'), (valid_features, 'Valid'), (test_features, 'Test')]:
        df['score'] = model.predict(df[feature_cols].fillna(0))
        df['rank'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # リークチェック
    train_hit = (train_features[train_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    test_hit = (test_features[test_features['rank'] == 1]['finish_position'] == 1).mean() * 100
    gap = train_hit - test_hit
    
    logger.info(f"\n過学習チェック:")
    logger.info(f"  Train的中率: {train_hit:.1f}%")
    logger.info(f"  Test的中率:  {test_hit:.1f}%")
    logger.info(f"  差分: {gap:.1f}%")
    
    if abs(gap) < 5:
        status = "✓ 正常 (差分 < 5%)"
    elif abs(gap) < 10:
        status = "✓ 許容範囲 (差分 < 10%)"
    elif abs(gap) < 15:
        status = "⚠ 注意 (差分 < 15%)"
    else:
        status = "✗ 過学習傾向あり (差分 >= 15%)"
    logger.info(f"  判定: {status}")
    
    # ROI計算
    train_roi, train_hr, train_n = calculate_roi(train_features, returns_df)
    valid_roi, valid_hr, valid_n = calculate_roi(valid_features, returns_df)
    test_roi, test_hr, test_n = calculate_roi(test_features, returns_df)
    
    logger.info(f"\nROI結果:")
    logger.info(f"  Train: ROI={train_roi:.1f}%, 的中率={train_hr:.1f}%, n={train_n}")
    logger.info(f"  Valid: ROI={valid_roi:.1f}%, 的中率={valid_hr:.1f}%, n={valid_n}")
    logger.info(f"  Test:  ROI={test_roi:.1f}%, 的中率={test_hr:.1f}%, n={test_n}")
    
    # 特徴量重要度
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info(f"\n特徴量重要度 Top 20:")
    for idx, row in importance.head(20).iterrows():
        # 新特徴量にはマーカーをつける
        marker = ""
        if v8_feature_cols and row['feature'] not in v8_feature_cols:
            marker = "★NEW "
        logger.info(f"  {marker}{row['feature']}: {row['importance']:.0f}")
    
    return {
        'name': fe_name,
        'n_features': len(feature_cols),
        'train_roi': train_roi,
        'valid_roi': valid_roi,
        'test_roi': test_roi,
        'train_hit': train_hit,
        'test_hit': test_hit,
        'gap': gap,
        'importance': importance
    }


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    logger.info("="*70)
    logger.info("V8.1 vs V9 特徴量エンジニア比較テスト")
    logger.info("="*70)
    
    # データ読み込み
    logger.info("\nデータ読み込み中...")
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    logger.info(f"  races: {len(races_df):,}件")
    logger.info(f"  pedigrees: {len(pedigrees_df):,}件")
    logger.info(f"  corners: {len(corners_df):,}件")
    logger.info(f"  race_details: {len(race_details_df):,}件")
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    logger.info(f"\nデータ分割:")
    logger.info(f"  Train: {len(train_df):,} ({train_df['race_date'].min().date()} - {train_df['race_date'].max().date()})")
    logger.info(f"  Valid: {len(valid_df):,} ({valid_df['race_date'].min().date()} - {valid_df['race_date'].max().date()})")
    logger.info(f"  Test:  {len(test_df):,} ({test_df['race_date'].min().date()} - {test_df['race_date'].max().date()})")
    
    # インポート
    from keibaai.src.features.leak_free_feature_engineer_v8 import LeakFreeFeatureEngineerV8
    from keibaai.src.features.leak_free_feature_engineer_v9 import LeakFreeFeatureEngineerV9
    
    # V8.1の特徴量リスト
    v8_feature_cols = LeakFreeFeatureEngineerV8.FEATURE_COLS
    
    results = []
    
    # V8.1評価
    v8_result = train_and_evaluate(
        LeakFreeFeatureEngineerV8, "V8.1",
        train_df, valid_df, test_df,
        pedigrees_df, corners_df, race_details_df, returns_df
    )
    results.append(v8_result)
    
    # V9評価
    v9_result = train_and_evaluate(
        LeakFreeFeatureEngineerV9, "V9",
        train_df, valid_df, test_df,
        pedigrees_df, corners_df, race_details_df, returns_df,
        v8_feature_cols=v8_feature_cols
    )
    results.append(v9_result)
    
    # 比較サマリー
    logger.info("\n" + "="*70)
    logger.info("比較サマリー")
    logger.info("="*70)
    
    logger.info(f"\n{'モデル':<8} {'特徴量':<8} {'Train ROI':<12} {'Test ROI':<12} {'Gap':<10} {'判定':<15}")
    logger.info("-"*70)
    for r in results:
        if abs(r['gap']) < 10:
            status = "✓ OK"
        elif abs(r['gap']) < 15:
            status = "⚠ 注意"
        else:
            status = "✗ 過学習"
        
        logger.info(f"{r['name']:<8} {r['n_features']:<8} {r['train_roi']:.1f}%{'':<7} {r['test_roi']:.1f}%{'':<7} {r['gap']:.1f}%{'':<5} {status}")
    
    # V9の改善度
    if len(results) == 2:
        v8, v9 = results
        test_improvement = v9['test_roi'] - v8['test_roi']
        
        logger.info(f"\n{'='*70}")
        logger.info("V9評価結論")
        logger.info("="*70)
        logger.info(f"Test ROI改善: {test_improvement:+.1f}%")
        logger.info(f"過学習差分: V8={v8['gap']:.1f}% → V9={v9['gap']:.1f}%")
        
        if v9['gap'] > 15:
            logger.warning("\n⚠ V9は過学習傾向があります。特徴量の見直しが必要です。")
        elif test_improvement > 2:
            logger.info("\n✓ V9はV8.1より優れています！採用を推奨します。")
        elif test_improvement > -2:
            logger.info("\n△ V9はV8.1とほぼ同等です。")
        else:
            logger.info("\n✗ V9はV8.1より劣っています。改善が必要です。")
    
    # 新特徴量の有効性分析
    if len(results) == 2:
        logger.info("\n" + "="*70)
        logger.info("新特徴量の有効性分析")
        logger.info("="*70)
        
        v9_importance = v9['importance']
        new_features = [c for c in v9_importance['feature'] if c not in v8_feature_cols]
        
        logger.info(f"\n新特徴量の重要度ランキング:")
        for feat in new_features:
            rank = v9_importance[v9_importance['feature'] == feat].index[0] + 1
            imp = v9_importance[v9_importance['feature'] == feat]['importance'].values[0]
            logger.info(f"  {feat}: 重要度順位 {rank}位, 値={imp:.0f}")


if __name__ == '__main__':
    main()
