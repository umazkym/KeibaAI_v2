#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値特徴量統合 Walk-Forward検証

【目的】
SP値（スピード指数）の考え方をV15モデルの特徴量として組み込むことで、
予測精度・回収率が向上するかを厳密に検証する。

【検証方法】
- 5年間Walk-Forward (2021-2025)
- A: V15ベースライン（SP値なし）
- B: V15 + SP値由来の特徴量（6個追加）
- 同一条件での比較（パラメータ・乱数シード固定）

【SP値から導出する特徴量】（全てリークフリー）
1. horse_sp_avg: 過去SP値の累積平均（shift付き）
2. horse_sp_trend: 直近3走SP値 - 全期間平均（成長/衰え）
3. horse_sp_std: SP値の標準偏差（安定性指標）
4. horse_sp_best: 過去最高SP値（ポテンシャル）
5. horse_last3f_index_avg: 上がり3F指数の累積平均
6. horse_sp_race_rank_avg: レース内SP順位の累積平均（相対力）

【リーク対策】
- SP値の計算自体は当該レースの走破タイムを使用（過去走のみ）
- 全ての特徴量はshift(1)で当該レースを除外
- 基準タイム・馬場指数のfitはTrain期間のみ
"""

import sys
import gc
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb
import logging

warnings.filterwarnings('ignore')

# プロジェクトルートの設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator
from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# ============================================================
# データロード
# ============================================================
def load_data():
    """全データの読み込み"""
    data_dir = project_root / "keibaai/data/parsed/parquet"

    logger.info("データ読み込み中...")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")

    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['race_id'] = races['race_id'].astype(str)

    logger.info(f"  races: {len(races):,}件")
    logger.info(f"  期間: {races['race_date'].min()} ~ {races['race_date'].max()}")

    return races, pedigrees, corners, race_details, horses


# ============================================================
# SP値特徴量の生成（リークフリー）
# ============================================================
def compute_sp_features(races_df: pd.DataFrame, race_details_df: pd.DataFrame,
                        train_end_date: str) -> pd.DataFrame:
    """
    SP値を算出し、馬ごとの累積特徴量に変換する。
    
    【リーク対策の全体設計】
    1. SpeedIndexCalculator.fit() は train_end_date 以前のデータのみで実行
       → 基準タイム・馬場指数はTrain期間で確定
    2. SP値の算出(calculate())は全期間で実行可能
       → SP値は「そのレースの走破タイム」から算出されるため、
         結果が出た後のデータとして扱う
    3. 特徴量化する際に shift(1) で当該レースを除外
       → 過去走のSP値のみを使用
    """
    logger.info("  SP値を算出中...")
    
    # --- Train期間のデータで基準タイム・馬場指数をfit ---
    train_mask = races_df['race_date'] <= pd.Timestamp(train_end_date)
    train_races = races_df[train_mask].copy()

    sp_calc = SpeedIndexCalculator(
        weight_coeff=0.3,
        use_class_offset=False,  # 予測用ではOFF
        use_last3f=True,
        use_condition_base=True,
    )
    sp_calc.fit(train_races)

    # --- 全期間のSP値を算出 ---
    sp_result = sp_calc.calculate(races_df)

    # --- ペース補正（race_detailsがある場合） ---
    rd = race_details_df.copy()
    rd['race_id'] = rd['race_id'].astype(str)
    
    # train期間でペースエンジンをfit
    train_rd_ids = set(train_races['race_id'].unique())
    train_rd = rd[rd['race_id'].isin(train_rd_ids)]
    
    if len(train_rd) > 0:
        pace_engine = PaceCorrectionEngine(
            leading_coeff=0.3,
            closing_coeff=0.2,
        )
        pace_engine.fit(train_races, rd)
        
        # ペース補正対象のレースIDを取得
        all_rd_ids = set(rd['race_id'].unique())
        sp_with_rd = sp_result[sp_result['race_id'].isin(all_rd_ids)]
        
        if len(sp_with_rd) > 0:
            sp_result = pace_engine.apply(sp_result, races_df, rd)
    
    logger.info(f"    SP値算出完了: {sp_result['sp_raw'].notna().sum():,} / {len(sp_result):,} 件")

    # --- SP値ベースの特徴量を馬ごとに生成 ---
    # 最良のSP値カラムを選択
    sp_col = 'sp_extended' if 'sp_extended' in sp_result.columns and sp_result['sp_extended'].notna().any() else 'sp_raw'
    
    sp_df = sp_result[['race_id', 'horse_id', 'race_date', sp_col, 'last3f_index']].copy()
    sp_df = sp_df.rename(columns={sp_col: 'sp_value'})
    sp_df['horse_id'] = sp_df['horse_id'].astype(str)
    sp_df['race_id'] = sp_df['race_id'].astype(str)
    sp_df['race_date'] = pd.to_datetime(sp_df['race_date'])
    
    # SP値が有効なレコードのみ使用
    sp_df = sp_df.dropna(subset=['sp_value'])
    
    # レース内SP順位を計算
    sp_df['sp_rank_in_race'] = sp_df.groupby('race_id')['sp_value'].rank(ascending=False)
    sp_df['sp_field_size'] = sp_df.groupby('race_id')['sp_value'].transform('count')
    sp_df['sp_relative_rank'] = sp_df['sp_rank_in_race'] / sp_df['sp_field_size']
    
    # 時系列ソート
    sp_df = sp_df.sort_values(['horse_id', 'race_date', 'race_id'])

    logger.info("  SP値特徴量を生成中...")

    # ----- 累積特徴量計算（全てshift(1)でリーク防止） -----
    # 1. horse_sp_avg: 過去SP値の累積平均
    sp_df['horse_sp_avg'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    
    # 2. horse_sp_trend: 直近3走平均 - 全期間平均
    sp_df['sp_last3_avg'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.rolling(3, min_periods=3).mean().shift(1)
    )
    sp_df['horse_sp_trend'] = sp_df['sp_last3_avg'] - sp_df['horse_sp_avg']
    
    # 3. horse_sp_std: SP値の標準偏差（安定性指標）
    sp_df['horse_sp_std'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().std().shift(1)
    )
    
    # 4. horse_sp_best: 過去最高SP値（ポテンシャル）
    sp_df['horse_sp_best'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().max().shift(1)
    )
    
    # 5. horse_last3f_index_avg: 上がり3F指数の累積平均
    sp_df['horse_last3f_index_avg'] = sp_df.groupby('horse_id')['last3f_index'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    
    # 6. horse_sp_race_rank_avg: レース内SP相対順位の累積平均
    sp_df['horse_sp_race_rank_avg'] = sp_df.groupby('horse_id')['sp_relative_rank'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    
    # 最低出走数フィルタ（3走以上でないとNaN）
    sp_df['cum_count'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().count().shift(1)
    )
    MIN_RACES = 3
    mask_insufficient = sp_df['cum_count'] < MIN_RACES
    for col in ['horse_sp_avg', 'horse_sp_trend', 'horse_sp_std', 'horse_sp_best',
                'horse_last3f_index_avg', 'horse_sp_race_rank_avg']:
        sp_df.loc[mask_insufficient, col] = np.nan

    # 出力カラムを絞る
    feature_cols = [
        'race_id', 'horse_id',
        'horse_sp_avg', 'horse_sp_trend', 'horse_sp_std',
        'horse_sp_best', 'horse_last3f_index_avg', 'horse_sp_race_rank_avg'
    ]
    result = sp_df[feature_cols].copy()
    
    # 各特徴量のカバレッジをログ出力
    for col in feature_cols[2:]:
        n_valid = result[col].notna().sum()
        logger.info(f"    {col}: {n_valid:,}/{len(result):,} ({n_valid/len(result)*100:.1f}%)")

    return result


# ============================================================
# V15モデルの訓練＆評価（共通ロジック）
# ============================================================
V15_PARAMS = {
    'objective': 'binary',
    'metric': 'auc',
    'learning_rate': 0.03,
    'max_depth': 3,
    'num_leaves': 20,
    'min_child_samples': 100,
    'reg_alpha': 3.0,
    'reg_lambda': 5.0,
    'bagging_fraction': 0.7,
    'feature_fraction': 0.7,
    'bagging_freq': 3,
    'verbose': -1,
    'seed': 42,
}


def calc_roi(df, preds, strategy='R1', min_odds=0.0):
    """ROIを計算"""
    d = df.copy()
    d['pred'] = preds
    d['rank'] = d.groupby('race_id')['pred'].rank(ascending=False, method='first')

    rank_map = {
        'R1': [1],
        'R2': [1, 2],
        'R3': [1, 2, 3],
    }
    ranks = rank_map.get(strategy, [1])

    bets = d[d['rank'].isin(ranks)]
    if min_odds > 0:
        bets = bets[bets['win_odds'] >= min_odds]

    if len(bets) == 0:
        return 0, 0, 0

    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def train_and_evaluate(train_f, test_f, feature_cols, label=''):
    """モデルを訓練して評価"""
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)

    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(V15_PARAMS, train_ds, num_boost_round=200)

    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)

    train_roi, train_hit, _ = calc_roi(train_f, train_pred)
    test_roi, test_hit, n_bets = calc_roi(test_f, test_pred)

    # 特徴量重要度
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)

    return {
        'train_roi': train_roi,
        'test_roi': test_roi,
        'train_hit': train_hit,
        'test_hit': test_hit,
        'n_bets': n_bets,
        'gap': train_roi - test_roi,
        'model': model,
        'importance': importance,
    }


# ============================================================
# メイン: Walk-Forward検証
# ============================================================
def main():
    logger.info("=" * 70)
    logger.info("SP値特徴量統合 Walk-Forward検証")
    logger.info("=" * 70)
    start_time = datetime.now()

    races, pedigrees, corners, race_details, horses = load_data()

    # Walk-Forward設定（5年）
    periods = [
        {'year': 2021, 'train_end': '2020-12-31', 'test_start': '2021-01-01', 'test_end': '2021-12-31'},
        {'year': 2022, 'train_end': '2021-12-31', 'test_start': '2022-01-01', 'test_end': '2022-12-31'},
        {'year': 2023, 'train_end': '2022-12-31', 'test_start': '2023-01-01', 'test_end': '2023-12-31'},
        {'year': 2024, 'train_end': '2023-12-31', 'test_start': '2024-01-01', 'test_end': '2024-12-31'},
        {'year': 2025, 'train_end': '2024-12-31', 'test_start': '2025-01-01', 'test_end': '2025-11-01'},
    ]

    all_results = []

    for period in periods:
        year = period['year']
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Year {year}: Train ~{period['train_end']}, Test {period['test_start']}~{period['test_end']}")
        logger.info(f"{'=' * 60}")

        # データ分割
        train = races[races['race_date'] <= period['train_end']].copy()
        test = races[
            (races['race_date'] >= period['test_start']) &
            (races['race_date'] < period['test_end'])
        ].copy()

        if len(test) == 0:
            logger.warning(f"  テストデータなし。スキップ。")
            continue

        logger.info(f"  Train: {len(train):,}件, Test: {len(test):,}件")

        # ---- A: V15ベースライン ----
        logger.info("\n  [A] V15ベースライン...")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)

        train_f_base = engine.transform(train)
        test_f_base = engine.transform(test)

        base_feature_cols = [c for c in engine.get_feature_columns() if c in train_f_base.columns]
        logger.info(f"    ベース特徴量数: {len(base_feature_cols)}")

        result_base = train_and_evaluate(train_f_base, test_f_base, base_feature_cols, label='V15')

        logger.info(f"    V15 Base:  Train ROI={result_base['train_roi']:.1f}%, "
                     f"Test ROI={result_base['test_roi']:.1f}%, "
                     f"Gap={result_base['gap']:.1f}%, "
                     f"Hit={result_base['test_hit']:.1f}%, "
                     f"Bets={result_base['n_bets']:,}")

        # ---- B: V15 + SP値特徴量 ----
        logger.info("\n  [B] V15 + SP値特徴量...")
        
        # SP値特徴量を生成（全期間のデータを使用し、Trainで基準タイムをfit）
        all_data = pd.concat([train, test], ignore_index=True)
        sp_features = compute_sp_features(all_data, race_details, period['train_end'])

        # V15特徴量にSP特徴量をマージ
        train_f_sp = train_f_base.copy()
        test_f_sp = test_f_base.copy()
        
        train_f_sp['race_id'] = train_f_sp['race_id'].astype(str)
        train_f_sp['horse_id'] = train_f_sp['horse_id'].astype(str)
        test_f_sp['race_id'] = test_f_sp['race_id'].astype(str)
        test_f_sp['horse_id'] = test_f_sp['horse_id'].astype(str)

        sp_feature_cols_only = [
            'horse_sp_avg', 'horse_sp_trend', 'horse_sp_std',
            'horse_sp_best', 'horse_last3f_index_avg', 'horse_sp_race_rank_avg'
        ]
        
        sp_merge_cols = ['race_id', 'horse_id'] + sp_feature_cols_only
        sp_to_merge = sp_features[sp_merge_cols].drop_duplicates(['race_id', 'horse_id'])

        train_f_sp = train_f_sp.merge(sp_to_merge, on=['race_id', 'horse_id'], how='left')
        test_f_sp = test_f_sp.merge(sp_to_merge, on=['race_id', 'horse_id'], how='left')

        # SP特徴量カバレッジの確認
        for col in sp_feature_cols_only:
            train_cov = train_f_sp[col].notna().mean() * 100
            test_cov = test_f_sp[col].notna().mean() * 100
            logger.info(f"    {col}: Train {train_cov:.1f}%, Test {test_cov:.1f}%")

        sp_feature_cols_all = base_feature_cols + sp_feature_cols_only
        logger.info(f"    統合特徴量数: {len(sp_feature_cols_all)} (+{len(sp_feature_cols_only)})")

        result_sp = train_and_evaluate(train_f_sp, test_f_sp, sp_feature_cols_all, label='V15+SP')

        logger.info(f"    V15+SP:   Train ROI={result_sp['train_roi']:.1f}%, "
                     f"Test ROI={result_sp['test_roi']:.1f}%, "
                     f"Gap={result_sp['gap']:.1f}%, "
                     f"Hit={result_sp['test_hit']:.1f}%, "
                     f"Bets={result_sp['n_bets']:,}")

        # ---- 差分分析 ----
        roi_diff = result_sp['test_roi'] - result_base['test_roi']
        gap_diff = result_sp['gap'] - result_base['gap']
        hit_diff = result_sp['test_hit'] - result_base['test_hit']

        logger.info(f"\n    ▶ ROI差: {roi_diff:+.1f}pt")
        logger.info(f"    ▶ Gap差: {gap_diff:+.1f}pt ({'悪化' if gap_diff > 0 else '改善'})")
        logger.info(f"    ▶ 的中率差: {hit_diff:+.1f}pt")

        # SP特徴量の重要度ランキング
        sp_imp = result_sp['importance']
        sp_imp_only = sp_imp[sp_imp['feature'].isin(sp_feature_cols_only)]
        logger.info("\n    SP特徴量の重要度:")
        for _, row in sp_imp_only.iterrows():
            total_rank = (sp_imp['importance'] > row['importance']).sum() + 1
            logger.info(f"      {row['feature']}: {row['importance']:.0f} (全体 {total_rank}/{len(sp_imp)}位)")

        all_results.append({
            'year': year,
            'v15_train_roi': result_base['train_roi'],
            'v15_test_roi': result_base['test_roi'],
            'v15_gap': result_base['gap'],
            'v15_test_hit': result_base['test_hit'],
            'sp_train_roi': result_sp['train_roi'],
            'sp_test_roi': result_sp['test_roi'],
            'sp_gap': result_sp['gap'],
            'sp_test_hit': result_sp['test_hit'],
            'roi_diff': roi_diff,
            'gap_diff': gap_diff,
            'hit_diff': hit_diff,
        })

        # メモリ解放
        del train_f_base, test_f_base, train_f_sp, test_f_sp, sp_features
        gc.collect()

    # ============================================================
    # 最終サマリー
    # ============================================================
    logger.info("\n" + "=" * 70)
    logger.info("最終結果サマリー")
    logger.info("=" * 70)

    df = pd.DataFrame(all_results)
    
    # 年度別結果
    logger.info("\n年度別 Test ROI 比較:")
    logger.info(f"{'年度':>6} | {'V15 Base':>10} | {'V15+SP':>10} | {'差分':>8} | {'Gap(Base)':>10} | {'Gap(SP)':>10}")
    logger.info("-" * 70)
    for _, row in df.iterrows():
        emoji = "✅" if row['roi_diff'] > 0 else "❌"
        logger.info(
            f"{int(row['year']):>6} | {row['v15_test_roi']:>9.1f}% | {row['sp_test_roi']:>9.1f}% | "
            f"{row['roi_diff']:>+7.1f} {emoji} | {row['v15_gap']:>9.1f}% | {row['sp_gap']:>9.1f}%"
        )

    # 平均値
    logger.info("-" * 70)
    mean_base = df['v15_test_roi'].mean()
    mean_sp = df['sp_test_roi'].mean()
    mean_diff = df['roi_diff'].mean()
    mean_gap_base = df['v15_gap'].mean()
    mean_gap_sp = df['sp_gap'].mean()
    win_years = (df['roi_diff'] > 0).sum()
    total_years = len(df)

    logger.info(
        f"{'平均':>6} | {mean_base:>9.1f}% | {mean_sp:>9.1f}% | "
        f"{mean_diff:>+7.1f}   | {mean_gap_base:>9.1f}% | {mean_gap_sp:>9.1f}%"
    )

    logger.info(f"\n改善年数: {win_years}/{total_years}")
    logger.info(f"平均ROI差: {mean_diff:+.1f}pt")
    logger.info(f"平均Gap差: {(mean_gap_sp - mean_gap_base):+.1f}pt")

    # 統計的有意性（簡易t検定）
    if len(df) >= 3:
        from scipy import stats
        t_stat, p_value = stats.ttest_rel(df['sp_test_roi'], df['v15_test_roi'])
        logger.info(f"\n対応のあるt検定: t={t_stat:.3f}, p={p_value:.4f}")
        if p_value < 0.05:
            logger.info("→ 統計的に有意な差あり (p < 0.05)")
        else:
            logger.info(f"→ 統計的に有意な差なし (p = {p_value:.4f})")

    # 結論
    logger.info("\n" + "=" * 70)
    if mean_diff > 1.0 and win_years >= 3:
        logger.info("✅ 結論: SP値特徴量の統合は有効。本番導入を検討。")
    elif mean_diff > 0:
        logger.info("△ 結論: SP値特徴量の統合は微小な効果。追加検証が必要。")
    else:
        logger.info("❌ 結論: SP値特徴量の統合は効果なし/逆効果。")
    logger.info("=" * 70)

    elapsed = datetime.now() - start_time
    logger.info(f"\n総実行時間: {elapsed}")

    # 結果をCSVに保存
    output_dir = project_root / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sp_feature_integration_results.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"結果保存: {output_path}")

    return df


if __name__ == "__main__":
    results = main()
