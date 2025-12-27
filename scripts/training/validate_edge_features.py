# -*- coding: utf-8 -*-
"""
エッジ特徴量（Edge Features）の検証

【背景】
prediction_analysis.mdより、回収率の源泉は:
1. 中堅騎手（勝率8-12%）: 36.5%の回収寄与 → 市場が過小評価
2. 前走4-5着馬: 22.9%の回収寄与 → 「負けた馬」と見なされるが実力低下なし
3. 6-10番人気: 18.8%の回収寄与 → V4.4のオッズ加重学習効果

【改修方針】
これらの「エッジソース」を特徴量として明示化し、モデルが学習しやすくする。

【追加特徴量】
1. jockey_mid_tier: 騎手勝率8-12%なら1（高ROI帯）
2. prev_finish_4_5: 前走4-5着なら1（復帰期待馬）
3. jockey_win_rate_band: 騎手勝率帯カテゴリ（0-8/8-12/12-16/16-20/20+）
4. prev_finish_category: 前走着順カテゴリ（1-3/4-5/6-10/11+）

【リーク対策】
- 全て過去データから計算(.shift(1)済み)
- オッズ・人気は使用しない
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def add_edge_features(df):
    """
    エッジ特徴量を追加
    
    prediction_analysis.mdで発見したROI寄与パターンを特徴量化
    """
    df = df.copy()
    
    # 1. 騎手勝率帯（8-12%が最も回収効率が良い）
    jockey_wr = df['jockey_win_rate'].fillna(0.10)  # デフォルト10%
    
    # 中堅騎手フラグ
    df['jockey_mid_tier'] = ((jockey_wr >= 0.08) & (jockey_wr < 0.12)).astype(float)
    
    # 騎手勝率帯カテゴリ
    df['jockey_wr_band'] = pd.cut(
        jockey_wr, 
        bins=[0, 0.08, 0.12, 0.16, 0.20, 1.0],
        labels=[0, 1, 2, 3, 4]  # 0: 0-8%, 1: 8-12% (エッジ帯), 2: 12-16%, 3: 16-20%, 4: 20%+
    ).astype(float)
    
    # 2. 前走着順帯（4-5着が最も回収効率が良い）
    if 'horse_last_finish' in df.columns:
        last_finish = df['horse_last_finish'].fillna(10)
    elif 'prev_1_finish' in df.columns:
        last_finish = df['prev_1_finish'].fillna(10)
    else:
        last_finish = pd.Series([10] * len(df))
    
    # 前走4-5着フラグ
    df['prev_finish_edge'] = ((last_finish >= 4) & (last_finish <= 5)).astype(float)
    
    # 前走着順カテゴリ
    df['prev_finish_band'] = pd.cut(
        last_finish,
        bins=[0, 3, 5, 10, 99],
        labels=[0, 1, 2, 3]  # 0: 1-3着, 1: 4-5着 (エッジ帯), 2: 6-10着, 3: 11+着
    ).astype(float)
    
    # 3. 複合エッジスコア（中堅騎手 × 前走4-5着）
    df['combo_edge_score'] = df['jockey_mid_tier'] + df['prev_finish_edge']
    
    # 4. 騎手勝率と前走着順の差異（市場の評価とのズレを示唆）
    # 騎手が上手いのに人気がない = 過小評価の可能性
    if 'jockey_top3_rate' in df.columns:
        df['jockey_skill_vs_form'] = df['jockey_top3_rate'].fillna(0.3) - (last_finish - 1) / 10
    
    return df


def calc_roi(df, preds):
    """単勝ROI計算"""
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    top1 = d[d['rank_pred'] == 1]
    hits = top1[top1['finish_position'] == 1]
    if len(top1) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(top1) * 100
    hit_rate = len(hits) / len(top1) * 100
    return roi, hit_rate


def train_model(train_df, valid_df, feature_cols, params):
    """モデル訓練"""
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("エッジ特徴量検証")
    print("=" * 80)
    print("\n【目的】prediction_analysis.mdで発見した回収源（騎手8-12%、前走4-5着）を特徴量化")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    test_periods = [
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2022-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年テスト]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        if len(train) < 10000:
            print("  ⚠️ 訓練データ不足、スキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        # ベースライン特徴量（V15 Fixed）
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # --- ベースラインモデル ---
        print("\n  【ベースライン】V15 Fixed")
        model_base = train_model(train_f, valid_f, base_features, params)
        X_test = test_f[base_features].fillna(0)
        preds_base = model_base.predict(X_test)
        roi_base, hit_base = calc_roi(test_f, preds_base)
        
        # TrainでのROI
        X_train = train_f[base_features].fillna(0)
        train_preds_base = model_base.predict(X_train)
        train_roi_base, _ = calc_roi(train_f, train_preds_base)
        gap_base = train_roi_base - roi_base
        
        # --- エッジ特徴量追加 ---
        print("  【改修版】V15 Fixed + エッジ特徴量")
        train_f = add_edge_features(train_f)
        valid_f = add_edge_features(valid_f)
        test_f = add_edge_features(test_f)
        
        edge_features = ['jockey_mid_tier', 'jockey_wr_band', 'prev_finish_edge', 'prev_finish_band', 'combo_edge_score']
        if 'jockey_skill_vs_form' in test_f.columns:
            edge_features.append('jockey_skill_vs_form')
        
        edge_features = [f for f in edge_features if f in test_f.columns]
        all_features = base_features + edge_features
        
        print(f"    追加特徴量: {edge_features}")
        
        model_edge = train_model(train_f, valid_f, all_features, params)
        X_test_edge = test_f[all_features].fillna(0)
        preds_edge = model_edge.predict(X_test_edge)
        roi_edge, hit_edge = calc_roi(test_f, preds_edge)
        
        # TrainでのROI
        X_train_edge = train_f[all_features].fillna(0)
        train_preds_edge = model_edge.predict(X_train_edge)
        train_roi_edge, _ = calc_roi(train_f, train_preds_edge)
        gap_edge = train_roi_edge - roi_edge
        
        diff = roi_edge - roi_base
        
        print(f"\n  結果（単勝ROI）:")
        print(f"    ┌───────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │      モデル        │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├───────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ V15 ベースライン   │  {roi_base:>6.1f}% │ {hit_base:>5.1f}% │ {gap_base:>6.1f}% │")
        print(f"    │ V15 + エッジ特徴量 │  {roi_edge:>6.1f}% │ {hit_edge:>5.1f}% │ {gap_edge:>6.1f}% │")
        print(f"    └───────────────────┴──────────┴─────────┴──────────┘")
        print(f"    差分: {diff:+.1f}%")
        
        # 特徴量重要度（エッジ特徴量のみ）
        importance = pd.DataFrame({
            'feature': all_features,
            'importance': model_edge.feature_importance(importance_type='gain')
        }).sort_values('importance', ascending=False)
        
        edge_importance = importance[importance['feature'].isin(edge_features)]
        print(f"\n    エッジ特徴量の重要度:")
        for _, row in edge_importance.iterrows():
            print(f"      {row['feature']}: {row['importance']:.1f}")
        
        all_results.append({
            'year': year,
            'roi_base': roi_base, 'roi_edge': roi_edge,
            'hit_base': hit_base, 'hit_edge': hit_edge,
            'gap_base': gap_base, 'gap_edge': gap_edge,
            'diff': diff,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間検証結果サマリー")
    print("=" * 80)
    
    if len(all_results) == 0:
        print("結果がありません")
        return
    
    print(f"\n{'年':<6} {'ベースライン':<15} {'エッジ追加':<15} {'差分':<10} {'Gap(Base)':<10} {'Gap(Edge)':<10}")
    print("-" * 70)
    
    for r in all_results:
        print(f"{r['year']:<6} {r['roi_base']:>10.1f}%     {r['roi_edge']:>10.1f}%     {r['diff']:>+7.1f}%  {r['gap_base']:>8.1f}%  {r['gap_edge']:>8.1f}%")
    
    avg_base = np.mean([r['roi_base'] for r in all_results])
    avg_edge = np.mean([r['roi_edge'] for r in all_results])
    avg_diff = np.mean([r['diff'] for r in all_results])
    avg_gap_base = np.mean([r['gap_base'] for r in all_results])
    avg_gap_edge = np.mean([r['gap_edge'] for r in all_results])
    
    print("-" * 70)
    print(f"{'平均':<6} {avg_base:>10.1f}%     {avg_edge:>10.1f}%     {avg_diff:>+7.1f}%  {avg_gap_base:>8.1f}%  {avg_gap_edge:>8.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_diff > 0:
        print(f"\n  ✅ エッジ特徴量追加で改善 (平均 +{avg_diff:.1f}%)")
        print(f"     prediction_analysis.mdの知見（騎手8-12%, 前走4-5着）の特徴量化が有効")
    else:
        print(f"\n  ❌ エッジ特徴量追加で悪化 (平均 {avg_diff:.1f}%)")
        print(f"     モデルは既にこれらのパターンを学習済みの可能性")
    
    if avg_gap_edge < avg_gap_base:
        print(f"\n  ✅ エッジ特徴量追加で過学習軽減 (Gap: {avg_gap_edge:.1f}% vs {avg_gap_base:.1f}%)")
    else:
        print(f"\n  ⚠️ エッジ特徴量追加で過学習増加 (Gap: {avg_gap_edge:.1f}% vs {avg_gap_base:.1f}%)")


if __name__ == "__main__":
    main()
