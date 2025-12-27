# -*- coding: utf-8 -*-
"""
モデル的中データの多角的深層分析

【分析項目】
1. 的中オッズ分布 - 高オッズ的中（ラッキーヒット）の影響度
2. 月別ROI安定性 - 特定の月に偏っていないか
3. 競馬場別ROI - 特定の競馬場に偏っていないか
4. 的中件数と平均オッズの関係
5. 各モデルの特徴分析
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


def get_v15_params():
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }


def get_v162_params():
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 18, 'max_depth': 3,
        'min_child_samples': 110, 'reg_alpha': 3.5, 'reg_lambda': 5.5,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.65,
    }


def get_v44_residual_params():
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def train_v15_model(train_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds, params):
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.maximum(residual_gain, 0)
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    return model


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def analyze_hits(df, pred_col, model_name):
    """的中データの詳細分析"""
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    
    n_bets = len(bets)
    n_hits = len(hits)
    hit_rate = n_hits / n_bets * 100 if n_bets > 0 else 0
    total_return = hits['win_odds'].sum()
    roi = total_return / n_bets * 100 if n_bets > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"【{model_name}】詳細分析")
    print("=" * 60)
    print(f"  総賭け数: {n_bets:,}件")
    print(f"  的中数: {n_hits:,}件")
    print(f"  的中率: {hit_rate:.1f}%")
    print(f"  ROI: {roi:.1f}%")
    
    # オッズ帯別分析
    print(f"\n  【オッズ帯別的中分析】")
    odds_bins = [(1, 2, '1-2倍'), (2, 5, '2-5倍'), (5, 10, '5-10倍'), 
                 (10, 20, '10-20倍'), (20, 50, '20-50倍'), (50, 999, '50倍以上')]
    
    total_contribution = 0
    for low, high, label in odds_bins:
        mask = (hits['win_odds'] >= low) & (hits['win_odds'] < high)
        subset = hits[mask]
        n = len(subset)
        contribution = subset['win_odds'].sum() / n_bets * 100 if n_bets > 0 else 0
        total_contribution += contribution
        pct = contribution / roi * 100 if roi > 0 else 0
        print(f"    {label:>10}: {n:>4}件, 収益貢献 {contribution:>5.1f}%, ROI寄与 {pct:>5.1f}%")
    
    # 高オッズ(20倍以上)の影響
    high_odds_hits = hits[hits['win_odds'] >= 20]
    high_odds_contribution = high_odds_hits['win_odds'].sum() / n_bets * 100 if n_bets > 0 else 0
    high_odds_pct = high_odds_contribution / roi * 100 if roi > 0 else 0
    print(f"\n  ⚠️ 高オッズ(20倍以上)依存度: {high_odds_pct:.1f}% ({len(high_odds_hits)}件)")
    
    # 月別ROI
    print(f"\n  【月別ROI】")
    bets_copy = bets.copy()
    bets_copy['month'] = bets_copy['race_date'].dt.month
    monthly_results = []
    for month in range(1, 13):
        m_bets = bets_copy[bets_copy['month'] == month]
        m_hits = m_bets[m_bets['finish_position'] == 1]
        if len(m_bets) > 0:
            m_roi = m_hits['win_odds'].sum() / len(m_bets) * 100
            monthly_results.append(m_roi)
            status = "✓" if m_roi >= 100 else "×"
            print(f"    {month:>2}月: {m_roi:>6.1f}% ({len(m_bets):>4}件) {status}")
        else:
            monthly_results.append(0)
    
    monthly_std = np.std([r for r in monthly_results if r > 0])
    print(f"  月別ばらつき σ: {monthly_std:.1f}%")
    
    # 競馬場別ROI（上位5）
    print(f"\n  【競馬場別ROI (上位/下位)】")
    venue_results = []
    for venue in bets_copy['venue'].unique():
        v_bets = bets_copy[bets_copy['venue'] == venue]
        v_hits = v_bets[v_bets['finish_position'] == 1]
        if len(v_bets) >= 50:
            v_roi = v_hits['win_odds'].sum() / len(v_bets) * 100
            venue_results.append((venue, v_roi, len(v_bets)))
    
    venue_results.sort(key=lambda x: x[1], reverse=True)
    print("    上位3競馬場:")
    for venue, v_roi, n in venue_results[:3]:
        print(f"      {venue}: {v_roi:.1f}% ({n}件)")
    print("    下位3競馬場:")
    for venue, v_roi, n in venue_results[-3:]:
        print(f"      {venue}: {v_roi:.1f}% ({n}件)")
    
    venue_rois = [r[1] for r in venue_results]
    venue_std = np.std(venue_rois) if len(venue_rois) > 1 else 0
    print(f"  競馬場別ばらつき σ: {venue_std:.1f}%")
    
    return {
        'model': model_name,
        'roi': roi, 'hit_rate': hit_rate, 'n_hits': n_hits,
        'high_odds_dependency': high_odds_pct,
        'monthly_std': monthly_std, 'venue_std': venue_std
    }


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("モデル的中データの多角的深層分析")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 2024年テストに集中（最新年）
    train_end = '2022-12-31'
    test_start = '2024-01-01'
    test_end = '2024-12-31'
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
    
    print(f"\nTrain: {len(train):,}, Test(2024): {len(test):,}")
    
    engine = LeakFreeFeatureEngineerV15Fixed()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # モデル訓練
    print("\nモデル訓練中...")
    
    # V15
    model_v15 = train_v15_model(train_f, feature_cols, get_v15_params())
    train_f['v15_pred'] = model_v15.predict(X_train)
    test_f['v15_pred'] = model_v15.predict(X_test)
    
    # V16.2
    model_v162 = train_v15_model(train_f, feature_cols, get_v162_params())
    test_f['v162_pred'] = model_v162.predict(X_test)
    
    # V4.4 Residual
    model_v44 = train_v44_residual(train_f, feature_cols, train_f['v15_pred'], get_v44_residual_params())
    test_f['v44_pred'] = model_v44.predict(X_test)
    
    # アンサンブル
    test_f['ens_a_pred'] = normalize(test_f['v15_pred']) * 0.5 + normalize(test_f['v44_pred']) * 0.5
    test_f['ens_b_pred'] = normalize(test_f['v162_pred']) * 0.5 + normalize(test_f['v44_pred']) * 0.5
    
    # 分析実行
    all_results = []
    
    all_results.append(analyze_hits(test_f, 'v15_pred', 'V15 Fixed (単体)'))
    all_results.append(analyze_hits(test_f, 'v162_pred', 'V16.2 軽度正則化 (単体)'))
    all_results.append(analyze_hits(test_f, 'v44_pred', 'V4.4 Residual (単体)'))
    all_results.append(analyze_hits(test_f, 'ens_a_pred', 'V15 + V4.4 Residual (現行)'))
    all_results.append(analyze_hits(test_f, 'ens_b_pred', 'V16.2 + V4.4 Residual'))
    
    # 比較サマリー
    print("\n" + "=" * 80)
    print("モデル比較サマリー")
    print("=" * 80)
    
    print(f"\n{'モデル':30s} {'ROI':>8s} {'的中率':>8s} {'高ｵｯｽﾞ依存':>10s} {'月σ':>8s} {'場σ':>8s}")
    print("-" * 80)
    for r in all_results:
        print(f"{r['model']:30s} {r['roi']:>7.1f}% {r['hit_rate']:>7.1f}% {r['high_odds_dependency']:>9.1f}% {r['monthly_std']:>7.1f}% {r['venue_std']:>7.1f}%")
    
    # 各モデルの強み分析
    print("\n" + "=" * 80)
    print("各モデルの強み・弱み分析")
    print("=" * 80)
    
    for r in all_results:
        print(f"\n【{r['model']}】")
        
        # 強み
        strengths = []
        if r['high_odds_dependency'] < 30:
            strengths.append("低リスク（高オッズ依存度低い）")
        if r['monthly_std'] < 30:
            strengths.append("月別安定性高い")
        if r['venue_std'] < 25:
            strengths.append("競馬場偏りなし")
        if r['hit_rate'] > 22:
            strengths.append("的中率高い")
        if r['roi'] > 85:
            strengths.append("高ROI")
        
        # 弱み
        weaknesses = []
        if r['high_odds_dependency'] >= 40:
            weaknesses.append("高オッズ依存が高い（不安定）")
        if r['monthly_std'] >= 35:
            weaknesses.append("月別ばらつき大")
        if r['venue_std'] >= 30:
            weaknesses.append("競馬場偏りあり")
        if r['hit_rate'] < 20:
            weaknesses.append("的中率低い")
        
        print(f"  強み: {', '.join(strengths) if strengths else 'なし'}")
        print(f"  弱み: {', '.join(weaknesses) if weaknesses else 'なし'}")
    
    # 今後の改善点
    print("\n" + "=" * 80)
    print("今後の改善点（深い推論）")
    print("=" * 80)
    
    print("""
    1. 【高オッズ依存度の軽減】
       - 現状: 高オッズ(20倍以上)がROIの30-40%を占める
       - リスク: 高オッズは再現性が低い（偶然の影響大）
       - 改善案: 的中率向上に重点を置いた学習（低オッズ馬の選別精度向上）
    
    2. 【月別安定性の向上】
       - 現状: 月によって大きくばらつく
       - 原因: 季節要因（馬場状態、馬のコンディション）
       - 改善案: 季節調整済み特徴量の追加（ただし過学習注意）
    
    3. 【競馬場特化モデルの検討】
       - 現状: 競馬場によってROI差が大きい
       - 原因: コース特性の違い
       - 改善案: 競馬場グループ別モデル（過学習リスクあり）
    
    4. 【V4.4 Residualの改良】
       - 現状: V15が過小評価した馬を拾う役割
       - 課題: 本当に強い馬を見逃すリスク
       - 改善案: 残差の計算方法を再検討
    
    5. 【特徴量エンジニアリング】
       - 現状: 66特徴量
       - 未活用データ: レース詳細の一部、血統の深掘り
       - 改善案: コーナー通過順位の時系列変化、血統×距離適性
    """)


if __name__ == "__main__":
    main()
