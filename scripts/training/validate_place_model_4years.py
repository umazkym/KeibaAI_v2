# -*- coding: utf-8 -*-
"""
複勝専用モデル 4年間Walk-forward検証

【目的】
単勝モデル（1着予測）ではなく、**3着以内（複勝）を直接ターゲット**にした
専用モデルを開発し、再現性を4年間で検証する。

【改良点】
1. 4年間Walk-forward検証（2021-2024）
2. オッズ加重学習の適用
3. 単勝モデル流用との比較
4. V15 Fixed特徴量エンジニア使用

【リーク対策】
- オッズ・人気は特徴量に含めない（sample_weightのみ使用可）
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


def calc_place_roi(df, preds, returns_df=None):
    """
    複勝ROI計算
    
    Args:
        df: 予測対象データ
        preds: スコア予測値
        returns_df: 払い戻しデータ（fukusho）
    
    Returns:
        roi: 複勝ROI
        hit_rate: 3着内率
    """
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 予測1位を購入
    top1 = d[d['rank_pred'] == 1].copy()
    
    if returns_df is not None and len(returns_df) > 0:
        # returns.parquetから複勝払戻を取得
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
        top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
        
        total_payout = top1['payout'].fillna(0).sum()
        hit_count = top1['payout'].notna().sum()
    else:
        # 3着以内 × 推定複勝オッズ
        hits = top1[top1['finish_position'] <= 3]
        hit_count = len(hits)
        # 複勝オッズは単勝の約30%と推定
        total_payout = (hits['win_odds'] * 0.30 * 100).sum() if len(hits) > 0 else 0
    
    total_bet = len(top1) * 100
    
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    hit_rate = hit_count / len(top1) * 100 if len(top1) > 0 else 0
    
    return roi, hit_rate


def calc_win_roi(df, preds):
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


def train_win_model(train_df, valid_df, feature_cols, params):
    """単勝モデル訓練（比較用：1着予測）"""
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def train_place_model(train_df, valid_df, feature_cols, params, use_weight=True):
    """
    複勝専用モデル訓練（3着以内予測）
    
    Args:
        use_weight: Trueならオッズ加重学習を適用
    """
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    # ターゲット: 3着以内かどうか
    y_train = (train_df['finish_position'] <= 3).astype(int)
    y_valid = (valid_df['finish_position'] <= 3).astype(int)
    
    if use_weight:
        # オッズ加重（高オッズの3着以内をより重視）
        odds = train_df['win_odds'].fillna(1.0).clip(upper=100)
        sample_weight = np.log1p(odds)
        train_ds = lgb.Dataset(X_train, y_train, weight=sample_weight)
    else:
        train_ds = lgb.Dataset(X_train, y_train)
    
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("複勝専用モデル 4年間Walk-forward検証")
    print("=" * 80)
    print("\n【目的】3着以内を直接ターゲットにしたモデルで複勝ROI改善を検証")
    print("【比較】単勝モデル流用 vs 複勝専用モデル vs 複勝専用+オッズ加重")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    # モデルパラメータ
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 15, 'max_depth': 3,
        'min_child_samples': 150, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    # 4年間Walk-forward検証
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
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        # returns_dfの絞り込み
        test_returns = returns[returns['race_id'].isin(test_f['race_id'].unique())]
        train_returns = returns[returns['race_id'].isin(train_f['race_id'].unique())]
        
        # ----- 3モデル訓練 -----
        print("\n  【1】単勝モデル（1着予測）学習中...")
        model_win = train_win_model(train_f, valid_f, feature_cols, params)
        
        print("  【2】複勝専用モデル（3着以内予測）学習中...")
        model_place = train_place_model(train_f, valid_f, feature_cols, params, use_weight=False)
        
        print("  【3】複勝専用+オッズ加重モデル 学習中...")
        model_place_w = train_place_model(train_f, valid_f, feature_cols, params, use_weight=True)
        
        # ----- テストデータで予測 -----
        X_test = test_f[feature_cols].fillna(0)
        preds_win = model_win.predict(X_test)
        preds_place = model_place.predict(X_test)
        preds_place_w = model_place_w.predict(X_test)
        
        # ----- 複勝ROI計算 -----
        roi_win, hit_win = calc_place_roi(test_f, preds_win, test_returns)
        roi_place, hit_place = calc_place_roi(test_f, preds_place, test_returns)
        roi_place_w, hit_place_w = calc_place_roi(test_f, preds_place_w, test_returns)
        
        # TrainでのROI（過学習チェック）
        X_train = train_f[feature_cols].fillna(0)
        train_preds_win = model_win.predict(X_train)
        train_preds_place = model_place.predict(X_train)
        train_preds_place_w = model_place_w.predict(X_train)
        
        train_roi_win, _ = calc_place_roi(train_f, train_preds_win, train_returns)
        train_roi_place, _ = calc_place_roi(train_f, train_preds_place, train_returns)
        train_roi_place_w, _ = calc_place_roi(train_f, train_preds_place_w, train_returns)
        
        gap_win = train_roi_win - roi_win
        gap_place = train_roi_place - roi_place
        gap_place_w = train_roi_place_w - roi_place_w
        
        print(f"\n  結果（複勝ROI）:")
        print(f"    ┌─────────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │      モデル          │ Test ROI │ 3着内率 │ Gap      │")
        print(f"    ├─────────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ 単勝モデル流用       │  {roi_win:>6.1f}% │ {hit_win:>5.1f}% │ {gap_win:>6.1f}% │")
        print(f"    │ 複勝専用             │  {roi_place:>6.1f}% │ {hit_place:>5.1f}% │ {gap_place:>6.1f}% │")
        print(f"    │ 複勝専用+オッズ加重  │  {roi_place_w:>6.1f}% │ {hit_place_w:>5.1f}% │ {gap_place_w:>6.1f}% │")
        print(f"    └─────────────────────┴──────────┴─────────┴──────────┘")
        
        best_roi = max(roi_win, roi_place, roi_place_w)
        if best_roi == roi_win:
            best = "単勝モデル流用"
        elif best_roi == roi_place:
            best = "複勝専用"
        else:
            best = "複勝専用+オッズ加重"
        print(f"    → 最良: {best} ({best_roi:.1f}%)")
        
        all_results.append({
            'year': year,
            'roi_win': roi_win, 'roi_place': roi_place, 'roi_place_w': roi_place_w,
            'hit_win': hit_win, 'hit_place': hit_place, 'hit_place_w': hit_place_w,
            'gap_win': gap_win, 'gap_place': gap_place, 'gap_place_w': gap_place_w,
        })
    
    # ----- サマリー -----
    print("\n" + "=" * 80)
    print("4年間Walk-forward検証結果サマリー")
    print("=" * 80)
    
    if len(all_results) == 0:
        print("結果がありません")
        return
    
    print(f"\n{'年':<6} {'単勝流用':<12} {'複勝専用':<12} {'複勝+加重':<12} {'最良モデル':<15}")
    print("-" * 60)
    
    for r in all_results:
        rois = [r['roi_win'], r['roi_place'], r['roi_place_w']]
        best_idx = rois.index(max(rois))
        best_names = ['単勝流用', '複勝専用', '複勝+加重']
        print(f"{r['year']:<6} {r['roi_win']:>8.1f}%   {r['roi_place']:>8.1f}%   {r['roi_place_w']:>8.1f}%   {best_names[best_idx]}")
    
    avg_win = np.mean([r['roi_win'] for r in all_results])
    avg_place = np.mean([r['roi_place'] for r in all_results])
    avg_place_w = np.mean([r['roi_place_w'] for r in all_results])
    avg_gap_win = np.mean([r['gap_win'] for r in all_results])
    avg_gap_place = np.mean([r['gap_place'] for r in all_results])
    avg_gap_place_w = np.mean([r['gap_place_w'] for r in all_results])
    
    print("-" * 60)
    print(f"{'平均':<6} {avg_win:>8.1f}%   {avg_place:>8.1f}%   {avg_place_w:>8.1f}%")
    print(f"{'Gap平均':<6} {avg_gap_win:>8.1f}%   {avg_gap_place:>8.1f}%   {avg_gap_place_w:>8.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best_avg = max(avg_win, avg_place, avg_place_w)
    if best_avg == avg_win:
        print(f"\n  → 単勝モデル流用が最良 (平均ROI: {avg_win:.1f}%)")
        print(f"     複勝専用モデルの開発効果なし")
    elif best_avg == avg_place:
        diff = avg_place - avg_win
        print(f"\n  ✅ 複勝専用モデルが最良 (平均ROI: {avg_place:.1f}%, +{diff:.1f}%)")
        print(f"     3着以内を直接ターゲットにする改修は有効")
    else:
        diff = avg_place_w - avg_win
        print(f"\n  ✅ 複勝専用+オッズ加重が最良 (平均ROI: {avg_place_w:.1f}%, +{diff:.1f}%)")
        print(f"     3着以内ターゲット + オッズ加重学習の組み合わせが有効")
    
    if avg_gap_place_w < avg_gap_win and avg_gap_place_w < avg_gap_place:
        print(f"\n  ✅ 複勝+加重モデルが最も過学習が少ない (Gap: {avg_gap_place_w:.1f}%)")


if __name__ == "__main__":
    main()
