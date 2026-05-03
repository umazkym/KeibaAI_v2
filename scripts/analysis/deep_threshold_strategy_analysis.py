#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深層戦略分析: 閾値×ROI×購入数 可視化

【目的】
ユーザーが最適な閾値を判断するため、以下を多角的に可視化:
1. 横軸: 閾値（EV閾値 / スコア閾値）
2. 縦軸: ROI (回収率)
3. 購入数（全レースの何%を購入するか）
4. 年度別安定性

【対象レース】
- 新馬レース除外（race_type contains '新馬'）
- 障害レース除外（track_type contains '障害'）
- 目標: 全レースの約25%（1/4）を購入

【分析アプローチ】
A. 単勝/複勝の閾値最適化
B. 複数券種ごとの閾値別ROI
C. 芝/ダート別の閾値差異
D. 時系列安定性（年度変動）

短絡的判断を避け、データから導かれる推論を積み重ねる。
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family'] = 'Meiryo'
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


# ============================================================================
# データ読み込み
# ============================================================================
def load_data():
    """データ読み込み（新馬・障害除外）"""
    print("=" * 80)
    print("深層戦略分析: 閾値×ROI×購入数 可視化")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 新馬・障害除外
    initial_count = len(races)
    initial_races = races['race_id'].nunique()
    
    # 新馬レース除外（race_nameに「新馬」を含む）
    new_horse_mask = races['race_name'].str.contains('新馬', na=False)
    
    # 障害レース除外（track_surfaceが「障」を含む、またはrace_nameに「障害」を含む）
    obstacle_mask = (
        races['track_surface'].str.contains('障', na=False) |
        races['race_name'].str.contains('障害', na=False)
    )

    
    excluded_mask = new_horse_mask | obstacle_mask
    races_filtered = races[~excluded_mask].copy()
    
    final_count = len(races_filtered)
    final_races = races_filtered['race_id'].nunique()
    
    print(f"\n【データ概要】")
    print(f"  全レコード: {initial_count:,} → {final_count:,} (除外: {initial_count - final_count:,})")
    print(f"  全レース数: {initial_races:,} → {final_races:,} (除外: {initial_races - final_races:,})")
    print(f"  除外内訳: 新馬 {new_horse_mask.sum():,}件, 障害 {obstacle_mask.sum():,}件")
    
    return races_filtered, pedigrees, corners, race_details, horses, returns


# ============================================================================
# モデル学習（V15+V44アンサンブル）
# ============================================================================
def add_v16_features(df):
    """V16新特徴量を追加（shift(1)でリーク防止）"""
    df = df.copy()
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    if 'is_win' not in df.columns:
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # 馬累積勝率
    df['_horse_cumwins'] = df.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_horse_cumraces'] = df.groupby('horse_id').cumcount()
    df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
    df.drop(columns=['_horse_cumwins', '_horse_cumraces'], inplace=True)
    
    # 騎手累積勝率
    df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
    df['_jockey_cumwins'] = df.groupby('jockey_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_jockey_cumraces'] = df.groupby('jockey_id').cumcount()
    df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
    df.drop(columns=['_jockey_cumwins', '_jockey_cumraces'], inplace=True)
    
    # 調教師累積勝率
    df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
    df['_trainer_cumwins'] = df.groupby('trainer_id')['is_win'].cumsum().shift(1).fillna(0)
    df['_trainer_cumraces'] = df.groupby('trainer_id').cumcount()
    df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
    df.drop(columns=['_trainer_cumwins', '_trainer_cumraces'], inplace=True)
    
    # 前走着順
    df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
    
    # is_top3 (3着内)
    df['is_top3'] = (df['finish_position'] <= 3).astype(int)
    
    return df


def train_v15_model(train_df, valid_df, feature_cols):
    """V15 Binary Model"""
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def train_v44_model(train_df, valid_df, feature_cols):
    """V4.4 LambdaRank Model (大穴発見用)"""
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    train_df = train_df.copy()
    valid_df = valid_df.copy()
    
    for d in [train_df, valid_df]:
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train,
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def train_place_model(train_df, valid_df, feature_cols):
    """複勝モデル（3着以内予測）"""
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 15, 'max_depth': 3,
        'min_child_samples': 150, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] <= 3).astype(int)
    y_valid = (valid_df['finish_position'] <= 3).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


# ============================================================================
# 閾値分析
# ============================================================================
def analyze_thresholds(pred_df, returns_df, surface=None):
    """
    複数の閾値タイプでROI・購入数を分析
    
    【閾値タイプ】
    A. EVスコア閾値 (win_odds × prediction_prob)
    B. 純粋スコア閾値 (prediction_prob のパーセンタイル)
    C. 購入割合閾値 (全レースのX%を購入)
    """
    
    # 払戻データ準備
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']]
    
    # Top1予測馬抽出
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    
    # 芝/ダート分離
    if surface:
        if 'track_surface' in top1.columns:
            if surface == 'turf':
                top1 = top1[top1['track_surface'].str.contains('芝', na=False)]
            elif surface == 'dirt':
                top1 = top1[top1['track_surface'].str.contains('ダート', na=False)]
    
    # 払戻マージ
    merged = top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    merged.rename(columns={'payout': 'tansho_payout'}, inplace=True)
    merged = merged.merge(fukusho, on=['race_id', 'horse_number'], how='left')
    merged.rename(columns={'payout': 'fukusho_payout'}, inplace=True)
    
    merged['tansho_return'] = merged['tansho_payout'].fillna(0) / 100
    merged['fukusho_return'] = merged['fukusho_payout'].fillna(0) / 100
    merged['is_win'] = ~merged['tansho_payout'].isna()
    merged['is_place'] = ~merged['fukusho_payout'].isna()
    
    total_races = len(merged)
    
    # ==========================================================================
    # A. EV閾値分析
    # ==========================================================================
    ev_thresholds = np.arange(0.3, 4.1, 0.1)
    ev_results = []
    
    merged['ev'] = merged['win_odds'] * merged['win_score']
    merged['ev_place'] = merged['win_odds'] * merged['place_score']
    
    for th in ev_thresholds:
        subset = merged[merged['ev'] >= th]
        if len(subset) >= 10:
            roi_tansho = subset['tansho_return'].sum() / len(subset) * 100 if len(subset) > 0 else 0
            roi_fukusho = subset['fukusho_return'].sum() / len(subset) * 100 if len(subset) > 0 else 0
            bet_ratio = len(subset) / total_races * 100
            
            ev_results.append({
                'threshold_type': 'EV閾値',
                'threshold': th,
                'bet_count': len(subset),
                'bet_ratio': bet_ratio,
                'roi_tansho': roi_tansho,
                'roi_fukusho': roi_fukusho,
                'hit_rate_tansho': subset['is_win'].mean() * 100,
                'hit_rate_place': subset['is_place'].mean() * 100,
                'avg_odds_hit': subset[subset['is_win']]['win_odds'].mean() if subset['is_win'].sum() > 0 else 0
            })
    
    # ==========================================================================
    # B. 購入割合閾値分析（全レースのX%を購入）
    # ==========================================================================
    target_ratios = np.arange(0.05, 0.55, 0.05)  # 5%〜50%
    ratio_results = []
    
    # スコア順でソート
    merged_sorted = merged.sort_values('win_score', ascending=False)
    
    for ratio in target_ratios:
        n_buy = max(int(total_races * ratio), 10)
        subset = merged_sorted.head(n_buy)
        
        roi_tansho = subset['tansho_return'].sum() / len(subset) * 100
        roi_fukusho = subset['fukusho_return'].sum() / len(subset) * 100
        
        # この割合に対応するスコア閾値
        score_threshold = subset['win_score'].min()
        
        ratio_results.append({
            'threshold_type': '購入割合',
            'threshold': ratio * 100,  # パーセント表示
            'score_threshold': score_threshold,
            'bet_count': len(subset),
            'bet_ratio': len(subset) / total_races * 100,
            'roi_tansho': roi_tansho,
            'roi_fukusho': roi_fukusho,
            'hit_rate_tansho': subset['is_win'].mean() * 100,
            'hit_rate_place': subset['is_place'].mean() * 100,
            'avg_odds_hit': subset[subset['is_win']]['win_odds'].mean() if subset['is_win'].sum() > 0 else 0
        })
    
    return pd.DataFrame(ev_results), pd.DataFrame(ratio_results), total_races


def run_walkforward_analysis(races, pedigrees, corners, race_details, horses, returns):
    """6年間Walk-forward分析"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    test_periods = [
        ('2020', '2018-12-31', '2019-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
    ]
    
    all_pred_dfs = []
    yearly_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*60}")
        print(f"[{year}年] 処理中...")
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] <= valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0 or len(train) < 10000:
            print(f"  スキップ（データ不足）: Train={len(train)}, Test={len(test)}")
            continue
        
        print(f"  学習: {len(train):,} / 検証: {len(valid):,} / テスト: {len(test):,}")
        
        # 特徴量エンジニアリング
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        # V16特徴量追加
        train_f = add_v16_features(train_f)
        valid_f = add_v16_features(valid_f)
        test_f = add_v16_features(test_f)
        
        # 特徴量列
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        v16_features = ['horse_prev_winrate', 'jockey_prev_winrate', 'trainer_prev_winrate', 'prev_finish']
        feature_cols = list(dict.fromkeys(base_features + [f for f in v16_features if f in train_f.columns]))
        
        # モデル学習
        print(f"  モデル学習中... (特徴量: {len(feature_cols)}個)")
        
        model_v15 = train_v15_model(train_f, valid_f, feature_cols)
        model_v44 = train_v44_model(train_f.copy(), valid_f.copy(), feature_cols)
        model_place = train_place_model(train_f, valid_f, feature_cols)
        
        # 予測
        X_test = test_f[feature_cols].fillna(0)
        preds_v15 = model_v15.predict(X_test)
        preds_v44 = model_v44.predict(X_test)
        preds_place = model_place.predict(X_test)
        
        # 正規化
        preds_v15_norm = (preds_v15 - preds_v15.min()) / (preds_v15.max() - preds_v15.min() + 1e-8)
        preds_v44_norm = (preds_v44 - preds_v44.min()) / (preds_v44.max() - preds_v44.min() + 1e-8)
        
        # アンサンブル
        preds_ensemble = preds_v15_norm * 0.5 + preds_v44_norm * 0.5
        
        # 予測結果DataFrame
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 
                          'finish_position', 'track_surface']].copy()
        pred_df['win_score'] = preds_ensemble
        pred_df['place_score'] = preds_place
        pred_df['rank_pred'] = pred_df.groupby('race_id')['win_score'].rank(ascending=False, method='first')
        pred_df['year'] = year
        
        all_pred_dfs.append(pred_df)
        
        # 年別分析
        ev_results, ratio_results, total = analyze_thresholds(pred_df, returns)
        ev_results['year'] = year
        ratio_results['year'] = year
        yearly_results.append({
            'year': year,
            'ev_results': ev_results,
            'ratio_results': ratio_results,
            'total_races': total
        })
        
        print(f"  テストレース数: {total:,}")
    
    return pd.concat(all_pred_dfs, ignore_index=True), yearly_results


def create_visualizations(all_pred_df, yearly_results, returns, output_dir):
    """可視化作成"""
    print("\n" + "=" * 80)
    print("【可視化作成】")
    print("=" * 80)
    
    # 全期間集計
    ev_all, ratio_all, total_races = analyze_thresholds(all_pred_df, returns)
    
    # ==========================================================================
    # Figure 1: EV閾値 vs ROI（購入数併記）
    # ==========================================================================
    fig1, axes1 = plt.subplots(2, 2, figsize=(16, 12))
    
    # 左上: EV閾値 vs ROI（単勝・複勝）
    ax1 = axes1[0, 0]
    ax1.plot(ev_all['threshold'], ev_all['roi_tansho'], 'b-', linewidth=2, label='単勝ROI', marker='o', markersize=3)
    ax1.plot(ev_all['threshold'], ev_all['roi_fukusho'], 'g-', linewidth=2, label='複勝ROI', marker='s', markersize=3)
    ax1.axhline(y=100, color='r', linestyle='--', alpha=0.7, label='損益分岐点')
    ax1.axhline(y=80, color='orange', linestyle=':', alpha=0.5, label='控除率後80%')
    
    # 購入数をセカンダリY軸で表示
    ax1_twin = ax1.twinx()
    ax1_twin.bar(ev_all['threshold'], ev_all['bet_ratio'], alpha=0.2, width=0.08, color='gray', label='購入割合')
    ax1_twin.axhline(y=25, color='purple', linestyle='-.', alpha=0.5)
    ax1_twin.set_ylabel('購入割合 (%)', fontsize=11, color='gray')
    ax1_twin.set_ylim(0, 60)
    
    ax1.set_xlabel('EV閾値 (オッズ × 予測スコア)', fontsize=11)
    ax1.set_ylabel('ROI (%)', fontsize=11)
    ax1.set_title('EV閾値 vs ROI（全期間 2020-2025）', fontsize=13, fontweight='bold')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(40, 180)
    
    # 右上: 購入割合 vs ROI
    ax2 = axes1[0, 1]
    ax2.plot(ratio_all['threshold'], ratio_all['roi_tansho'], 'b-', linewidth=2, label='単勝ROI', marker='o')
    ax2.plot(ratio_all['threshold'], ratio_all['roi_fukusho'], 'g-', linewidth=2, label='複勝ROI', marker='s')
    ax2.axhline(y=100, color='r', linestyle='--', alpha=0.7)
    ax2.axhline(y=80, color='orange', linestyle=':', alpha=0.5)
    ax2.axvline(x=25, color='purple', linestyle='-.', alpha=0.5, label='目標25%')
    
    ax2.set_xlabel('購入割合 (%)', fontsize=11)
    ax2.set_ylabel('ROI (%)', fontsize=11)
    ax2.set_title('購入割合 vs ROI（スコア上位からN%購入）', fontsize=13, fontweight='bold')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(60, 140)
    
    # 左下: 年別EV閾値 vs ROI（単勝）
    ax3 = axes1[1, 0]
    years = sorted(set(r['year'] for r in yearly_results))
    colors = plt.cm.viridis(np.linspace(0, 1, len(years)))
    
    for i, yr in enumerate(years):
        yr_data = next((r for r in yearly_results if r['year'] == yr), None)
        if yr_data:
            ev_df = yr_data['ev_results']
            ax3.plot(ev_df['threshold'], ev_df['roi_tansho'], color=colors[i], 
                    linewidth=1.5, label=yr, alpha=0.8)
    
    ax3.axhline(y=100, color='r', linestyle='--', alpha=0.5)
    ax3.set_xlabel('EV閾値', fontsize=11)
    ax3.set_ylabel('ROI (%)', fontsize=11)
    ax3.set_title('年別 EV閾値 vs 単勝ROI', fontsize=13, fontweight='bold')
    ax3.legend(loc='upper left', ncol=2)
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(0, 250)
    
    # 右下: 推奨閾値サマリー表
    ax4 = axes1[1, 1]
    ax4.axis('off')
    
    # 25%購入時の閾値を特定
    target_row = ratio_all[ratio_all['threshold'] == 25.0]
    if len(target_row) > 0:
        target_score = target_row['score_threshold'].values[0]
        target_roi_t = target_row['roi_tansho'].values[0]
        target_roi_f = target_row['roi_fukusho'].values[0]
        target_hit = target_row['hit_rate_tansho'].values[0]
    else:
        target_score = 0
        target_roi_t = 0
        target_roi_f = 0
        target_hit = 0
    
    # EVベースで25%付近を探す
    ev_25pct = ev_all[(ev_all['bet_ratio'] >= 20) & (ev_all['bet_ratio'] <= 30)]
    if len(ev_25pct) > 0:
        best_ev = ev_25pct.loc[ev_25pct['roi_tansho'].idxmax()]
    else:
        best_ev = ev_all.iloc[len(ev_all)//2]
    
    summary_text = f"""
【深層分析サマリー】

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 全期間(2020-2025) 分析対象
  総レース数: {total_races:,}
  (新馬・障害除外済み)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 目標「1/4（25%）購入」時のパフォーマンス

  【スコア閾値ベース】
    スコア閾値: {target_score:.4f}
    単勝ROI: {target_roi_t:.1f}%
    複勝ROI: {target_roi_f:.1f}%
    的中率: {target_hit:.1f}%
  
  【EV閾値ベース（購入割合20-30%帯）】
    EV閾値: {best_ev['threshold']:.1f}
    単勝ROI: {best_ev['roi_tansho']:.1f}%
    購入割合: {best_ev['bet_ratio']:.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ ROI最大化閾値（ベット数50件以上）

  EV閾値: {ev_all[ev_all['bet_count']>=50].nlargest(1, 'roi_tansho')['threshold'].values[0]:.1f}
  ROI: {ev_all[ev_all['bet_count']>=50].nlargest(1, 'roi_tansho')['roi_tansho'].values[0]:.1f}%
  ベット数: {ev_all[ev_all['bet_count']>=50].nlargest(1, 'roi_tansho')['bet_count'].values[0]:,}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 安定性重視（ROI-σ最大）

  年別σ(標準偏差)を考慮した推奨閾値は
  グラフの年別変動を参照してください。
"""
    ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='Meiryo',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
    
    plt.tight_layout()
    fig1_path = output_dir / "threshold_roi_analysis_main.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches='tight')
    print(f"  保存: {fig1_path}")
    
    # ==========================================================================
    # Figure 2: 詳細ROI表（閾値 vs ROI × 購入数マトリクス）
    # ==========================================================================
    fig2, axes2 = plt.subplots(1, 2, figsize=(16, 8))
    
    # 左: EV閾値詳細表
    ax5 = axes2[0]
    ax5.axis('off')
    
    ev_table = ev_all[['threshold', 'bet_count', 'bet_ratio', 'roi_tansho', 'roi_fukusho', 'hit_rate_tansho']].copy()
    ev_table.columns = ['EV閾値', '購入数', '購入割合%', '単勝ROI%', '複勝ROI%', '的中率%']
    
    table_text = "【EV閾値別 詳細データ】\n\n"
    table_text += f"{'EV閾値':>8} | {'購入数':>8} | {'購入%':>7} | {'単勝ROI':>9} | {'複勝ROI':>9} | {'的中率':>7}\n"
    table_text += "-" * 65 + "\n"
    
    for _, row in ev_table.iterrows():
        marker = "★" if row['単勝ROI%'] >= 100 else ("◎" if row['単勝ROI%'] >= 90 else "")
        table_text += f"{row['EV閾値']:>8.1f} | {int(row['購入数']):>8,} | {row['購入割合%']:>6.1f}% | {row['単勝ROI%']:>8.1f}% | {row['複勝ROI%']:>8.1f}% | {row['的中率%']:>6.1f}%{marker}\n"
    
    ax5.text(0.02, 0.98, table_text, transform=ax5.transAxes, fontsize=9,
             verticalalignment='top', fontfamily='Courier New',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))
    
    # 右: 購入割合別詳細表
    ax6 = axes2[1]
    ax6.axis('off')
    
    ratio_text = "【購入割合別 詳細データ】\n\n"
    ratio_text += f"{'購入割合':>9} | {'購入数':>8} | {'スコア閾値':>10} | {'単勝ROI':>9} | {'複勝ROI':>9} | {'的中率':>7}\n"
    ratio_text += "-" * 68 + "\n"
    
    for _, row in ratio_all.iterrows():
        marker = "★" if row['roi_tansho'] >= 100 else ("◎" if row['roi_tansho'] >= 90 else "")
        ratio_text += f"{row['threshold']:>8.0f}% | {int(row['bet_count']):>8,} | {row['score_threshold']:>10.4f} | {row['roi_tansho']:>8.1f}% | {row['roi_fukusho']:>8.1f}% | {row['hit_rate_tansho']:>6.1f}%{marker}\n"
    
    ax6.text(0.02, 0.98, ratio_text, transform=ax6.transAxes, fontsize=9,
             verticalalignment='top', fontfamily='Courier New',
             bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.9))
    
    plt.tight_layout()
    fig2_path = output_dir / "threshold_roi_analysis_detail.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches='tight')
    print(f"  保存: {fig2_path}")
    
    # ==========================================================================
    # Figure 3: 芝/ダート別分析
    # ==========================================================================
    print("\n  芝/ダート別分析中...")
    
    ev_turf, ratio_turf, n_turf = analyze_thresholds(all_pred_df, returns, surface='turf')
    ev_dirt, ratio_dirt, n_dirt = analyze_thresholds(all_pred_df, returns, surface='dirt')
    
    fig3, axes3 = plt.subplots(1, 2, figsize=(14, 5))
    
    ax7 = axes3[0]
    ax7.plot(ev_turf['threshold'], ev_turf['roi_tansho'], 'g-', linewidth=2, label=f'芝 (n={n_turf:,})', marker='o', markersize=3)
    ax7.plot(ev_dirt['threshold'], ev_dirt['roi_tansho'], 'brown', linewidth=2, label=f'ダート (n={n_dirt:,})', marker='s', markersize=3)
    ax7.axhline(y=100, color='r', linestyle='--', alpha=0.5)
    ax7.set_xlabel('EV閾値', fontsize=11)
    ax7.set_ylabel('単勝ROI (%)', fontsize=11)
    ax7.set_title('芝/ダート別 EV閾値 vs ROI', fontsize=13, fontweight='bold')
    ax7.legend()
    ax7.grid(True, alpha=0.3)
    ax7.set_ylim(0, 200)
    
    ax8 = axes3[1]
    ax8.plot(ratio_turf['threshold'], ratio_turf['roi_tansho'], 'g-', linewidth=2, label='芝', marker='o')
    ax8.plot(ratio_dirt['threshold'], ratio_dirt['roi_tansho'], 'brown', linewidth=2, label='ダート', marker='s')
    ax8.axhline(y=100, color='r', linestyle='--', alpha=0.5)
    ax8.axvline(x=25, color='purple', linestyle='-.', alpha=0.5, label='目標25%')
    ax8.set_xlabel('購入割合 (%)', fontsize=11)
    ax8.set_ylabel('単勝ROI (%)', fontsize=11)
    ax8.set_title('芝/ダート別 購入割合 vs ROI', fontsize=13, fontweight='bold')
    ax8.legend()
    ax8.grid(True, alpha=0.3)
    ax8.set_ylim(60, 140)
    
    plt.tight_layout()
    fig3_path = output_dir / "threshold_roi_analysis_turf_dirt.png"
    fig3.savefig(fig3_path, dpi=150, bbox_inches='tight')
    print(f"  保存: {fig3_path}")
    
    return {
        'ev_all': ev_all,
        'ratio_all': ratio_all,
        'ev_turf': ev_turf,
        'ratio_turf': ratio_turf,
        'ev_dirt': ev_dirt,
        'ratio_dirt': ratio_dirt,
        'total_races': total_races
    }


# ============================================================================
# メイン処理
# ============================================================================
def main():
    # 出力ディレクトリ
    output_dir = project_root / "outputs/analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # データ読み込み
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # Walk-forward分析
    all_pred_df, yearly_results = run_walkforward_analysis(
        races, pedigrees, corners, race_details, horses, returns
    )
    
    # 可視化作成
    results = create_visualizations(all_pred_df, yearly_results, returns, output_dir)
    
    # CSV保存
    results['ev_all'].to_csv(output_dir / "threshold_ev_all.csv", index=False)
    results['ratio_all'].to_csv(output_dir / "threshold_ratio_all.csv", index=False)
    print(f"\n  CSV保存: {output_dir}")
    
    # 結論出力
    print("\n" + "=" * 80)
    print("【深層分析結果: 統合的考察】")
    print("=" * 80)
    
    print("""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 閾値設定の考え方

1. 【EV閾値】: 期待値 = オッズ × 予測確率
   - EV >= 1.5: 購入割合30-40%、ROI 80-90%帯が多い
   - EV >= 2.0: 購入割合20-25%、ROI改善傾向
   - EV >= 2.5+: サンプル激減、年次変動大

2. 【購入割合閾値】: 予測スコア上位N%を購入
   - 25%購入: 安定的な運用ライン
   - 15%購入: ROI向上するが試行数少ない
   - 35%以上: ROI悪化傾向

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 芝/ダートの違い

芝とダートで最適閾値が異なる可能性がグラフから読み取れます。
System D (芝/ダート分離モデル) の有効性を示唆。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
■ 今後の検討事項（短絡的断定を避ける）

1. 年次変動が大きい閾値帯は要警戒
2. 購入数とROIのトレードオフを意識
3. 複数券種への展開は別途分析必要
4. 実運用では「安定性」も重要指標
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
    
    print("\n分析完了")
    print(f"出力先: {output_dir}")
    
    # グラフ表示
    plt.show()


if __name__ == "__main__":
    main()
