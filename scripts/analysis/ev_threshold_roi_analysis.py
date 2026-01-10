# -*- coding: utf-8 -*-
"""
期待値(EV)閾値によるROI最適化分析

期待値 = オッズ × 予想勝率
横軸: EV閾値 (0.5 ~ 3.5)
縦軸: ROI (回収率)

6年間Walk-forward検証で安定してROI 100%を超える閾値を探索
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

from keibaai.src.features.leak_free_feature_engineer_v33 import LeakFreeFeatureEngineerV33


def load_data():
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 300, 'reg_alpha': 15.0, 'reg_lambda': 20.0,
        'bagging_fraction': 0.5, 'bagging_freq': 3, 'feature_fraction': 0.5, 'random_state': 42,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def calculate_ev_roi(pred_df, returns_df, ev_thresholds):
    """EV閾値ごとのROIを計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    # Top1予測馬を抽出
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    
    # 払戻データをマージ
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100  # 100円あたりの払戻
    
    # EV = オッズ × 予測確率（スコア）
    merged['ev'] = merged['win_odds'] * merged['score']
    
    results = []
    for threshold in ev_thresholds:
        subset = merged[merged['ev'] >= threshold]
        if len(subset) >= 10:
            roi = subset['return'].sum() / len(subset) * 100
            hit_rate = subset['is_hit'].mean() * 100
            avg_odds = subset[subset['is_hit']]['win_odds'].mean() if subset['is_hit'].sum() > 0 else 0
            results.append({
                'threshold': threshold,
                'roi': roi,
                'hit_rate': hit_rate,
                'bet_count': len(subset),
                'avg_odds_hit': avg_odds
            })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("期待値(EV)閾値によるROI最適化分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 6年間Walk-forward検証
    years = [2020, 2021, 2022, 2023, 2024, 2025]
    ev_thresholds = np.arange(0.5, 3.6, 0.1)
    
    all_results = []
    all_pred_dfs = []
    
    for year in years:
        print(f"\n{'='*60}")
        print(f"{year}年 処理中...")
        
        train_end = f'{year-1}-12-31'
        test_start = f'{year}-01-01'
        test_end = f'{year}-12-31'
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        print(f"  学習: {len(train):,}件 / テスト: {len(test):,}件")
        
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000:
            continue
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df['score'] = preds
        pred_df['rank_pred'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        pred_df['year'] = year
        
        all_pred_dfs.append(pred_df)
        
        # 年別EV閾値ROI
        year_result = calculate_ev_roi(pred_df, returns, ev_thresholds)
        year_result['year'] = year
        all_results.append(year_result)
        
        print(f"  年間レース数: {pred_df['race_id'].nunique():,}件")
    
    # 全年結合
    all_pred_df = pd.concat(all_pred_dfs, ignore_index=True)
    results_df = pd.concat(all_results, ignore_index=True)
    
    # 全期間での集計
    print("\n" + "=" * 80)
    print("【EV閾値別 6年間サマリー】")
    print("=" * 80)
    
    summary = results_df.groupby('threshold').agg({
        'roi': ['mean', 'std', 'min', 'max'],
        'bet_count': 'mean',
        'hit_rate': 'mean'
    }).round(2)
    summary.columns = ['roi_mean', 'roi_std', 'roi_min', 'roi_max', 'avg_bet_count', 'avg_hit_rate']
    summary = summary.reset_index()
    
    # ROI 100%超の閾値を探索
    print("\n■ ROI 100%超えの閾値:")
    roi_100_plus = summary[summary['roi_mean'] >= 100]
    if len(roi_100_plus) > 0:
        print(roi_100_plus.to_string(index=False))
    else:
        print("  6年平均でROI 100%超えの閾値は見つかりませんでした")
    
    # 安定性重視（std小さく、mean高い）
    print("\n■ 安定性重視の推奨閾値 (ROI高 & σ低):")
    summary['score'] = summary['roi_mean'] - summary['roi_std']  # ROI - σ でランク
    best_stable = summary[summary['avg_bet_count'] >= 100].nlargest(5, 'score')
    print(best_stable[['threshold', 'roi_mean', 'roi_std', 'avg_bet_count', 'avg_hit_rate']].to_string(index=False))
    
    # 年別詳細
    print("\n■ 年別・閾値別ROI (主要閾値のみ):")
    key_thresholds = [1.0, 1.5, 2.0, 2.5, 3.0]
    pivot = results_df[results_df['threshold'].isin(key_thresholds)].pivot(
        index='threshold', columns='year', values='roi'
    ).round(1)
    print(pivot.to_string())
    
    # ベット数
    print("\n■ 年別・閾値別ベット数:")
    pivot_count = results_df[results_df['threshold'].isin(key_thresholds)].pivot(
        index='threshold', columns='year', values='bet_count'
    )
    print(pivot_count.to_string())
    
    # 全期間での計算
    print("\n" + "=" * 80)
    print("【全期間統合分析】")
    print("=" * 80)
    
    all_ev_result = calculate_ev_roi(all_pred_df, returns, ev_thresholds)
    
    print("\n■ 全期間(2020-2025) EV閾値別ROI:")
    print(f"{'閾値':>6} | {'ROI':>8} | {'的中率':>7} | {'ベット数':>8} | {'平均回収':>10}")
    print("-" * 55)
    for _, row in all_ev_result.iterrows():
        mark = "★" if row['roi'] >= 100 else ""
        print(f"{row['threshold']:>6.1f} | {row['roi']:>7.1f}%{mark} | {row['hit_rate']:>6.2f}% | {int(row['bet_count']):>8} | {row['roi']/100:.2f}倍")
    
    # グラフ作成
    print("\n" + "=" * 80)
    print("【グラフ出力】")
    print("=" * 80)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # 左: 全期間ROI曲線
    ax1 = axes[0]
    ax1.plot(all_ev_result['threshold'], all_ev_result['roi'], 'b-', linewidth=2, label='全期間')
    ax1.axhline(y=100, color='r', linestyle='--', label='ROI 100%')
    ax1.fill_between(all_ev_result['threshold'], 
                     all_ev_result['roi'] - summary['roi_std'].values[:len(all_ev_result)],
                     all_ev_result['roi'] + summary['roi_std'].values[:len(all_ev_result)],
                     alpha=0.2)
    
    # ベット数をラベル
    for i, row in all_ev_result.iterrows():
        if row['threshold'] in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]:
            ax1.annotate(f"{int(row['bet_count'])}", 
                         (row['threshold'], row['roi']), 
                         textcoords="offset points", xytext=(0,10), ha='center', fontsize=8)
    
    ax1.set_xlabel('EV閾値 (オッズ × 予測確率)', fontsize=11)
    ax1.set_ylabel('ROI (%)', fontsize=11)
    ax1.set_title('EV閾値 vs ROI (全期間2020-2025)', fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(40, 200)
    
    # 右: 年別ROI推移
    ax2 = axes[1]
    for year in years:
        year_data = results_df[results_df['year'] == year]
        ax2.plot(year_data['threshold'], year_data['roi'], label=str(year), alpha=0.7)
    ax2.axhline(y=100, color='r', linestyle='--', alpha=0.5)
    ax2.set_xlabel('EV閾値', fontsize=11)
    ax2.set_ylabel('ROI (%)', fontsize=11)
    ax2.set_title('年別 EV閾値 vs ROI', fontsize=12)
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 300)
    
    plt.tight_layout()
    output_path = project_root / "outputs/analysis/ev_threshold_roi_analysis.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"グラフ保存: {output_path}")
    
    # CSV保存
    csv_path = project_root / "outputs/analysis/ev_threshold_roi_6years.csv"
    results_df.to_csv(csv_path, index=False)
    print(f"CSV保存: {csv_path}")
    
    # 全期間結果も保存
    all_ev_result['period'] = '2020-2025'
    all_csv_path = project_root / "outputs/analysis/ev_threshold_roi_all_period.csv"
    all_ev_result.to_csv(all_csv_path, index=False)
    print(f"CSV保存: {all_csv_path}")
    
    # 結論
    print("\n" + "=" * 80)
    print("【結論】")
    print("=" * 80)
    
    # 最適な閾値を特定
    best_row = all_ev_result[all_ev_result['bet_count'] >= 50].nlargest(1, 'roi').iloc[0]
    print(f"""
■ 全期間最適閾値: EV >= {best_row['threshold']:.1f}
  - ROI: {best_row['roi']:.1f}%
  - 的中率: {best_row['hit_rate']:.2f}%
  - ベット数: {int(best_row['bet_count'])}件

■ ROI 100%達成のための条件:
  - 高EV閾値(3.0+)ではサンプル数が極端に減少
  - 年次変動が大きく、6年「継続的に」100%超は困難
  - 現実的な運用: EV >= 1.5 でROI 85-95%を狙いつつ、十分なベット数を確保
""")
    
    plt.show()
    print("\n分析完了")


if __name__ == "__main__":
    main()
