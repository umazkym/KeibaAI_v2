# -*- coding: utf-8 -*-
"""
モデルタイプ別EV閾値分析

モデルタイプ:
1. V15 Binary（的中型）: 勝つ馬を予測
2. V4.4 Residual（穴馬検出型）: 過小評価された馬を発見
3. Ensemble（V15 + V4.4）: 両者の組み合わせ

仮説: 穴馬検出型モデルではEV閾値を上げるとROIが上昇する
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


def train_v15_model(train_df, valid_df, feature_cols):
    """V15 Binary（的中型）: is_winを予測"""
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


def train_v44_residual_model(train_df, valid_df, feature_cols, v15_preds_train, v15_preds_valid):
    """V4.4 Residual（穴馬検出型）: V15が過小評価した馬を発見"""
    # 残差ターゲット: 実際に勝った（is_win=1）がV15が低評価した馬
    y_train_actual = (train_df['finish_position'] == 1).astype(int)
    y_valid_actual = (valid_df['finish_position'] == 1).astype(int)
    
    # 残差 = 実際の結果(0/1) - V15予測
    residual_train = y_train_actual - v15_preds_train
    residual_valid = y_valid_actual - v15_preds_valid
    
    # 残差が正 = V15が過小評価 → 勝った馬をターゲット
    # LambdaRank形式で学習（残差をrelevanceスコアとして使用）
    groups_train = train_df.groupby('race_id').size().values
    groups_valid = valid_df.groupby('race_id').size().values
    
    # オッズ加重
    if 'win_odds' in train_df.columns:
        weights = np.log1p(train_df['win_odds'].fillna(1).values)
    else:
        weights = None
    
    params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 300, 'reg_alpha': 15.0, 'reg_lambda': 20.0,
        'random_state': 42,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    # LambdaRankにはrelevanceラベル（整数）が必要
    # 残差をスケーリングしてrelevance化
    # 勝ち馬でV15が低評価 → 高スコア (5)
    # 負け馬 → 低スコア (0)
    def to_relevance(residual, is_win):
        rel = np.zeros(len(residual))
        rel[is_win == 1] = 5  # 勝ち馬は基本5
        # V15の予測より良い結果ならボーナス（上限5に収める）
        # 残差が正（勝ったのに低評価）→ 重要
        rel = np.clip(rel + residual * 2, 0, 5).astype(int)
        return rel
    
    rel_train = to_relevance(residual_train.values, y_train_actual.values)
    rel_valid = to_relevance(residual_valid.values, y_valid_actual.values)
    
    train_ds = lgb.Dataset(X_train, rel_train, group=groups_train, weight=weights)
    valid_ds = lgb.Dataset(X_valid, rel_valid, group=groups_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def calculate_ev_roi_by_threshold(pred_df, returns_df, ev_thresholds, score_col='score', model_name=''):
    """EV閾値ごとのROIを計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    # Top1予測馬を抽出
    rank_col = f'rank_{score_col}'
    pred_df[rank_col] = pred_df.groupby('race_id')[score_col].rank(ascending=False, method='first')
    top1 = pred_df[pred_df[rank_col] == 1].copy()
    
    # 払戻データをマージ
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100
    
    # EV = オッズ × 予測確率（スコア）
    merged['ev'] = merged['win_odds'] * merged[score_col]
    
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
                'avg_odds_hit': avg_odds,
                'model': model_name
            })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("モデルタイプ別EV閾値分析")
    print("=" * 80)
    print("1. V15 Binary（的中型）: 勝つ馬を予測")
    print("2. V4.4 Residual（穴馬検出型）: 過小評価された馬を発見")
    print("3. Ensemble: 両者の組み合わせ")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 4年間Walk-forward検証（計算時間短縮のため）
    years = [2022, 2023, 2024, 2025]
    ev_thresholds = np.arange(0.5, 4.1, 0.1)
    
    all_results = []
    
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
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # 学習/検証分割
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000:
            continue
        
        # ========== V15 Binary（的中型）==========
        print("  V15 Binary 学習中...")
        v15_model = train_v15_model(train_sub, valid_sub, feature_cols)
        
        X_train = train_sub[feature_cols].fillna(0)
        X_valid = valid_sub[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        v15_preds_train = v15_model.predict(X_train)
        v15_preds_valid = v15_model.predict(X_valid)
        v15_preds_test = v15_model.predict(X_test)
        
        # ========== V4.4 Residual（穴馬検出型）==========
        print("  V4.4 Residual 学習中...")
        v44_model = train_v44_residual_model(train_sub, valid_sub, feature_cols, 
                                              v15_preds_train, v15_preds_valid)
        v44_preds_test = v44_model.predict(X_test)
        
        # スコアを正規化（0-1）
        v44_preds_test_norm = (v44_preds_test - v44_preds_test.min()) / (v44_preds_test.max() - v44_preds_test.min() + 1e-10)
        
        # ========== Ensemble ==========
        ens_preds_test = 0.5 * v15_preds_test + 0.5 * v44_preds_test_norm
        
        # 予測結果をDataFrameに
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df['score_v15'] = v15_preds_test
        pred_df['score_v44'] = v44_preds_test_norm
        pred_df['score_ens'] = ens_preds_test
        pred_df['year'] = year
        
        # 各モデルでEV閾値分析
        for score_col, model_name in [('score_v15', 'V15_Binary（的中型）'), 
                                       ('score_v44', 'V4.4_Residual（穴馬検出型）'),
                                       ('score_ens', 'Ensemble')]:
            result = calculate_ev_roi_by_threshold(pred_df, returns, ev_thresholds, score_col, model_name)
            result['year'] = year
            all_results.append(result)
        
        print(f"  年間レース数: {pred_df['race_id'].nunique():,}件")
    
    # 結果集計
    results_df = pd.concat(all_results, ignore_index=True)
    
    # モデル別・閾値別平均ROI
    print("\n" + "=" * 80)
    print("【モデル別 EV閾値 vs ROI サマリー（4年平均）】")
    print("=" * 80)
    
    for model in ['V15_Binary（的中型）', 'V4.4_Residual（穴馬検出型）', 'Ensemble']:
        model_data = results_df[results_df['model'] == model]
        summary = model_data.groupby('threshold').agg({
            'roi': ['mean', 'std'],
            'bet_count': 'mean',
            'hit_rate': 'mean'
        }).round(2)
        summary.columns = ['roi_mean', 'roi_std', 'avg_bet_count', 'avg_hit_rate']
        summary = summary.reset_index()
        
        print(f"\n■ {model}:")
        print(f"{'EV閾値':>7} | {'ROI':>8} | {'σ':>6} | {'ベット数':>8} | {'的中率':>7}")
        print("-" * 50)
        for _, row in summary.iterrows():
            if row['threshold'] in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]:
                mark = "★" if row['roi_mean'] >= 100 else ""
                print(f"{row['threshold']:>7.1f} | {row['roi_mean']:>6.1f}%{mark} | {row['roi_std']:>5.1f} | {row['avg_bet_count']:>8.0f} | {row['avg_hit_rate']:>6.2f}%")
        
        # ROI最大の閾値
        best = summary[summary['avg_bet_count'] >= 50].nlargest(1, 'roi_mean')
        if len(best) > 0:
            print(f"  → 最適閾値: EV >= {best.iloc[0]['threshold']:.1f} (ROI {best.iloc[0]['roi_mean']:.1f}%, ベット数 {best.iloc[0]['avg_bet_count']:.0f})")
    
    # グラフ作成
    print("\n" + "=" * 80)
    print("【グラフ出力】")
    print("=" * 80)
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    colors = {'V15_Binary（的中型）': 'blue', 'V4.4_Residual（穴馬検出型）': 'red', 'Ensemble': 'green'}
    
    for ax, model in zip(axes, ['V15_Binary（的中型）', 'V4.4_Residual（穴馬検出型）', 'Ensemble']):
        model_data = results_df[results_df['model'] == model]
        summary = model_data.groupby('threshold').agg({'roi': ['mean', 'std'], 'bet_count': 'mean'}).reset_index()
        summary.columns = ['threshold', 'roi_mean', 'roi_std', 'bet_count']
        
        ax.plot(summary['threshold'], summary['roi_mean'], 
                color=colors[model], linewidth=2, label=model)
        ax.fill_between(summary['threshold'], 
                        summary['roi_mean'] - summary['roi_std'],
                        summary['roi_mean'] + summary['roi_std'],
                        alpha=0.2, color=colors[model])
        ax.axhline(y=100, color='gray', linestyle='--', alpha=0.7, label='ROI 100%')
        
        # ベット数ラベル
        for _, row in summary.iterrows():
            if row['threshold'] in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]:
                ax.annotate(f"{int(row['bet_count'])}", 
                            (row['threshold'], row['roi_mean']), 
                            textcoords="offset points", xytext=(0,8), ha='center', fontsize=7)
        
        ax.set_xlabel('EV閾値（オッズ × 予測確率）', fontsize=10)
        ax.set_ylabel('ROI (%)', fontsize=10)
        ax.set_title(model, fontsize=11)
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 200)
        ax.set_xlim(0.5, 4.0)
    
    plt.tight_layout()
    output_path = project_root / "outputs/analysis/ev_threshold_model_comparison.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"グラフ保存: {output_path}")
    
    # CSV保存
    csv_path = project_root / "outputs/analysis/ev_threshold_model_comparison.csv"
    results_df.to_csv(csv_path, index=False)
    print(f"CSV保存: {csv_path}")
    
    # 結論
    print("\n" + "=" * 80)
    print("【結論】")
    print("=" * 80)
    
    # 各モデルでROI 100%超の閾値があるか確認
    for model in ['V15_Binary（的中型）', 'V4.4_Residual（穴馬検出型）', 'Ensemble']:
        model_data = results_df[results_df['model'] == model]
        summary = model_data.groupby('threshold')['roi'].mean().reset_index()
        roi_100_plus = summary[summary['roi'] >= 100]
        if len(roi_100_plus) > 0:
            print(f"\n■ {model}: ROI 100%超の閾値あり ★")
            print(f"  閾値: {roi_100_plus['threshold'].min():.1f} ~ {roi_100_plus['threshold'].max():.1f}")
        else:
            print(f"\n■ {model}: ROI 100%超の閾値なし")
            best = summary.nlargest(1, 'roi').iloc[0]
            print(f"  最高ROI: {best['roi']:.1f}% (閾値 {best['threshold']:.1f})")
    
    plt.show()
    print("\n分析完了")


if __name__ == "__main__":
    main()
