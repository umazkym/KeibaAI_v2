# -*- coding: utf-8 -*-
"""
マーケットニュートラルモデルによるEV閾値分析

【核心】
オッズ・人気等の市場情報を特徴量から完全に除外し、
「市場が織り込んでいない情報」のみで勝率を推定する。

これにより、EV = 独立勝率 × オッズ が機能するかを検証。
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


def get_market_neutral_features(all_features):
    """
    市場情報（オッズ・人気関連）を除外した特徴量リストを返す
    """
    # 除外するキーワード
    exclude_keywords = [
        'odds', 'popularity', 'favorite', 'fav_', 
        'pop_', 'market', 'implied', 'rank_odds',
        'ninki', 'baiu', 'odds_', '_odds', 'popularity_',
    ]
    
    market_neutral = []
    excluded = []
    
    for col in all_features:
        col_lower = col.lower()
        is_market_related = any(kw in col_lower for kw in exclude_keywords)
        
        if is_market_related:
            excluded.append(col)
        else:
            market_neutral.append(col)
    
    return market_neutral, excluded


def train_model(train_df, valid_df, feature_cols, calibrate=False):
    """
    モデル学習（確率キャリブレーション付き）
    """
    params = {
        'objective': 'binary', 'metric': 'binary_logloss', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 16, 'max_depth': 4,
        'min_child_samples': 200, 'reg_alpha': 10.0, 'reg_lambda': 15.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6, 
        'random_state': 42,
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


def analyze_calibration(preds, y_true, name):
    """
    確率キャリブレーションの品質を分析
    """
    from sklearn.calibration import calibration_curve
    
    # 10ビンでキャリブレーション曲線を計算
    prob_true, prob_pred = calibration_curve(y_true, preds, n_bins=10, strategy='uniform')
    
    # Brier Score
    brier = np.mean((preds - y_true) ** 2)
    
    # ECE (Expected Calibration Error)
    bin_edges = np.linspace(0, 1, 11)
    ece = 0
    for i in range(10):
        mask = (preds >= bin_edges[i]) & (preds < bin_edges[i+1])
        if mask.sum() > 0:
            acc = y_true[mask].mean()
            conf = preds[mask].mean()
            ece += mask.sum() * np.abs(acc - conf)
    ece /= len(preds)
    
    return {
        'name': name,
        'brier_score': brier,
        'ece': ece,
        'prob_true': prob_true,
        'prob_pred': prob_pred
    }


def calculate_ev_roi(pred_df, returns_df, ev_thresholds, model_name):
    """EV閾値ごとのROIを計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    pred_df = pred_df.copy()
    pred_df['rank_pred'] = pred_df.groupby('race_id')['prob'].rank(ascending=False, method='first')
    
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    
    merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                        on=['race_id', 'horse_number'], how='left')
    merged['is_hit'] = ~merged['payout'].isna()
    merged['return'] = merged['payout'].fillna(0) / 100
    
    # EV = 独立勝率 × オッズ
    merged['ev'] = merged['prob'] * merged['win_odds']
    
    results = []
    for threshold in ev_thresholds:
        subset = merged[merged['ev'] >= threshold]
        if len(subset) >= 10:
            roi = subset['return'].sum() / len(subset) * 100
            hit_rate = subset['is_hit'].mean() * 100
            avg_prob = subset['prob'].mean()
            avg_odds = subset['win_odds'].mean()
            results.append({
                'threshold': threshold,
                'roi': roi,
                'hit_rate': hit_rate,
                'bet_count': len(subset),
                'avg_prob': avg_prob,
                'avg_odds': avg_odds,
                'model': model_name
            })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("マーケットニュートラルモデルによるEV閾値分析")
    print("=" * 80)
    print("【目的】オッズ情報を除外し、市場と独立した勝率推定でEVを計算")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 4年間Walk-forward検証
    years = [2022, 2023, 2024, 2025]
    ev_thresholds = np.arange(0.5, 4.1, 0.1)
    
    all_results_standard = []
    all_results_neutral = []
    all_calibration = {'standard': [], 'neutral': []}
    
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
        
        all_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # マーケットニュートラル特徴量を抽出
        neutral_features, excluded_features = get_market_neutral_features(all_features)
        
        if year == years[0]:
            print(f"\n  【特徴量分析】")
            print(f"  全特徴量: {len(all_features)}個")
            print(f"  マーケットニュートラル: {len(neutral_features)}個")
            print(f"  除外（市場関連）: {len(excluded_features)}個")
            print(f"  除外例: {excluded_features[:5]}")
        
        # 学習/検証分割
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000:
            continue
        
        # ========== 標準モデル（全特徴量）==========
        print(f"  標準モデル学習中...")
        model_std = train_model(train_sub, valid_sub, all_features)
        
        X_valid_std = valid_sub[all_features].fillna(0)
        X_test_std = test_f[all_features].fillna(0)
        preds_valid_std = model_std.predict(X_valid_std)
        preds_test_std = model_std.predict(X_test_std)
        
        # ========== マーケットニュートラルモデル ==========
        print(f"  マーケットニュートラルモデル学習中...")
        model_neutral = train_model(train_sub, valid_sub, neutral_features)
        
        X_valid_neutral = valid_sub[neutral_features].fillna(0)
        X_test_neutral = test_f[neutral_features].fillna(0)
        preds_valid_neutral = model_neutral.predict(X_valid_neutral)
        preds_test_neutral = model_neutral.predict(X_test_neutral)
        
        # キャリブレーション分析
        y_valid = (valid_sub['finish_position'] == 1).astype(int).values
        calib_std = analyze_calibration(preds_valid_std, y_valid, '標準モデル')
        calib_neutral = analyze_calibration(preds_valid_neutral, y_valid, 'ニュートラル')
        all_calibration['standard'].append(calib_std)
        all_calibration['neutral'].append(calib_neutral)
        
        print(f"  Brier Score - 標準: {calib_std['brier_score']:.4f}, ニュートラル: {calib_neutral['brier_score']:.4f}")
        
        # 予測結果をDataFrameに
        pred_df_std = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df_std['prob'] = preds_test_std
        pred_df_std['year'] = year
        
        pred_df_neutral = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df_neutral['prob'] = preds_test_neutral
        pred_df_neutral['year'] = year
        
        # EV閾値分析
        result_std = calculate_ev_roi(pred_df_std, returns, ev_thresholds, '標準モデル')
        result_neutral = calculate_ev_roi(pred_df_neutral, returns, ev_thresholds, 'マーケットニュートラル')
        
        result_std['year'] = year
        result_neutral['year'] = year
        
        all_results_standard.append(result_std)
        all_results_neutral.append(result_neutral)
        
        print(f"  年間レース数: {pred_df_std['race_id'].nunique():,}件")
    
    # 結果統合
    results_std = pd.concat(all_results_standard, ignore_index=True)
    results_neutral = pd.concat(all_results_neutral, ignore_index=True)
    
    # ========== 比較分析 ==========
    print("\n" + "=" * 80)
    print("【標準モデル vs マーケットニュートラルモデル EV閾値比較】")
    print("=" * 80)
    
    # 4年平均
    summary_std = results_std.groupby('threshold').agg({
        'roi': ['mean', 'std'],
        'bet_count': 'mean',
        'hit_rate': 'mean',
        'avg_prob': 'mean',
        'avg_odds': 'mean'
    }).round(2)
    summary_std.columns = ['roi_mean', 'roi_std', 'bet_count', 'hit_rate', 'avg_prob', 'avg_odds']
    
    summary_neutral = results_neutral.groupby('threshold').agg({
        'roi': ['mean', 'std'],
        'bet_count': 'mean',
        'hit_rate': 'mean',
        'avg_prob': 'mean',
        'avg_odds': 'mean'
    }).round(2)
    summary_neutral.columns = ['roi_mean', 'roi_std', 'bet_count', 'hit_rate', 'avg_prob', 'avg_odds']
    
    print("\n■ 標準モデル（オッズ含む）:")
    print(f"{'EV閾値':>7} | {'ROI':>7} | {'σ':>5} | {'件数':>6} | {'的中率':>6} | {'平均確率':>7} | {'平均オッズ':>8}")
    print("-" * 65)
    for th in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        if th in summary_std.index:
            r = summary_std.loc[th]
            mark = "★" if r['roi_mean'] >= 100 else ""
            print(f"{th:>7.1f} | {r['roi_mean']:>6.1f}%{mark} | {r['roi_std']:>5.1f} | {r['bet_count']:>6.0f} | {r['hit_rate']:>5.2f}% | {r['avg_prob']:>6.3f} | {r['avg_odds']:>7.1f}倍")
    
    print("\n■ マーケットニュートラルモデル（オッズ除外）:")
    print(f"{'EV閾値':>7} | {'ROI':>7} | {'σ':>5} | {'件数':>6} | {'的中率':>6} | {'平均確率':>7} | {'平均オッズ':>8}")
    print("-" * 65)
    for th in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        if th in summary_neutral.index:
            r = summary_neutral.loc[th]
            mark = "★" if r['roi_mean'] >= 100 else ""
            print(f"{th:>7.1f} | {r['roi_mean']:>6.1f}%{mark} | {r['roi_std']:>5.1f} | {r['bet_count']:>6.0f} | {r['hit_rate']:>5.2f}% | {r['avg_prob']:>6.3f} | {r['avg_odds']:>7.1f}倍")
    
    # 差分分析
    print("\n■ ROI差分（ニュートラル - 標準）:")
    for th in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        if th in summary_std.index and th in summary_neutral.index:
            diff = summary_neutral.loc[th]['roi_mean'] - summary_std.loc[th]['roi_mean']
            arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
            print(f"  EV >= {th:.1f}: {diff:+.1f}% {arrow}")
    
    # 深い推論
    print("\n" + "=" * 80)
    print("【深い推論】")
    print("=" * 80)
    
    # ニュートラルモデルでの高EV馬の特性を分析
    all_neutral_preds = pd.concat([
        pd.concat(all_results_neutral).reset_index(drop=True)
    ])
    
    best_neutral = summary_neutral['roi_mean'].max()
    best_std = summary_std['roi_mean'].max()
    
    print(f"""
■ 分析結果:
  標準モデル最高ROI: {best_std:.1f}%
  マーケットニュートラル最高ROI: {best_neutral:.1f}%
  
■ 推論:
""")
    
    if best_neutral > best_std:
        print("""
  【発見】マーケットニュートラルモデルの方が高ROI！
  
  これは以下を示唆:
  1. 市場情報（オッズ）がノイズとして作用していた
  2. 「純粋な能力予測」の方がEV計算に適している
  3. オッズとの「独立性」がEV戦略の鍵
  
  → 「過去成績のみ」で推定した勝率 × 現在オッズ = 真のEV
""")
    else:
        print("""
  【発見】標準モデルの方が高ROI（またはほぼ同等）
  
  これは以下を示唆:
  1. オッズ情報自体に予測価値がある（市場の知恵）
  2. ただしEV計算では「市場効率」を再現するだけ
  3. 根本的にEV閾値戦略はこのデータセットでは限界がある
  
  → 新しいアプローチが必要:
    a) 券種の多様化（複勝・馬連で控除率削減）
    b) 条件別モデル（競馬場・クラス別）
    c) 新しいデータソース（調教、パドック等）
""")
    
    # グラフ作成
    print("\n" + "=" * 80)
    print("【グラフ出力】")
    print("=" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 左上: ROI比較曲線
    ax1 = axes[0, 0]
    ax1.plot(summary_std.index, summary_std['roi_mean'], 'b-o', label='標準モデル', linewidth=2)
    ax1.fill_between(summary_std.index, 
                     summary_std['roi_mean'] - summary_std['roi_std'],
                     summary_std['roi_mean'] + summary_std['roi_std'], alpha=0.2, color='blue')
    ax1.plot(summary_neutral.index, summary_neutral['roi_mean'], 'r-^', label='マーケットニュートラル', linewidth=2)
    ax1.fill_between(summary_neutral.index, 
                     summary_neutral['roi_mean'] - summary_neutral['roi_std'],
                     summary_neutral['roi_mean'] + summary_neutral['roi_std'], alpha=0.2, color='red')
    ax1.axhline(y=100, color='gray', linestyle='--', alpha=0.7)
    ax1.set_xlabel('EV閾値')
    ax1.set_ylabel('ROI (%)')
    ax1.set_title('標準 vs マーケットニュートラル: EV閾値 vs ROI')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 150)
    
    # 右上: 平均確率の比較
    ax2 = axes[0, 1]
    ax2.plot(summary_std.index, summary_std['avg_prob'] * 100, 'b-o', label='標準モデル')
    ax2.plot(summary_neutral.index, summary_neutral['avg_prob'] * 100, 'r-^', label='マーケットニュートラル')
    ax2.set_xlabel('EV閾値')
    ax2.set_ylabel('平均予測確率 (%)')
    ax2.set_title('EV閾値ごとの平均予測確率')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 左下: 平均オッズの比較
    ax3 = axes[1, 0]
    ax3.plot(summary_std.index, summary_std['avg_odds'], 'b-o', label='標準モデル')
    ax3.plot(summary_neutral.index, summary_neutral['avg_odds'], 'r-^', label='マーケットニュートラル')
    ax3.set_xlabel('EV閾値')
    ax3.set_ylabel('平均オッズ (倍)')
    ax3.set_title('EV閾値ごとの平均オッズ')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 右下: キャリブレーション曲線
    ax4 = axes[1, 1]
    # 最終年のキャリブレーションを表示
    calib_std = all_calibration['standard'][-1]
    calib_neutral = all_calibration['neutral'][-1]
    ax4.plot([0, 1], [0, 1], 'k--', label='完璧なキャリブレーション')
    ax4.plot(calib_std['prob_pred'], calib_std['prob_true'], 'b-o', label=f"標準 (ECE={calib_std['ece']:.3f})")
    ax4.plot(calib_neutral['prob_pred'], calib_neutral['prob_true'], 'r-^', label=f"ニュートラル (ECE={calib_neutral['ece']:.3f})")
    ax4.set_xlabel('予測確率')
    ax4.set_ylabel('実際の勝率')
    ax4.set_title('確率キャリブレーション曲線')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.set_xlim(0, 0.3)
    ax4.set_ylim(0, 0.3)
    
    plt.tight_layout()
    output_path = project_root / "outputs/analysis/market_neutral_ev_analysis.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"グラフ保存: {output_path}")
    
    # CSV保存
    results_std.to_csv(project_root / "outputs/analysis/ev_analysis_standard.csv", index=False)
    results_neutral.to_csv(project_root / "outputs/analysis/ev_analysis_neutral.csv", index=False)
    
    plt.show()
    print("\n分析完了")


if __name__ == "__main__":
    main()
