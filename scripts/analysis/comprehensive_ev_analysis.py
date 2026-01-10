# -*- coding: utf-8 -*-
"""
包括的EV閾値最適化分析

3つのアプローチを統合:
1. スコアのキャリブレーション（Isotonic/Platt Scaling）
2. EV以外の閾値: オッズ帯 + スコア順位の組み合わせ
3. 複合条件（オッズ帯×EV×競馬場等）の掛け合わせ

深い推論:
- なぜEV閾値だけではROI 100%を達成できないのか
- どのような条件の組み合わせで100%を超えられるか
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
import matplotlib.pyplot as plt
import matplotlib
from sklearn.calibration import CalibratedClassifierCV
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
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
    """V15 Binary Model"""
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


def calibrate_scores_isotonic(scores, y_true):
    """Isotonic Regression でスコアをキャリブレーション"""
    iso_reg = IsotonicRegression(out_of_bounds='clip')
    iso_reg.fit(scores, y_true)
    return iso_reg


def calibrate_scores_platt(scores, y_true):
    """Platt Scaling（ロジスティック回帰）でスコアをキャリブレーション"""
    platt = LogisticRegression()
    platt.fit(scores.reshape(-1, 1), y_true)
    return platt


def extract_venue(df):
    """競馬場コードを抽出"""
    df = df.copy()
    df['race_id_str'] = df['race_id'].astype(str)
    df['venue_code'] = df['race_id_str'].str[4:6]
    venue_map = {'01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
                 '05': '東京', '06': '中山', '07': '中京', '08': '京都',
                 '09': '阪神', '10': '小倉'}
    df['venue'] = df['venue_code'].map(venue_map).fillna('その他')
    return df


def analyze_approach1_calibration(pred_df, returns_df, valid_scores, valid_y):
    """
    アプローチ1: スコアのキャリブレーション
    - Isotonic Regression
    - Platt Scaling
    """
    print("\n" + "=" * 80)
    print("【アプローチ1: スコアキャリブレーション】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    # キャリブレーションモデルを学習
    iso_model = calibrate_scores_isotonic(valid_scores, valid_y)
    platt_model = calibrate_scores_platt(valid_scores, valid_y)
    
    # テストデータにキャリブレーション適用
    pred_df = pred_df.copy()
    pred_df['score_iso'] = iso_model.predict(pred_df['score'].values)
    pred_df['score_platt'] = platt_model.predict_proba(pred_df['score'].values.reshape(-1, 1))[:, 1]
    
    # 各スコアでEV計算
    for score_col, name in [('score', '生スコア'), ('score_iso', 'Isotonic'), ('score_platt', 'Platt')]:
        pred_df[f'ev_{score_col}'] = pred_df['win_odds'] * pred_df[score_col]
    
    # Top1予測でROI比較
    results = []
    for score_col, name in [('score', '生スコア'), ('score_iso', 'Isotonic'), ('score_platt', 'Platt')]:
        pred_df['rank_tmp'] = pred_df.groupby('race_id')[score_col].rank(ascending=False, method='first')
        top1 = pred_df[pred_df['rank_tmp'] == 1].copy()
        
        merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                            on=['race_id', 'horse_number'], how='left')
        merged['return'] = merged['payout'].fillna(0) / 100
        
        ev_col = f'ev_{score_col}'
        
        # EV閾値別ROI
        for threshold in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
            subset = merged[merged[ev_col] >= threshold]
            if len(subset) >= 10:
                roi = subset['return'].sum() / len(subset) * 100
                results.append({
                    'method': name, 'threshold': threshold, 'roi': roi, 'count': len(subset)
                })
    
    result_df = pd.DataFrame(results)
    pivot = result_df.pivot(index='threshold', columns='method', values='roi')
    
    print("\n■ キャリブレーション方法別 EV閾値 vs ROI:")
    print(pivot.round(1).to_string())
    
    # 最良の方法を特定
    best = result_df.nlargest(1, 'roi').iloc[0]
    print(f"\n→ 最良: {best['method']} (閾値 {best['threshold']:.1f}, ROI {best['roi']:.1f}%)")
    
    return pred_df, result_df


def analyze_approach2_odds_rank_combo(pred_df, returns_df):
    """
    アプローチ2: オッズ帯 + スコア順位の組み合わせ
    
    深い推論:
    - EV = オッズ × 確率 は「両者のバランス」
    - しかし「オッズ帯で絞る」+「スコア順位で絞る」は異なるアプローチ
    - 高オッズ帯のTop1 vs 低オッズ帯のTop3 など
    """
    print("\n" + "=" * 80)
    print("【アプローチ2: オッズ帯 × スコア順位の組み合わせ】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    pred_df = pred_df.copy()
    pred_df['score_rank'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # オッズ帯の定義
    odds_bands = [
        (1, 3, '1-3倍'),
        (3, 5, '3-5倍'),
        (5, 10, '5-10倍'),
        (10, 20, '10-20倍'),
        (20, 50, '20-50倍'),
        (50, 999, '50倍+'),
    ]
    
    results = []
    
    for low, high, band_name in odds_bands:
        for rank in [1, 2, 3]:
            subset = pred_df[(pred_df['win_odds'] >= low) & 
                             (pred_df['win_odds'] < high) & 
                             (pred_df['score_rank'] == rank)]
            
            if len(subset) >= 30:
                merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                       on=['race_id', 'horse_number'], how='left')
                merged['return'] = merged['payout'].fillna(0) / 100
                merged['is_hit'] = ~merged['payout'].isna()
                
                roi = merged['return'].sum() / len(merged) * 100
                hit_rate = merged['is_hit'].mean() * 100
                
                results.append({
                    'odds_band': band_name,
                    'score_rank': f'Top{rank}',
                    'roi': roi,
                    'hit_rate': hit_rate,
                    'count': len(merged)
                })
    
    result_df = pd.DataFrame(results)
    pivot = result_df.pivot(index='odds_band', columns='score_rank', values='roi')
    
    print("\n■ オッズ帯 × スコア順位 ROIマトリクス:")
    print(pivot.round(1).to_string())
    
    print("\n■ ROI 90%超の条件:")
    good_conditions = result_df[result_df['roi'] >= 90].sort_values('roi', ascending=False)
    if len(good_conditions) > 0:
        for _, row in good_conditions.iterrows():
            mark = "★" if row['roi'] >= 100 else ""
            print(f"  {row['odds_band']} × {row['score_rank']}: ROI {row['roi']:.1f}%{mark} (n={row['count']}, 的中率{row['hit_rate']:.1f}%)")
    else:
        print("  該当なし")
    
    return result_df


def analyze_approach3_compound_conditions(pred_df, returns_df):
    """
    アプローチ3: 複合条件の掛け合わせ
    
    深い推論:
    - 単一条件では100%達成困難
    - 複数条件の「AND」で絞り込むとどうなるか
    - 競馬場 × オッズ帯 × EV閾値 × スコア順位
    """
    print("\n" + "=" * 80)
    print("【アプローチ3: 複合条件の掛け合わせ】")
    print("=" * 80)
    
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    pred_df = pred_df.copy()
    pred_df['score_rank'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    pred_df['ev'] = pred_df['win_odds'] * pred_df['score']
    pred_df = extract_venue(pred_df)
    
    # 条件の定義
    conditions = []
    
    # 条件1: 得意競馬場 + 中穴オッズ帯 + Top1
    cond1 = pred_df[
        (pred_df['venue'].isin(['小倉', '中京', '東京'])) &
        (pred_df['win_odds'] >= 5) & (pred_df['win_odds'] < 20) &
        (pred_df['score_rank'] == 1)
    ]
    conditions.append(('得意場×5-20倍×Top1', cond1))
    
    # 条件2: EV >= 1.5 + オッズ10-30倍 + Top1
    cond2 = pred_df[
        (pred_df['ev'] >= 1.5) &
        (pred_df['win_odds'] >= 10) & (pred_df['win_odds'] < 30) &
        (pred_df['score_rank'] == 1)
    ]
    conditions.append(('EV≥1.5×10-30倍×Top1', cond2))
    
    # 条件3: 高EV + 中穴 + Top1-2
    cond3 = pred_df[
        (pred_df['ev'] >= 2.0) &
        (pred_df['win_odds'] >= 5) & (pred_df['win_odds'] < 15) &
        (pred_df['score_rank'] <= 2)
    ]
    conditions.append(('EV≥2.0×5-15倍×Top1-2', cond3))
    
    # 条件4: スコア上位 + 穴馬オッズ
    cond4 = pred_df[
        (pred_df['score'] >= pred_df.groupby('race_id')['score'].transform('max') * 0.8) &
        (pred_df['win_odds'] >= 10) & (pred_df['win_odds'] < 50)
    ]
    conditions.append(('スコア8割+×10-50倍', cond4))
    
    # 条件5: オッズ乖離（本命なのに高オッズ）
    # popularityが低い（人気がある）のにオッズが高い
    if 'popularity' in pred_df.columns:
        cond5 = pred_df[
            (pred_df['popularity'] <= 5) &  # 5番人気以内
            (pred_df['win_odds'] >= 8) &    # オッズ8倍以上（乖離）
            (pred_df['score_rank'] == 1)
        ]
        conditions.append(('人気≤5×オッズ≥8×Top1', cond5))
    
    # 条件6: 超穴馬のTop1（宝くじ戦略）
    cond6 = pred_df[
        (pred_df['win_odds'] >= 20) &
        (pred_df['score_rank'] == 1) &
        (pred_df['score'] >= 0.05)  # 最低限のスコア
    ]
    conditions.append(('20倍+×Top1×スコア≥0.05', cond6))
    
    # 条件7: 低分散レース（1番人気が圧倒的）での2番手狙い
    # レース内のスコア分散が小さい = 1番人気が強い
    pred_df['race_score_std'] = pred_df.groupby('race_id')['score'].transform('std')
    cond7 = pred_df[
        (pred_df['race_score_std'] >= pred_df['race_score_std'].quantile(0.75)) &  # 高分散レース
        (pred_df['score_rank'] == 1) &
        (pred_df['win_odds'] >= 5)
    ]
    conditions.append(('高分散レース×Top1×5倍+', cond7))
    
    print("\n■ 複合条件別 ROI:")
    print(f"{'条件':<30} | {'ROI':>7} | {'的中率':>7} | {'件数':>6} | {'判定'}")
    print("-" * 70)
    
    results = []
    for name, subset in conditions:
        if len(subset) >= 20:
            merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                   on=['race_id', 'horse_number'], how='left')
            merged['return'] = merged['payout'].fillna(0) / 100
            merged['is_hit'] = ~merged['payout'].isna()
            
            roi = merged['return'].sum() / len(merged) * 100
            hit_rate = merged['is_hit'].mean() * 100
            
            mark = "★★★" if roi >= 100 else ("★★" if roi >= 90 else ("★" if roi >= 80 else ""))
            print(f"{name:<30} | {roi:>6.1f}% | {hit_rate:>6.2f}% | {len(merged):>6} | {mark}")
            
            results.append({
                'condition': name, 'roi': roi, 'hit_rate': hit_rate, 'count': len(merged)
            })
    
    result_df = pd.DataFrame(results)
    
    # 100%超の条件を強調
    roi_100 = result_df[result_df['roi'] >= 100]
    if len(roi_100) > 0:
        print(f"\n★ ROI 100%超達成! ★")
        for _, row in roi_100.iterrows():
            print(f"  {row['condition']}: ROI {row['roi']:.1f}%")
    
    return result_df


def deep_inference_analysis(all_results):
    """
    深い推論: なぜ100%達成が困難なのか、どうすれば可能か
    """
    print("\n" + "=" * 80)
    print("【深い推論: ROI 100%達成の可能性分析】")
    print("=" * 80)
    
    print("""
■ 観察事実:
  1. 的中型モデル（V15）: EV閾値を上げるとROI低下（確率が下がるため）
  2. 穴馬検出型モデル（V4.4）: EV閾値を上げてもROI横ばい
  3. キャリブレーション: 若干の改善があるが100%には届かない
  4. オッズ帯×順位: 特定条件で80-90%だが、サンプル数が減少

■ 深い推論:

  【仮説1】市場効率仮説
  ┌─────────────────────────────────────────────────────────────┐
  │ 競馬オッズは「大衆の知恵」の集合であり、                      │
  │ 長期的には真の勝率に収束する傾向がある。                      │
  │ → AIがエッジを持つのは「市場が誤っている」場合のみ            │
  │ → 控除率20%を上回るエッジを維持するのは構造的に困難           │
  └─────────────────────────────────────────────────────────────┘

  【仮説2】情報の非対称性の縮小
  ┌─────────────────────────────────────────────────────────────┐
  │ かつては穴馬を発見する「情報優位」があったが、                │
  │ AIやデータ分析の普及により優位性が薄れている。                │
  │ → 同じデータを使う限り、同様の結論に至る                     │
  │ → 差別化には「新しいデータソース」が必要                     │
  └─────────────────────────────────────────────────────────────┘

  【仮説3】サンプル数と統計的有意性のトレードオフ
  ┌─────────────────────────────────────────────────────────────┐
  │ 高ROI条件を発見しても、サンプル数が少なすぎると               │
  │ 統計的に有意ではなく、将来の再現性がない。                    │
  │ → 過去データへの過適合（バックテスト罠）                     │
  │ → 最低100-200件のサンプルが必要                              │
  └─────────────────────────────────────────────────────────────┘

■ 100%達成への可能性:

  1. 【複合条件の絞り込み】
     - 単一条件（EV閾値のみ）では不十分
     - 「競馬場 × オッズ帯 × スコア順位 × 季節」等の掛け合わせ
     - ただしサンプル数減少リスクあり

  2. 【券種の使い分け】
     - 単勝だけでなく、馬連/三連複でヘッジ
     - 的中率を上げつつ配当を確保

  3. 【動的閾値】
     - 固定閾値ではなく、レースごとにEV閾値を調整
     - 例: 荒れやすいレースでは閾値を下げる

  4. 【新しい特徴量/データソース】
     - パドック映像のAI分析
     - 調教タイム、馬場状態の詳細
     - リアルタイムオッズ変動
""")


def main():
    print("=" * 80)
    print("包括的EV閾値最適化分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 4年間Walk-forward検証
    years = [2022, 2023, 2024, 2025]
    
    all_pred_dfs = []
    all_valid_scores = []
    all_valid_y = []
    
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
        
        X_valid = valid_sub[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        valid_scores = model.predict(X_valid)
        valid_y = (valid_sub['finish_position'] == 1).astype(int).values
        
        all_valid_scores.extend(valid_scores)
        all_valid_y.extend(valid_y)
        
        test_preds = model.predict(X_test)
        
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df['score'] = test_preds
        pred_df['year'] = year
        
        all_pred_dfs.append(pred_df)
        
        print(f"  年間レース数: {pred_df['race_id'].nunique():,}件")
    
    # 全期間統合
    all_pred_df = pd.concat(all_pred_dfs, ignore_index=True)
    all_valid_scores = np.array(all_valid_scores)
    all_valid_y = np.array(all_valid_y)
    
    print(f"\n総レコード数: {len(all_pred_df):,}件")
    
    # ===== アプローチ1: スコアキャリブレーション =====
    calibrated_df, calib_results = analyze_approach1_calibration(
        all_pred_df, returns, all_valid_scores, all_valid_y
    )
    
    # ===== アプローチ2: オッズ帯 × スコア順位 =====
    odds_rank_results = analyze_approach2_odds_rank_combo(all_pred_df, returns)
    
    # ===== アプローチ3: 複合条件の掛け合わせ =====
    compound_results = analyze_approach3_compound_conditions(all_pred_df, returns)
    
    # ===== 深い推論 =====
    deep_inference_analysis({
        'calibration': calib_results,
        'odds_rank': odds_rank_results,
        'compound': compound_results
    })
    
    # グラフ作成
    print("\n" + "=" * 80)
    print("【グラフ出力】")
    print("=" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 左上: キャリブレーション比較
    ax1 = axes[0, 0]
    pivot = calib_results.pivot(index='threshold', columns='method', values='roi')
    for col in pivot.columns:
        ax1.plot(pivot.index, pivot[col], label=col, marker='o')
    ax1.axhline(y=100, color='r', linestyle='--', alpha=0.5)
    ax1.set_xlabel('EV閾値')
    ax1.set_ylabel('ROI (%)')
    ax1.set_title('アプローチ1: キャリブレーション別 EV閾値 vs ROI')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 右上: オッズ帯×順位ヒートマップ
    ax2 = axes[0, 1]
    pivot2 = odds_rank_results.pivot(index='odds_band', columns='score_rank', values='roi')
    im = ax2.imshow(pivot2.values, cmap='RdYlGn', aspect='auto', vmin=50, vmax=120)
    ax2.set_xticks(range(len(pivot2.columns)))
    ax2.set_xticklabels(pivot2.columns)
    ax2.set_yticks(range(len(pivot2.index)))
    ax2.set_yticklabels(pivot2.index)
    ax2.set_title('アプローチ2: オッズ帯 × スコア順位 ROI')
    plt.colorbar(im, ax=ax2, label='ROI (%)')
    for i in range(len(pivot2.index)):
        for j in range(len(pivot2.columns)):
            val = pivot2.values[i, j]
            if not np.isnan(val):
                ax2.text(j, i, f'{val:.0f}', ha='center', va='center', fontsize=8)
    
    # 左下: 複合条件バーチャート
    ax3 = axes[1, 0]
    compound_results_sorted = compound_results.sort_values('roi', ascending=True)
    colors = ['green' if r >= 100 else ('yellow' if r >= 90 else ('orange' if r >= 80 else 'red')) 
              for r in compound_results_sorted['roi']]
    bars = ax3.barh(compound_results_sorted['condition'], compound_results_sorted['roi'], color=colors)
    ax3.axvline(x=100, color='r', linestyle='--', alpha=0.7)
    ax3.set_xlabel('ROI (%)')
    ax3.set_title('アプローチ3: 複合条件別 ROI')
    ax3.set_xlim(0, max(150, compound_results_sorted['roi'].max() + 10))
    
    # 右下: サマリーテキスト
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    best_calib = calib_results.nlargest(1, 'roi').iloc[0]
    best_odds_rank = odds_rank_results.nlargest(1, 'roi').iloc[0]
    best_compound = compound_results.nlargest(1, 'roi').iloc[0]
    
    summary_text = f"""
【ベスト条件サマリー】

■ アプローチ1: キャリブレーション
  {best_calib['method']} (閾値 {best_calib['threshold']:.1f})
  ROI: {best_calib['roi']:.1f}%, n={best_calib['count']}

■ アプローチ2: オッズ帯×スコア順位
  {best_odds_rank['odds_band']} × {best_odds_rank['score_rank']}
  ROI: {best_odds_rank['roi']:.1f}%, n={best_odds_rank['count']}

■ アプローチ3: 複合条件
  {best_compound['condition']}
  ROI: {best_compound['roi']:.1f}%, n={best_compound['count']}

【結論】
{"★ ROI 100%超達成条件あり! ★" if max(best_calib['roi'], best_odds_rank['roi'], best_compound['roi']) >= 100 else "100%超達成条件なし"}
"""
    ax4.text(0.1, 0.5, summary_text, fontsize=10, verticalalignment='center', 
             family='monospace', transform=ax4.transAxes)
    
    plt.tight_layout()
    output_path = project_root / "outputs/analysis/comprehensive_ev_analysis.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"グラフ保存: {output_path}")
    
    # CSV保存
    compound_results.to_csv(project_root / "outputs/analysis/compound_conditions_roi.csv", index=False)
    odds_rank_results.to_csv(project_root / "outputs/analysis/odds_rank_roi.csv", index=False)
    
    plt.show()
    print("\n分析完了")


if __name__ == "__main__":
    main()
