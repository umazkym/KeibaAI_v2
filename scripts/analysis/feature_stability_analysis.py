# -*- coding: utf-8 -*-
"""
特徴量安定性分析スクリプト

年別の特徴量重要度を計算し、時間に対して安定している特徴量を特定する
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    """データを読み込む"""
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def train_and_get_importance(train_df, valid_df, feature_cols):
    """V15モデルを学習し、特徴量重要度を取得"""
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
    
    model = lgb.train(params, train_ds, num_boost_round=300, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    
    # 特徴量重要度を取得
    importance = model.feature_importance(importance_type='gain')
    importance_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': importance
    })
    
    # ROI計算
    preds = model.predict(X_valid)
    valid_df = valid_df.copy()
    valid_df['score'] = preds
    valid_df['rank_pred'] = valid_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = valid_df[valid_df['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bet_df) * 100 if len(bet_df) > 0 else 0
    
    return importance_df, roi


def main():
    print("=" * 80)
    print("特徴量安定性分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件")
    
    # 10年間Walk-forward検証
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        ('2020', '2018-12-31', '2019-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
        ('2019', '2017-12-31', '2018-01-01', '2018-12-31', '2019-01-01', '2019-12-31'),
        ('2018', '2016-12-31', '2017-01-01', '2017-12-31', '2018-01-01', '2018-12-31'),
    ]
    
    all_importance = []
    all_rois = []
    
    for i, (year, train_end, valid_start, valid_end, test_start, test_end) in enumerate(tqdm(test_periods, desc="年別分析")):
        print(f"\n[{year}年]")
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        if len(train) < 10000 or len(test) < 5000:
            print(f"  スキップ（データ不足）")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # 重要度とROIを取得
        importance_df, roi = train_and_get_importance(train_f, test_f, feature_cols)
        importance_df['year'] = year
        importance_df['roi'] = roi
        
        all_importance.append(importance_df)
        all_rois.append({'year': year, 'roi': roi})
        
        print(f"  ROI: {roi:.1f}%")
    
    # 結合
    importance_all = pd.concat(all_importance, ignore_index=True)
    roi_df = pd.DataFrame(all_rois)
    
    # 特徴量ごとの安定性分析
    print("\n" + "=" * 80)
    print("特徴量安定性分析結果")
    print("=" * 80)
    
    stability = importance_all.groupby('feature').agg({
        'importance': ['mean', 'std', 'min', 'max']
    }).round(2)
    stability.columns = ['mean', 'std', 'min', 'max']
    stability['cv'] = (stability['std'] / (stability['mean'] + 0.001)).round(2)  # 変動係数
    stability['range'] = stability['max'] - stability['min']
    stability = stability.sort_values('cv')
    
    # 安定特徴量（CV < 0.5）
    stable_features = stability[stability['cv'] < 0.5]
    unstable_features = stability[stability['cv'] >= 0.5]
    
    print(f"\n【安定特徴量】（CV < 0.5）: {len(stable_features)}個")
    print("-" * 70)
    print(f"{'特徴量':<40} {'平均重要度':>12} {'σ':>8} {'CV':>8}")
    print("-" * 70)
    for feat in stable_features.head(20).index:
        row = stable_features.loc[feat]
        print(f"{feat:<40} {row['mean']:>12.1f} {row['std']:>8.1f} {row['cv']:>8.2f}")
    
    print(f"\n【不安定特徴量】（CV >= 0.5）: {len(unstable_features)}個")
    print("-" * 70)
    print(f"{'特徴量':<40} {'平均重要度':>12} {'σ':>8} {'CV':>8}")
    print("-" * 70)
    for feat in unstable_features.head(20).index:
        row = unstable_features.loc[feat]
        print(f"{feat:<40} {row['mean']:>12.1f} {row['std']:>8.1f} {row['cv']:>8.2f}")
    
    # 年別の重要度変化（上位特徴量）
    print("\n【上位10特徴量の年別重要度】")
    print("-" * 80)
    
    top_features = stability.sort_values('mean', ascending=False).head(10).index.tolist()
    pivot = importance_all[importance_all['feature'].isin(top_features)].pivot(
        index='feature', columns='year', values='importance'
    )
    pivot = pivot.reindex(top_features)
    print(pivot.round(0).to_string())
    
    # 2025年に急落した可能性のある特徴量を特定
    print("\n【2025年に重要度が大きく変化した特徴量】")
    print("-" * 70)
    
    if '2025' in importance_all['year'].values and '2024' in importance_all['year'].values:
        imp_2024 = importance_all[importance_all['year'] == '2024'].set_index('feature')['importance']
        imp_2025 = importance_all[importance_all['year'] == '2025'].set_index('feature')['importance']
        
        change = (imp_2025 - imp_2024).sort_values()
        
        print("\n  重要度が大きく減少した特徴量:")
        for feat in change.head(5).index:
            print(f"    {feat}: {imp_2024.get(feat, 0):.0f} → {imp_2025.get(feat, 0):.0f} ({change[feat]:.0f})")
        
        print("\n  重要度が大きく増加した特徴量:")
        for feat in change.tail(5).index:
            print(f"    {feat}: {imp_2024.get(feat, 0):.0f} → {imp_2025.get(feat, 0):.0f} (+{change[feat]:.0f})")
    
    # ROI推移
    print("\n【年別ROI推移】")
    print("-" * 40)
    for _, row in roi_df.iterrows():
        print(f"  {row['year']}: {row['roi']:.1f}%")
    print(f"  平均: {roi_df['roi'].mean():.1f}% (σ={roi_df['roi'].std():.1f}%)")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    stability.to_csv(output_dir / "feature_stability.csv")
    importance_all.to_csv(output_dir / "feature_importance_yearly.csv", index=False)
    
    print(f"\n結果を {output_dir} に保存しました。")
    
    # 推奨事項
    print("\n" + "=" * 80)
    print("推奨事項")
    print("=" * 80)
    
    print(f"""
1. 安定特徴量のみでモデル構築を検討
   - 安定特徴量 {len(stable_features)}個 のみを使用
   - 現在の66個から削減

2. 不安定特徴量の除外を検討
   - 不安定特徴量 {len(unstable_features)}個 を除外
   - 特にCV > 1.0 の特徴量は要検討

3. 正則化の強化
   - max_depth を 3 → 2 に削減
   - min_child_samples を 100 → 500 に増加
""")


if __name__ == "__main__":
    main()
