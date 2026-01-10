# -*- coding: utf-8 -*-
"""
ROI>100%達成分析 - 閾値×ROIシミュレーション

各モデル（V31/V32/V33）および各種アンサンブルで:
1. 閾値を変動させたときのROI推移
2. 年別安定性
3. 馬券数（極端に少なくないか）

を詳細に分析する
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from tqdm import tqdm
import matplotlib.pyplot as plt
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v31 import LeakFreeFeatureEngineerV31
from keibaai.src.features.leak_free_feature_engineer_v32 import LeakFreeFeatureEngineerV32
from keibaai.src.features.leak_free_feature_engineer_v33 import LeakFreeFeatureEngineerV33


def load_data():
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


def train_model(train_df, valid_df, feature_cols):
    """モデル学習"""
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 8,
        'max_depth': 2,
        'min_child_samples': 300,
        'reg_alpha': 15.0,
        'reg_lambda': 20.0,
        'bagging_fraction': 0.5,
        'bagging_freq': 3,
        'feature_fraction': 0.5,
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


def simulate_threshold_roi(test_df, preds, thresholds=[0.5, 0.6, 0.7, 0.8, 0.9, 0.95]):
    """
    閾値を変動させたときのROIと馬券数を計算
    閾値 = 予測スコアの上位何%を購入対象にするか
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # レースごとにTop1馬を取得
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    results = []
    
    for threshold in thresholds:
        # 閾値 = 予測スコアの上位X%
        score_threshold = bet_df['score'].quantile(1 - threshold)
        filtered = bet_df[bet_df['score'] >= score_threshold]
        
        if len(filtered) > 0:
            hits = filtered[filtered['is_hit']]
            roi = hits['win_odds'].sum() / len(filtered) * 100
            hit_rate = len(hits) / len(filtered) * 100
            bet_count = len(filtered)
        else:
            roi, hit_rate, bet_count = 0, 0, 0
        
        results.append({
            'threshold': threshold,
            'score_threshold': score_threshold,
            'roi': roi,
            'hit_rate': hit_rate,
            'bet_count': bet_count,
        })
    
    return pd.DataFrame(results)


def simulate_ev_threshold_roi(test_df, preds, ev_thresholds=[0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]):
    """
    期待値（予測スコア × オッズ）ベースの閾値シミュレーション
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # レースごとにTop1馬を取得
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # 期待値を概算（スコアを確率として扱う、正規化なし）
    # 実際の確率は不明だが、スコアが高いほど勝率が高いと仮定
    # スコアを0-1に正規化
    min_score = bet_df['score'].min()
    max_score = bet_df['score'].max()
    bet_df['norm_score'] = (bet_df['score'] - min_score) / (max_score - min_score + 1e-10)
    
    # 期待値の概算（オッズ調整）
    bet_df['ev_estimate'] = bet_df['norm_score'] * bet_df['win_odds']
    
    results = []
    
    for ev_thresh in ev_thresholds:
        filtered = bet_df[bet_df['ev_estimate'] >= ev_thresh]
        
        if len(filtered) > 0:
            hits = filtered[filtered['is_hit']]
            roi = hits['win_odds'].sum() / len(filtered) * 100
            hit_rate = len(hits) / len(filtered) * 100
            bet_count = len(filtered)
        else:
            roi, hit_rate, bet_count = 0, 0, 0
        
        results.append({
            'ev_threshold': ev_thresh,
            'roi': roi,
            'hit_rate': hit_rate,
            'bet_count': bet_count,
        })
    
    return pd.DataFrame(results)


def simulate_odds_band_roi(test_df, preds):
    """
    オッズ帯別のROI分析
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # オッズ帯
    odds_bands = [
        (1, 2, '1-2倍'),
        (2, 5, '2-5倍'),
        (5, 10, '5-10倍'),
        (10, 20, '10-20倍'),
        (20, 50, '20-50倍'),
        (50, 1000, '50倍以上'),
    ]
    
    results = []
    for low, high, label in odds_bands:
        subset = bet_df[(bet_df['win_odds'] >= low) & (bet_df['win_odds'] < high)]
        if len(subset) > 0:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100
            hit_rate = len(hits) / len(subset) * 100
            bet_count = len(subset)
        else:
            roi, hit_rate, bet_count = 0, 0, 0
        
        results.append({
            'odds_band': label,
            'roi': roi,
            'hit_rate': hit_rate,
            'bet_count': bet_count,
        })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("ROI>100%達成分析 - 閾値×ROIシミュレーション")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件")
    
    # 直近の検証データ（2024-2025年）で分析
    train_end = '2023-12-31'
    test_start = '2024-01-01'
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[races['race_date'] >= test_start].copy()
    
    print(f"\n訓練データ: {len(train):,}件")
    print(f"テストデータ: {len(test):,}件（2024-2025年）")
    
    # 各モデルを学習・予測
    models_results = {}
    
    for model_name, EngineClass in [
        ('V31', LeakFreeFeatureEngineerV31),
        ('V32', LeakFreeFeatureEngineerV32),
        ('V33', LeakFreeFeatureEngineerV33),
    ]:
        print(f"\n【{model_name}】学習・予測...")
        
        engine = EngineClass()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        # Validation用にtrainを分割
        val_start = '2023-01-01'
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        models_results[model_name] = {
            'preds': preds,
            'test_df': test_f,
        }
        
        # 基本ROI
        d = test_f.copy()
        d['score'] = preds
        d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
        bet_df = d[d['rank_pred'] == 1].copy()
        bet_df['is_hit'] = bet_df['finish_position'] == 1
        hits = bet_df[bet_df['is_hit']]
        base_roi = hits['win_odds'].sum() / len(bet_df) * 100
        print(f"  基本ROI（全レース購入）: {base_roi:.1f}%")
    
    # アンサンブル予測
    print("\n【アンサンブル】予測...")
    ensemble_preds = (
        models_results['V31']['preds'] * 0.4 +
        models_results['V32']['preds'] * 0.3 +
        models_results['V33']['preds'] * 0.3
    )
    models_results['Ensemble'] = {
        'preds': ensemble_preds,
        'test_df': models_results['V31']['test_df'],
    }
    
    print("\n" + "=" * 80)
    print("【閾値×ROIシミュレーション結果】")
    print("=" * 80)
    
    thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]
    
    for model_name in ['V31', 'V32', 'V33', 'Ensemble']:
        print(f"\n■ {model_name}")
        print("-" * 60)
        
        preds = models_results[model_name]['preds']
        test_df = models_results[model_name]['test_df']
        
        result = simulate_threshold_roi(test_df, preds, thresholds)
        
        print(f"{'閾値':>8} {'ROI':>10} {'的中率':>10} {'馬券数':>10}")
        for _, row in result.iterrows():
            roi_mark = "★" if row['roi'] >= 100 else ""
            print(f"{row['threshold']:>7.0%} {row['roi']:>9.1f}%{roi_mark} {row['hit_rate']:>9.1f}% {row['bet_count']:>10.0f}")
    
    print("\n" + "=" * 80)
    print("【期待値ベース閾値シミュレーション】")
    print("=" * 80)
    
    ev_thresholds = [0.5, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0]
    
    for model_name in ['V31', 'V32', 'V33', 'Ensemble']:
        print(f"\n■ {model_name}")
        print("-" * 60)
        
        preds = models_results[model_name]['preds']
        test_df = models_results[model_name]['test_df']
        
        result = simulate_ev_threshold_roi(test_df, preds, ev_thresholds)
        
        print(f"{'EV閾値':>8} {'ROI':>10} {'的中率':>10} {'馬券数':>10}")
        for _, row in result.iterrows():
            roi_mark = "★" if row['roi'] >= 100 else ""
            print(f"{row['ev_threshold']:>8.1f} {row['roi']:>9.1f}%{roi_mark} {row['hit_rate']:>9.1f}% {row['bet_count']:>10.0f}")
    
    print("\n" + "=" * 80)
    print("【オッズ帯別ROI分析】")
    print("=" * 80)
    
    for model_name in ['V31', 'V32', 'V33', 'Ensemble']:
        print(f"\n■ {model_name}")
        print("-" * 60)
        
        preds = models_results[model_name]['preds']
        test_df = models_results[model_name]['test_df']
        
        result = simulate_odds_band_roi(test_df, preds)
        
        print(f"{'オッズ帯':>10} {'ROI':>10} {'的中率':>10} {'馬券数':>10}")
        for _, row in result.iterrows():
            roi_mark = "★" if row['roi'] >= 100 else ""
            print(f"{row['odds_band']:>10} {row['roi']:>9.1f}%{roi_mark} {row['hit_rate']:>9.1f}% {row['bet_count']:>10.0f}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    
    # 結果をCSV保存
    output_dir = project_root / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 詳細結果を保存
    all_threshold_results = []
    for model_name in ['V31', 'V32', 'V33', 'Ensemble']:
        preds = models_results[model_name]['preds']
        test_df = models_results[model_name]['test_df']
        result = simulate_threshold_roi(test_df, preds, thresholds)
        result['model'] = model_name
        all_threshold_results.append(result)
    
    pd.concat(all_threshold_results).to_csv(output_dir / "roi_threshold_simulation.csv", index=False)
    print(f"\n結果を {output_dir / 'roi_threshold_simulation.csv'} に保存しました。")


if __name__ == "__main__":
    main()
