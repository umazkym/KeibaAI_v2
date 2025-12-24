#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
複数期間モデル比較シミュレーション (進捗表示版)
"""

import sys
import os
import io

# バッファ無効化
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', line_buffering=True)
os.environ['PYTHONIOENCODING'] = 'utf-8'

from pathlib import Path

# プロジェクトルート
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'keibaai' / 'src'))

import pandas as pd
import numpy as np
import pickle
import lightgbm as lgb
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import yaml
import warnings
import gc
import time
warnings.filterwarnings('ignore')

def log(msg):
    """リアルタイムログ出力"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)

log("=" * 60)
log("複数期間モデル比較シミュレーション 開始")
log("=" * 60)

# =============================================================================
# 設定
# =============================================================================
data_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet'
models_dir = project_root / 'keibaai' / 'models'
CACHE_FILE = models_dir / 'multi_period_simulation_features.parquet'

# =============================================================================
# 1. データ読み込み
# =============================================================================
log("[STEP 1/4] データ読み込み...")

perf_path = data_dir / 'horses_performance' / 'horses_performance_fixed.parquet'
if not perf_path.exists():
    log(f"ERROR: {perf_path} が見つかりません")
    sys.exit(1)

df = pd.read_parquet(perf_path)
df['race_date'] = pd.to_datetime(df['race_date'])
log(f"  データ件数: {len(df):,}")
log(f"  期間: {df['race_date'].min().date()} ~ {df['race_date'].max().date()}")

# Odds マージ
races_path = data_dir / 'races' / 'races.parquet'
if races_path.exists():
    races_df = pd.read_parquet(races_path)
    if 'win_odds' not in df.columns:
        df = df.merge(
            races_df[['race_id', 'horse_number', 'win_odds']].drop_duplicates(['race_id', 'horse_number']),
            on=['race_id', 'horse_number'],
            how='left'
        )
        log("  win_odds マージ完了")

# Pedigrees & Horses
ped_path = data_dir / 'pedigrees' / 'pedigrees.parquet'
ped_df = pd.read_parquet(ped_path) if ped_path.exists() else pd.DataFrame()

prof_path = data_dir / 'horses' / 'horses.parquet'
prof_df = pd.read_parquet(prof_path) if prof_path.exists() else pd.DataFrame()

log("[STEP 1/4] 完了")

# =============================================================================
# 2. 特徴量生成
# =============================================================================
log("[STEP 2/4] 特徴量生成...")

if CACHE_FILE.exists():
    log(f"  キャッシュ使用: {CACHE_FILE.name}")
    features_df = pd.read_parquet(CACHE_FILE)
    log(f"  読み込み完了: {len(features_df):,} 行")
else:
    log("  キャッシュなし - 新規生成開始")
    log("  FeatureEngine をロード中...")
    
    from keibaai.src.features.feature_engine import FeatureEngine
    config_path = project_root / 'keibaai' / 'configs' / 'features.yaml'
    engine = FeatureEngine(config_path)
    
    # 生成対象期間
    start_date = pd.Timestamp('2022-01-01')
    end_date = df['race_date'].max()
    
    # 月数を計算
    total_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    log(f"  生成対象: {start_date.date()} ~ {end_date.date()} ({total_months}ヶ月)")
    
    current_date = start_date
    feature_dfs = []
    month_count = 0
    
    while current_date <= end_date:
        next_month = current_date + relativedelta(months=1)
        month_count += 1
        
        # Target: 今月
        target_mask = (df['race_date'] >= current_date) & (df['race_date'] < next_month)
        target_df = df[target_mask].copy()
        
        if target_df.empty:
            log(f"  [{month_count:02d}/{total_months}] {current_date.strftime('%Y-%m')}: スキップ (データなし)")
            current_date = next_month
            continue
        
        # History: 過去全て
        history_mask = (df['race_date'] < current_date)
        history_df = df[history_mask].copy()
        
        log(f"  [{month_count:02d}/{total_months}] {current_date.strftime('%Y-%m')}: {len(target_df):,}件処理中...")
        start_time = time.time()
        
        try:
            features = engine.generate_features(
                shutuba_df=target_df,
                results_history_df=history_df,
                horse_profiles_df=prof_df,
                pedigree_df=ped_df
            )
            
            # 必要カラム保持
            base_cols = ['race_id', 'horse_number', 'horse_id', 'race_date', 'finish_position', 'win_odds']
            cols_to_merge = [c for c in base_cols if c not in features.columns and c in target_df.columns]
            if cols_to_merge:
                features = features.merge(
                    target_df[['race_id', 'horse_id'] + cols_to_merge],
                    on=['race_id', 'horse_id'],
                    how='left',
                    suffixes=('', '_raw')
                )
            
            feature_dfs.append(features)
            elapsed = time.time() - start_time
            log(f"              完了 ({elapsed:.1f}秒)")
            
        except Exception as e:
            log(f"              エラー: {str(e)[:50]}")
        
        current_date = next_month
        gc.collect()
    
    if not feature_dfs:
        log("ERROR: 特徴量が生成されませんでした")
        sys.exit(1)
    
    features_df = pd.concat(feature_dfs, ignore_index=True)
    log(f"  全期間結合完了: {len(features_df):,} 行")
    
    # キャッシュ保存
    features_df.to_parquet(CACHE_FILE)
    log(f"  キャッシュ保存: {CACHE_FILE.name}")

log(f"[STEP 2/4] 完了 - 特徴量カラム数: {len(features_df.columns)}")

# =============================================================================
# 3. モデル評価
# =============================================================================
log("[STEP 3/4] モデル評価...")

test_periods = [
    ('2022', '2022-01-01', '2022-12-31'),
    ('2023', '2023-01-01', '2023-12-31'),
    ('2024', '2024-01-01', '2024-12-31'),
    ('2025', '2025-01-01', '2025-10-31'),
]

target_models = [
    'mu_v5_0',
    'mu_v10_0',
    'mu_v11_0',
    'mu_v8_0',
    'mu_v4_3',
    'mu_v2_6',
    'v26_restored'
]

def load_model(model_name):
    """モデルファイルを読み込む（ranker, model, booster等のファイル名に対応）"""
    model_dir = models_dir / model_name
    if not model_dir.exists():
        return None
    
    # 優先順: model > ranker > regressor > その他 .pkl
    pkl_files = list(model_dir.glob('*.pkl'))
    
    # 優先度でソート
    def priority(p):
        name = p.name.lower()
        if 'model' in name: return 0
        if 'ranker' in name: return 1
        if 'regressor' in name: return 2
        return 3
    
    pkl_files.sort(key=priority)
    
    for p in pkl_files:
        try:
            with open(p, 'rb') as f:
                return pickle.load(f)
        except Exception:
            continue
    
    # .txt (LightGBM Booster形式)
    for p in model_dir.glob('*.txt'):
        try:
            return lgb.Booster(model_file=str(p))
        except Exception:
            continue
    
    return None

def load_features_list(model_name):
    model_dir = models_dir / model_name
    json_path = model_dir / 'feature_names.json'
    if json_path.exists():
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    csv_path = model_dir / 'feature_importance.csv'
    if csv_path.exists():
        return pd.read_csv(csv_path)['feature'].tolist()
    return None

def calculate_roi(test_df, preds):
    test_df = test_df.copy()
    test_df['pred'] = preds
    test_df['rank'] = test_df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    top1 = test_df[test_df['rank'] == 1]
    
    if 'is_win' in test_df.columns:
        hits = top1[top1['is_win'] == 1]
    else:
        hits = top1[top1['finish_position'] == 1]
    
    valid_bets = top1.dropna(subset=['win_odds'])
    valid_hits = hits[hits.index.isin(valid_bets.index)]
    
    if len(valid_bets) == 0:
        return 0, 0, 0
    
    return valid_hits['win_odds'].sum() / len(valid_bets), len(valid_bets), len(valid_hits)

results_summary = []

for i, model_name in enumerate(target_models, 1):
    log(f"  [{i}/{len(target_models)}] {model_name} 評価中...")
    
    model = load_model(model_name)
    if model is None:
        log(f"      -> スキップ (モデルなし)")
        continue
    
    required_features = load_features_list(model_name)
    if required_features is None:
        log(f"      -> スキップ (特徴量リストなし)")
        continue
    
    # 特徴量アライメント
    missing = [c for c in required_features if c not in features_df.columns]
    found = len(required_features) - len(missing)
    log(f"      特徴量: {found}/{len(required_features)} (欠損{len(missing)})")
    
    for c in missing:
        features_df[c] = 0
    
    X_full = features_df[required_features].fillna(0)
    
    model_res = {'model': model_name}
    rois = []
    
    for period_name, start, end in test_periods:
        mask = (features_df['race_date'] >= start) & (features_df['race_date'] <= end)
        if mask.sum() == 0:
            continue
        
        try:
            preds = model.predict(X_full[mask])
            roi, bets, hits = calculate_roi(features_df[mask], preds)
            model_res[period_name] = roi
            rois.append(roi)
            log(f"      {period_name}: ROI={roi*100:.1f}% (Bets={bets}, Hits={hits})")
        except Exception as e:
            log(f"      {period_name}: エラー - {str(e)[:30]}")
    
    if rois:
        model_res['avg'] = np.mean(rois)
        model_res['std'] = np.std(rois)
        results_summary.append(model_res)

log("[STEP 3/4] 完了")

# =============================================================================
# 4. 結果出力
# =============================================================================
log("[STEP 4/4] 結果サマリー")
log("=" * 70)

results_summary.sort(key=lambda x: x.get('avg', -1), reverse=True)

log(f"{'Model':<15} | {'2022':>7} | {'2023':>7} | {'2024':>7} | {'2025':>7} | {'Avg':>7} | {'Std':>6}")
log("-" * 70)

for res in results_summary:
    row = f"{res['model']:<15}"
    for p in ['2022', '2023', '2024', '2025']:
        val = res.get(p, None)
        if val is not None:
            row += f" | {val*100:>6.1f}%"
        else:
            row += f" | {'N/A':>7}"
    row += f" | {res.get('avg', 0)*100:>6.1f}%"
    row += f" | ±{res.get('std', 0)*100:>5.1f}%"
    log(row)

# JSON保存
output_path = models_dir / 'multi_period_simulation_summary.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(results_summary, f, indent=2, ensure_ascii=False)

log("=" * 70)
log(f"結果保存: {output_path.name}")
log("完了!")
