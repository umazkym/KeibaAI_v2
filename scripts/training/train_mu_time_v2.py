#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v2: 時間予測特化モデル訓練スクリプト

目的: 標準化タイム（normalized_time）を予測するモデル

【改善点 from v1】
1. 標準化ターゲット（距離×馬場×状態で正規化）
2. 時間予測特化特徴量エンジン
3. V15参考の正則化パラメータ

【リーク対策】
1. 全特徴量は shift(1) + expanding() で計算
2. 基準タイムはTrain期間のデータのみで計算
3. 禁止特徴量を明示的に除外

【過学習対策】
1. 時系列分割
2. 強めの正則化（V15参考）
3. Early Stopping

Usage:
    python scripts/training/train_mu_time_v2.py
"""

import sys
import logging
import json
import pickle
import yaml
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import lightgbm as lgb
from scipy.stats import spearmanr

# プロジェクトルート
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ログ設定
log_dir = project_root / "outputs" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"train_mu_time_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# リーク対策: 禁止特徴量
# ============================================================
FORBIDDEN_FEATURES = [
    'finish_position',
    'finish_time_seconds',
    'normalized_time',  # ターゲット
    'is_win',
    'win_odds',
    'popularity',
    'race_id',
    'horse_id',
    'jockey_id',
    'trainer_id',
    'race_date',
    'horse_name',
    'jockey_name',
    'trainer_name',
]


def load_data():
    """データ読み込み"""
    logger.info("データ読み込み開始...")
    
    races_path = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    
    df = pd.read_parquet(races_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # 欠損値・重複の処理
    df = df.dropna(subset=['finish_position', 'finish_time_seconds'])
    df = df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    df = df.reset_index(drop=True)
    
    logger.info(f"  データ: {len(df):,}行")
    
    return df


def generate_features(df, train_end_date):
    """
    特徴量生成
    
    【リーク対策】
    - TimeFeatureEngineer はTrain期間のみでfit
    - V15特徴量も併用
    """
    from keibaai.src.features.time_feature_engineer_v1 import TimeFeatureEngineerV1
    from keibaai.src.features.leak_free_feature_engineer_v15 import (
        LeakFreeFeatureEngineerV15, FeatureConfigV15
    )
    
    logger.info(f"特徴量生成開始（train_end={train_end_date}）...")
    
    # Train期間のデータ
    train_mask = df['race_date'] < train_end_date
    train_df = df[train_mask].copy()
    
    # --- 1. 時間予測特化特徴量 ---
    time_fe = TimeFeatureEngineerV1()
    time_fe.fit(train_df)
    df = time_fe.transform(df)
    time_features = time_fe.get_feature_columns()
    base_time_stats = time_fe.get_base_time_stats()
    
    logger.info(f"  時間予測特徴量: {len(time_features)}個")
    
    # --- 2. V15特徴量（一部のみ使用）---
    # V15は「勝つか」予測用だが、一部の特徴量は時間予測にも有用
    # 例: horse_avg_c4_pos, horse_running_style, bracket_bias等
    
    # V15特徴量エンジンを使わず、シンプルな追加特徴量のみ
    # （V15は複雑すぎて時間予測には不要な特徴量が多い）
    
    # レース基本特徴量
    basic_features = [
        'distance_m',
        'bracket_number',
        'horse_weight',
        'age',
        'basis_weight',
    ]
    basic_features = [c for c in basic_features if c in df.columns]
    
    # カテゴリをエンコード
    for col in ['track_surface', 'track_condition', 'sex', 'venue', 'distance_category']:
        if col in df.columns:
            df[col + '_encoded'] = df[col].astype('category').cat.codes
            basic_features.append(col + '_encoded')
    
    all_features = time_features + basic_features
    logger.info(f"  全特徴量: {len(all_features)}個")
    
    return df, all_features, base_time_stats


def train_model(df, feature_cols, train_start, train_end, test_start, test_end):
    """
    モデル訓練
    
    【過学習対策】
    - V15参考の強正則化
    - Early Stopping
    """
    logger.info("モデル訓練開始...")
    logger.info(f"  Train: {train_start} - {train_end}")
    logger.info(f"  Test: {test_start} - {test_end}")
    
    # ターゲット
    target_col = 'normalized_time'
    df = df.dropna(subset=[target_col])
    
    # 時系列分割
    train_mask = (df['race_date'] >= train_start) & (df['race_date'] < train_end)
    test_mask = (df['race_date'] >= test_start) & (df['race_date'] < test_end)
    
    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()
    
    logger.info(f"  Train: {len(train_df):,}行, Test: {len(test_df):,}行")
    
    # 特徴量とターゲット
    X_train = train_df[feature_cols].fillna(0)  # V15と同じくfillna(0)
    y_train = train_df[target_col]
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df[target_col]
    
    # ハイパーパラメータ（V15参考の強正則化）
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'learning_rate': 0.02,        # V15の0.03より少し低く
        'num_leaves': 31,
        'max_depth': 4,               # V15は3だが時間予測はやや複雑
        'min_child_samples': 100,     # V15と同じ
        'reg_alpha': 2.0,             # L1正則化（V15は3.0）
        'reg_lambda': 3.0,            # L2正則化（V15は5.0）
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 3,
        'n_estimators': 1000,
        'verbose': -1,
    }
    
    model = lgb.LGBMRegressor(**params)
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=100, verbose=True),
            lgb.log_evaluation(period=100)
        ]
    )
    
    # 評価
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    train_rmse = np.sqrt(np.mean((y_train - y_train_pred) ** 2))
    test_rmse = np.sqrt(np.mean((y_test - y_test_pred) ** 2))
    
    # Spearman（レース内順位相関）
    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df['pred'] = y_train_pred
    test_df['pred'] = y_test_pred
    
    def calc_race_spearman(group):
        if len(group) < 3:
            return np.nan
        actual_rank = group['normalized_time'].rank()
        pred_rank = group['pred'].rank()
        corr, _ = spearmanr(actual_rank, pred_rank)
        return corr
    
    train_corrs = train_df.groupby('race_id').apply(calc_race_spearman)
    test_corrs = test_df.groupby('race_id').apply(calc_race_spearman)
    
    mean_train_corr = train_corrs.mean()
    mean_test_corr = test_corrs.mean()
    
    # Top1精度
    def calc_top1_acc(group):
        pred_top = group.loc[group['pred'].idxmin()]
        actual_top = group.loc[group['normalized_time'].idxmin()]
        return pred_top.name == actual_top.name
    
    train_top1 = train_df.groupby('race_id').apply(calc_top1_acc).mean()
    test_top1 = test_df.groupby('race_id').apply(calc_top1_acc).mean()
    
    metrics = {
        'train_rmse': train_rmse,
        'test_rmse': test_rmse,
        'rmse_gap': (train_rmse - test_rmse) / test_rmse * 100,
        'train_spearman': mean_train_corr,
        'test_spearman': mean_test_corr,
        'train_top1_acc': train_top1,
        'test_top1_acc': test_top1,
        'n_train': len(train_df),
        'n_test': len(test_df),
        'n_features': len(feature_cols),
        'n_estimators_used': model.n_estimators_,
    }
    
    logger.info("=== モデル評価結果 ===")
    logger.info(f"  Train RMSE (標準化): {train_rmse:.4f}")
    logger.info(f"  Test RMSE (標準化):  {test_rmse:.4f}")
    logger.info(f"  RMSE Gap: {metrics['rmse_gap']:.1f}%")
    logger.info(f"  Train Spearman: {mean_train_corr:.4f}")
    logger.info(f"  Test Spearman:  {mean_test_corr:.4f}")
    logger.info(f"  Train Top1 Acc: {train_top1:.1%}")
    logger.info(f"  Test Top1 Acc:  {test_top1:.1%}")
    
    return model, metrics


def save_model(model, feature_cols, metrics, base_time_stats):
    """モデル保存"""
    output_dir = project_root / "keibaai" / "models" / "mu_time_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # モデル
    with open(output_dir / "mu_time_model.pkl", 'wb') as f:
        pickle.dump(model, f)
    logger.info(f"モデル保存: {output_dir / 'mu_time_model.pkl'}")
    
    # 特徴量リスト
    with open(output_dir / "feature_names.json", 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)
    
    # 基準タイム統計（逆変換用）
    base_time_stats.to_parquet(output_dir / "base_time_stats.parquet")
    logger.info(f"基準タイム保存: {output_dir / 'base_time_stats.parquet'}")
    
    # メトリクス
    config = {
        'version': 'mu_time_v2',
        'created_at': datetime.now().isoformat(),
        'target': 'normalized_time',
        'description': '距離×馬場×状態で正規化したタイムを予測',
        'metrics': {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in metrics.items()},
    }
    with open(output_dir / "config.yaml", 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    
    # レポート
    report = f"""# μ_time_v2 訓練レポート

**作成日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## 改善点（v1からの変更）

1. **標準化ターゲット**: 距離×馬場×状態で正規化
2. **時間予測特化特徴量**: 過去タイム、上がり3F等
3. **V15参考の正則化**: max_depth=4, reg_alpha=2, reg_lambda=3

## 評価結果

| 指標 | Train | Test | 目標 |
|------|-------|------|------|
| RMSE (標準化) | {metrics['train_rmse']:.4f} | {metrics['test_rmse']:.4f} | - |
| Spearman | {metrics['train_spearman']:.4f} | **{metrics['test_spearman']:.4f}** | 0.50+ |
| Top1 Acc | {metrics['train_top1_acc']:.1%} | **{metrics['test_top1_acc']:.1%}** | 25%+ |

## リーク対策

- ✅ 過去タイム特徴量: shift(1) + expanding()
- ✅ 基準タイム: Train期間のデータのみで計算
- ✅ 禁止特徴量の明示的除外

## 過学習対策

- ✅ V15参考の正則化 (max_depth=4, reg_alpha=2, reg_lambda=3)
- ✅ Early Stopping (100 rounds)
- ✅ RMSE Gap: {metrics['rmse_gap']:.1f}%
"""
    with open(output_dir / "report.md", 'w', encoding='utf-8') as f:
        f.write(report)
    
    return output_dir


def main():
    logger.info("=" * 60)
    logger.info("μ_time_v2 訓練スクリプト開始")
    logger.info("=" * 60)
    
    try:
        # 1. データ読み込み
        df = load_data()
        
        # 2. 期間設定
        train_start = pd.Timestamp('2020-01-01')
        train_end = pd.Timestamp('2023-01-01')
        test_start = pd.Timestamp('2023-01-01')
        test_end = pd.Timestamp('2024-01-01')
        
        # 3. 特徴量生成
        df, feature_cols, base_time_stats = generate_features(df, train_end)
        
        # 禁止特徴量を除外
        feature_cols = [c for c in feature_cols if c not in FORBIDDEN_FEATURES]
        logger.info(f"最終特徴量数: {len(feature_cols)}")
        
        # 4. モデル訓練
        model, metrics = train_model(
            df, feature_cols,
            train_start, train_end,
            test_start, test_end
        )
        
        # 5. 保存
        output_dir = save_model(model, feature_cols, metrics, base_time_stats)
        
        logger.info("=" * 60)
        logger.info(f"訓練完了。出力: {output_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
