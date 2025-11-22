"""
μモデルのハイパーパラメータ最適化スクリプト (Optuna使用)

【重要】データリーク防止のための原則:
1. 時系列分割: 過去データで学習、未来データで検証
2. レース後情報の除外: finish_position, win_odds等は絶対に使わない
3. 同一レース内の情報: horse_numberは使える（レース前に確定）
4. 2024年データは一切使わない（最終検証のみ）

実行方法:
    python optimize_mu_hyperparameters.py --n_trials 50 --timeout 10800
"""

import optuna
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler
import lightgbm as lgb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import logging
import argparse
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, ndcg_score
import warnings
warnings.filterwarnings('ignore')

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('optuna_optimization.log', encoding='utf-8')
    ]
)

class DataLeakValidator:
    """データリーク検証クラス"""
    
    FORBIDDEN_COLUMNS = [
        'finish_position', 'finish_time_seconds', 'finish_time_str',
        'win_odds', 'popularity', 'prize_money',
        'margin_seconds', 'margin_str', 'passing_order',
        'last_3f_time', 'time_except_last3f'
    ]
    
    @classmethod
    def validate_features(cls, df, feature_cols):
        """特徴量にレース後情報が含まれていないか検証"""
        leaked = [col for col in feature_cols if col in cls.FORBIDDEN_COLUMNS]
        if leaked:
            raise ValueError(f"❌ データリーク検出: {leaked}")
        logging.info("✅ データリーク検証: 合格")
        return True

def load_training_data():
    """学習データのロード（2020-2023年のみ、2024年は除外）"""
    logging.info("学習データをロード中...")
    
    # 特徴量データ
    features_dir = Path('keibaai/data/features/parquet')
    year_partitions = sorted(features_dir.glob('year=*'))
    
    dfs = []
    for year_partition in year_partitions:
        year = int(year_partition.name.split('=')[1])
        # 2024年は最終検証用なので除外
        if year >= 2024:
            logging.info(f"  年={year}: スキップ（最終検証用）")
            continue
        
        df_year = pd.read_parquet(year_partition)
        dfs.append(df_year)
        logging.info(f"  年={year}: {len(df_year)}行")
    
    features_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"特徴量データ: {len(features_df)}行")
    
    # race_dateで時系列ソート（重要！）
    if 'race_date' in features_df.columns:
        features_df['race_date'] = pd.to_datetime(features_df['race_date'])
        features_df = features_df.sort_values('race_date').reset_index(drop=True)
        logging.info("✅ 時系列でソート完了")
    
    # レース結果データ
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # 2020-2023年のみ
    races_df = races_df[
        (races_df['race_date'].dt.year >= 2020) & 
        (races_df['race_date'].dt.year < 2024)
    ]
    
    # マージ
    merge_keys = ['race_id', 'horse_id']
    target_cols = ['finish_position', 'finish_time_seconds']
    
    # データリーク防止: ターゲット変数を除外
    exclude_cols = DataLeakValidator.FORBIDDEN_COLUMNS
    cols_to_drop = [c for c in exclude_cols if c in features_df.columns]
    if cols_to_drop:
        logging.warning(f"特徴量からターゲット変数を除外: {cols_to_drop}")
        features_df = features_df.drop(columns=cols_to_drop)
    
    merged = features_df.merge(
        races_df[merge_keys + target_cols],
        on=merge_keys,
        how='inner'
    )
    
    # 欠損値除去
    merged = merged.dropna(subset=target_cols + ['race_id'])
    
    logging.info(f"マージ後: {len(merged)}行, {merged['race_id'].nunique()}レース")
    return merged

def get_feature_columns(df):
    """特徴量カラムを取得（メタデータとターゲットを除く）"""
    exclude = [
        'race_id', 'horse_id', 'horse_name', 'jockey_name', 'trainer_name',
        'race_date', 'year', 'month', 'day',
        'finish_position', 'finish_time_seconds'
    ] + DataLeakValidator.FORBIDDEN_COLUMNS
    
    feature_cols = [col for col in df.columns if col not in exclude]
    
    # 型チェックの前に現状をログ出力
    logging.info(f"除外前カラム数: {len(df.columns)}")
    logging.info(f"除外後候補数: {len(feature_cols)}")
    
    # 数値型のみ
    numeric_cols = [col for col in feature_cols if pd.api.types.is_numeric_dtype(df[col])]
    if len(numeric_cols) < len(feature_cols):
        non_numeric = set(feature_cols) - set(numeric_cols)
        logging.warning(f"非数値カラムが除外されました: {len(non_numeric)}個")
        logging.debug(f"除外されたカラム: {non_numeric}")
    
    feature_cols = numeric_cols
    
    # データリーク検証
    DataLeakValidator.validate_features(df, feature_cols)
    
    logging.info(f"最終特徴量数: {len(feature_cols)}")
    if len(feature_cols) == 0:
        logging.error("特徴量が0個です！データ型を確認してください。")
        logging.info("先頭5行のデータ型:")
        logging.info(df.dtypes.head(20))
        raise ValueError("特徴量がありません")
        
    return feature_cols

def time_series_cv_split(df, n_splits=5):
    """
    時系列クロスバリデーション分割
    """
    # race_dateでソート済みを前提
    race_dates = df['race_date'].unique()
    race_dates = sorted(race_dates)
    
    splits = []
    total_races = len(race_dates)
    
    if total_races < n_splits + 2:
        logging.warning(f"レース開催日数が少なすぎます: {total_races}日")
    
    for i in range(n_splits):
        # 訓練期間: 最初から i/n_splits の位置まで
        # 検証期間: i/n_splits から (i+1)/n_splits まで
        # 分割ロジックを修正して確実にデータが入るようにする
        train_end_idx = int(total_races * (i + 2) / (n_splits + 2))
        val_start_idx = train_end_idx
        val_end_idx = int(total_races * (i + 3) / (n_splits + 2))
        
        if val_end_idx > total_races:
            val_end_idx = total_races
        
        train_dates = race_dates[:train_end_idx]
        val_dates = race_dates[val_start_idx:val_end_idx]
        
        train_idx = df[df['race_date'].isin(train_dates)].index
        val_idx = df[df['race_date'].isin(val_dates)].index
        
        logging.info(f"  Fold {i+1}: Train={len(train_idx)}行 ({len(train_dates)}日), Val={len(val_idx)}行 ({len(val_dates)}日)")
        
        if len(train_idx) == 0 or len(val_idx) == 0:
            logging.warning(f"  Fold {i+1} のデータが空です！スキップします。")
            continue
            
        splits.append((train_idx, val_idx))
    
    return splits

def objective(trial, df, feature_cols):
    """Optuna目的関数"""
    
    # ハイパーパラメータの提案
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'num_leaves': trial.suggest_int('num_leaves', 20, 100),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000, step=100),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 1.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 1.0, log=True),
        'random_state': 42,
        'n_jobs': 1
    }
    
    # Time Series CV
    cv_splits = time_series_cv_split(df, n_splits=3)  # 3-foldで高速化
    scores = []
    
    if not cv_splits:
        raise ValueError("有効なCV分割が作成できませんでした")
    
    for fold_idx, (train_idx, val_idx) in enumerate(cv_splits):
        train_df = df.loc[train_idx]
        val_df = df.loc[val_idx]
        
        X_train = train_df[feature_cols]
        y_train = train_df['finish_time_seconds']
        X_val = val_df[feature_cols]
        y_val = val_df['finish_time_seconds']
        
        # データ形状チェック
        if X_train.empty or X_val.empty:
            logging.warning(f"Fold {fold_idx}: データが空です。X_train={X_train.shape}, X_val={X_val.shape}")
            continue
            
        # LightGBM学習
        model = lgb.LGBMRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            eval_metric='rmse',
            callbacks=[
                lgb.early_stopping(stopping_rounds=50, verbose=False),
                lgb.log_evaluation(0) # ログ出力を抑制
            ]
        )
        
        # 予測
        y_pred = model.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        scores.append(rmse)
        
        # Pruning (早期終了)
        trial.report(rmse, fold_idx)
        if trial.should_prune():
            raise optuna.TrialPruned()
    
    if not scores:
        return float('inf')
        
    mean_score = np.mean(scores)
    return mean_score

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n_trials', type=int, default=50, help='最適化試行回数')
    parser.add_argument('--timeout', type=int, default=10800, help='タイムアウト(秒)')
    args = parser.parse_args()
    
    logging.info("="*60)
    logging.info("μモデル ハイパーパラメータ最適化")
    logging.info("="*60)
    
    # データロード
    df = load_training_data()
    feature_cols = get_feature_columns(df)
    
    # Optuna最適化
    study = optuna.create_study(
        direction='minimize',
        sampler=TPESampler(seed=42),
        pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=5)
    )
    
    logging.info(f"\n最適化開始: {args.n_trials}試行, タイムアウト={args.timeout}秒")
    
    study.optimize(
        lambda trial: objective(trial, df, feature_cols),
        n_trials=args.n_trials,
        timeout=args.timeout,
        n_jobs=1  # LightGBMが並列化するので1で十分
    )
    
    # 結果
    logging.info("\n" + "="*60)
    logging.info("最適化完了")
    logging.info("="*60)
    logging.info(f"最良スコア (RMSE): {study.best_value:.6f}")
    logging.info(f"最良パラメータ:")
    for key, value in study.best_params.items():
        logging.info(f"  {key}: {value}")
    
    # 保存
    output_path = Path('keibaai/configs/optimized_params.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    result = {
        'best_score': study.best_value,
        'best_params': study.best_params,
        'n_trials': len(study.trials),
        'optimization_date': datetime.now().isoformat()
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    logging.info(f"\n✅ 最適パラメータを保存: {output_path}")
    
    # 最適化履歴の可視化（オプション）
    try:
        import optuna.visualization as vis
        fig1 = vis.plot_optimization_history(study)
        fig1.write_html('optuna_history.html')
        
        fig2 = vis.plot_param_importances(study)
        fig2.write_html('optuna_param_importance.html')
        
        logging.info("📊 可視化ファイルを保存: optuna_history.html, optuna_param_importance.html")
    except:
        logging.info("可視化スキップ（plotlyが必要）")

if __name__ == '__main__':
    main()
