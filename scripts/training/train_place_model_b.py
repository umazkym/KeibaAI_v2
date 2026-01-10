"""
3着内予測モデル（モデルB）学習・評価スクリプト

V22特徴量を使用した複勝・馬連・ワイド向けの3着内予測モデル。

【モデルB概要】
- ターゲット: is_place (finish_position <= 3)
- 目的: 複勝ROIの向上、馬連・ワイドでの活用
- 特徴量: V22（季節・競馬場特化特徴量）

【評価指標】
- 複勝ROI（主指標）
- 3着内率
- Train-Test Gap

【券種活用】
- 複勝: モデルB予測Top1-3
- 馬連: V15 Top1 × モデルB Top2
- ワイド: モデルB Top2-3の組み合わせ
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit

# パス設定
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.features.leak_free_feature_engineer_v22 import LeakFreeFeatureEngineerV22

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    data_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet'
    
    logger.info("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / 'races' / 'races.parquet')
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees' / 'pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corners' / 'corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details' / 'race_details.parquet')
    returns_df = pd.read_parquet(data_dir / 'returns' / 'returns.parquet')
    
    # finish_positionがNAのレコードを除外（確定前のレース）
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    logger.info(f"  races: {len(races_df):,}件")
    logger.info(f"  returns: {len(returns_df):,}件")
    
    return races_df, pedigrees_df, corners_df, race_details_df, returns_df


def calc_place_roi(df: pd.DataFrame, pred_col: str = 'pred', returns_df: pd.DataFrame = None) -> dict:
    """複勝ROI計算"""
    df = df.copy()
    
    # 各レースでTop1予測を抽出
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    # 3着内率
    place_rate = (top1['finish_position'] <= 3).mean() * 100
    
    # 複勝払戻金を取得（returns_dfがある場合）
    if returns_df is not None:
        # 複勝（bet_type='fukusho'）の払戻金を結合
        fukusho = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
        fukusho = fukusho.rename(columns={'payout': 'place_payout'})
        
        # 払戻金がない場合は0
        top1 = top1.merge(fukusho, on=['race_id', 'horse_number'], how='left')
        top1['place_payout'] = top1['place_payout'].fillna(0)
        
        # 複勝ROI
        total_bet = len(top1) * 100  # 100円単位
        total_return = top1['place_payout'].sum()
        roi = (total_return / total_bet * 100) if total_bet > 0 else 0
    else:
        # 払戻金がない場合は3着内率から推定
        roi = place_rate * 0.7  # 目安
    
    return {
        'roi': roi,
        'place_rate': place_rate,
        'bets': len(top1),
    }


def calc_win_roi(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """単勝ROI計算（比較用）"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    total_bet = len(top1)
    total_return = top1.loc[top1['finish_position'] == 1, 'win_odds'].sum()
    roi = (total_return / total_bet * 100) if total_bet > 0 else 0
    hit_rate = (top1['finish_position'] == 1).mean() * 100
    
    return {'roi': roi, 'hit_rate': hit_rate, 'bets': total_bet}


def train_place_model(train_df, feature_cols, params=None):
    """3着内予測モデルの学習"""
    # ターゲット: 3着以内
    y_train = (train_df['finish_position'] <= 3).astype(int)
    X_train = train_df[feature_cols].fillna(0)
    
    # LightGBMパラメータ
    if params is None:
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'max_depth': 5,
            'learning_rate': 0.05,
            'min_child_samples': 50,
            'reg_alpha': 1.0,
            'reg_lambda': 1.0,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'verbose': -1,
        }
    
    train_data = lgb.Dataset(X_train, y_train)
    
    model = lgb.train(
        params,
        train_data,
        num_boost_round=200,
        callbacks=[lgb.log_evaluation(0)]
    )
    
    return model


def train_win_model(train_df, feature_cols, params=None):
    """単勝予測モデルの学習（比較用）"""
    # ターゲット: 1着
    y_train = (train_df['finish_position'] == 1).astype(int)
    X_train = train_df[feature_cols].fillna(0)
    
    # LightGBMパラメータ
    if params is None:
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'max_depth': 5,
            'learning_rate': 0.05,
            'min_child_samples': 50,
            'reg_alpha': 1.0,
            'reg_lambda': 1.0,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'verbose': -1,
        }
    
    train_data = lgb.Dataset(X_train, y_train)
    
    model = lgb.train(
        params,
        train_data,
        num_boost_round=200,
        callbacks=[lgb.log_evaluation(0)]
    )
    
    return model


def calc_place_rate(df: pd.DataFrame, pred_col: str = 'pred') -> float:
    """3着内率計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1]
    return (top1['finish_position'] <= 3).mean() * 100


def run_walk_forward_validation(races_df, pedigrees_df, corners_df, race_details_df, returns_df):
    """4年Walk-forward検証"""
    logger.info("\n" + "=" * 60)
    logger.info("3着内予測モデル（モデルB） Walk-forward検証")
    logger.info("=" * 60)
    
    years = [2021, 2022, 2023, 2024]
    results = []
    
    for test_year in years:
        train_end = f'{test_year}-01-01'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info(f"\n--- Test Year: {test_year} ---")
        
        # データ分割
        races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
        
        train_df = races_df[races_df['race_date'] < train_end].copy()
        test_df = races_df[
            (races_df['race_date'] >= test_start) & 
            (races_df['race_date'] <= test_end)
        ].copy()
        
        if len(train_df) < 1000 or len(test_df) < 1000:
            logger.warning(f"  データ不足: train={len(train_df)}, test={len(test_df)}")
            continue
        
        logger.info(f"  Train: {len(train_df):,}件, Test: {len(test_df):,}件")
        
        # 特徴量エンジン（V22）
        engine = LeakFreeFeatureEngineerV22()
        engine.fit(train_df, pedigrees_df, corners_df, race_details_df)
        
        train_features = engine.transform(train_df)
        test_features = engine.transform(test_df)
        
        feature_cols = engine.get_feature_columns()
        logger.info(f"  特徴量数: {len(feature_cols)}")
        
        # === 3着内予測モデル（モデルB）===
        place_model = train_place_model(train_features, feature_cols)
        
        X_train = train_features[feature_cols].fillna(0)
        X_test = test_features[feature_cols].fillna(0)
        
        train_features['pred_place'] = place_model.predict(X_train)
        test_features['pred_place'] = place_model.predict(X_test)
        
        # 複勝ROI計算
        train_place_rate = calc_place_rate(train_features, 'pred_place')
        test_place_roi = calc_place_roi(test_features, 'pred_place', returns_df)
        
        logger.info(f"  [モデルB] 複勝結果:")
        logger.info(f"    Train 3着内率: {train_place_rate:.1f}%")
        logger.info(f"    Test 3着内率: {test_place_roi['place_rate']:.1f}%")
        logger.info(f"    Test 複勝ROI: {test_place_roi['roi']:.1f}%")
        
        # === 単勝予測モデル（比較用）===
        win_model = train_win_model(train_features, feature_cols)
        
        train_features['pred_win'] = win_model.predict(X_train)
        test_features['pred_win'] = win_model.predict(X_test)
        
        train_win_roi = calc_win_roi(train_features, 'pred_win')
        test_win_roi = calc_win_roi(test_features, 'pred_win')
        
        logger.info(f"  [比較] 単勝モデル:")
        logger.info(f"    Train ROI: {train_win_roi['roi']:.1f}%")
        logger.info(f"    Test ROI: {test_win_roi['roi']:.1f}%")
        
        results.append({
            'year': test_year,
            'place_roi': test_place_roi['roi'],
            'place_rate': test_place_roi['place_rate'],
            'win_roi': test_win_roi['roi'],
            'win_hit_rate': test_win_roi['hit_rate'],
        })
    
    # 集計
    if results:
        logger.info("\n" + "=" * 60)
        logger.info("4年平均結果")
        logger.info("=" * 60)
        
        avg_place_roi = np.mean([r['place_roi'] for r in results])
        avg_place_rate = np.mean([r['place_rate'] for r in results])
        avg_win_roi = np.mean([r['win_roi'] for r in results])
        
        logger.info(f"\n  モデルB（3着内予測）:")
        logger.info(f"    平均複勝ROI: {avg_place_roi:.1f}%")
        logger.info(f"    平均3着内率: {avg_place_rate:.1f}%")
        
        logger.info(f"\n  単勝モデル（比較）:")
        logger.info(f"    平均単勝ROI: {avg_win_roi:.1f}%")
        
        return results
    
    return None


def main():
    """メイン処理"""
    logger.info("3着内予測モデル（モデルB）学習・評価開始")
    logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # データ読み込み
    races_df, pedigrees_df, corners_df, race_details_df, returns_df = load_data()
    
    # Walk-forward検証
    results = run_walk_forward_validation(
        races_df, pedigrees_df, corners_df, race_details_df, returns_df
    )
    
    if results:
        logger.info("\n" + "=" * 60)
        logger.info("年度別詳細")
        logger.info("=" * 60)
        
        for r in results:
            logger.info(f"\n{r['year']}年:")
            logger.info(f"  複勝ROI: {r['place_roi']:.1f}%, 3着内率: {r['place_rate']:.1f}%")
            logger.info(f"  単勝ROI: {r['win_roi']:.1f}%, 的中率: {r['win_hit_rate']:.1f}%")
    
    logger.info("\n学習・評価完了")


if __name__ == "__main__":
    main()
