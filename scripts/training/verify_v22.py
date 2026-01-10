"""
V22特徴量エンジン検証スクリプト

季節・競馬場特化特徴量の効果を4年Walk-forward検証で評価する。

【検証項目】
1. 単勝ROI比較（V15 Fixed vs V22）
2. 夏場（8-9月）ROI改善
3. 苦手競馬場（札幌/新潟/阪神）ROI改善
4. Train-Test Gap確認

【成功基準】
- 年間平均ROI: 83.9% → 85-88%
- 夏場ROI: 60-68% → 80%+
- 苦手競馬場ROI: 64-69% → 80%+
- Gap < 30%
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

from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
from keibaai.src.features.leak_free_feature_engineer_v22 import LeakFreeFeatureEngineerV22

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    data_dir = PROJECT_ROOT / 'data' / 'processed'
    
    logger.info("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / 'races.parquet')
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details.parquet')
    
    logger.info(f"  races: {len(races_df):,}件")
    logger.info(f"  pedigrees: {len(pedigrees_df):,}件")
    logger.info(f"  corners: {len(corners_df):,}件")
    logger.info(f"  race_details: {len(race_details_df):,}件")
    
    return races_df, pedigrees_df, corners_df, race_details_df


def calc_roi(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """ROI計算"""
    # 各レースでTop1予測を抽出
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    # 単勝ROI
    total_bet = len(top1)
    total_return = top1.loc[top1['finish_position'] == 1, 'win_odds'].sum()
    roi = (total_return / total_bet * 100) if total_bet > 0 else 0
    hit_rate = (top1['finish_position'] == 1).mean() * 100
    
    return {'roi': roi, 'hit_rate': hit_rate, 'bets': total_bet, 'return': total_return}


def calc_segment_roi(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """セグメント別ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    # 月と競馬場を抽出
    top1['race_date'] = pd.to_datetime(top1['race_date'], errors='coerce')
    top1['month'] = top1['race_date'].dt.month
    
    results = {}
    
    # 夏場（8-9月）
    summer = top1[top1['month'].isin([8, 9])]
    if len(summer) > 0:
        summer_return = summer.loc[summer['finish_position'] == 1, 'win_odds'].sum()
        results['summer_roi'] = (summer_return / len(summer) * 100)
        results['summer_bets'] = len(summer)
    else:
        results['summer_roi'] = 0
        results['summer_bets'] = 0
    
    # 苦手競馬場
    weak_venues = ['札幌', '新潟', '阪神']
    weak = top1[top1['venue'].isin(weak_venues)]
    if len(weak) > 0:
        weak_return = weak.loc[weak['finish_position'] == 1, 'win_odds'].sum()
        results['weak_venue_roi'] = (weak_return / len(weak) * 100)
        results['weak_venue_bets'] = len(weak)
    else:
        results['weak_venue_roi'] = 0
        results['weak_venue_bets'] = 0
    
    return results


def train_and_evaluate(train_df, test_df, feature_cols):
    """モデル学習・評価"""
    # ターゲット
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_test = (test_df['finish_position'] == 1).astype(int)
    
    X_train = train_df[feature_cols].fillna(0)
    X_test = test_df[feature_cols].fillna(0)
    
    # LightGBMパラメータ（V15と同じ）
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
    
    # 予測
    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df['pred'] = model.predict(X_train)
    test_df['pred'] = model.predict(X_test)
    
    # ROI計算
    train_roi = calc_roi(train_df)
    test_roi = calc_roi(test_df)
    segment_roi = calc_segment_roi(test_df)
    
    return {
        'train_roi': train_roi['roi'],
        'test_roi': test_roi['roi'],
        'gap': train_roi['roi'] - test_roi['roi'],
        'summer_roi': segment_roi['summer_roi'],
        'weak_venue_roi': segment_roi['weak_venue_roi'],
        'model': model,
    }


def run_walk_forward_validation(races_df, pedigrees_df, corners_df, race_details_df, 
                                 engine_class, engine_name: str):
    """4年Walk-forward検証"""
    logger.info(f"\n{'='*60}")
    logger.info(f"{engine_name} Walk-forward検証")
    logger.info(f"{'='*60}")
    
    # 年ごとの検証
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
        
        # 特徴量エンジン
        engine = engine_class()
        engine.fit(train_df, pedigrees_df, corners_df, race_details_df)
        
        train_features = engine.transform(train_df)
        test_features = engine.transform(test_df)
        
        feature_cols = engine.get_feature_columns()
        logger.info(f"  特徴量数: {len(feature_cols)}")
        
        # 学習・評価
        result = train_and_evaluate(train_features, test_features, feature_cols)
        result['year'] = test_year
        results.append(result)
        
        logger.info(f"  Train ROI: {result['train_roi']:.1f}%")
        logger.info(f"  Test ROI: {result['test_roi']:.1f}%")
        logger.info(f"  Gap: {result['gap']:.1f}%")
        logger.info(f"  夏場ROI: {result['summer_roi']:.1f}%")
        logger.info(f"  苦手競馬場ROI: {result['weak_venue_roi']:.1f}%")
    
    # 集計
    if results:
        avg_test_roi = np.mean([r['test_roi'] for r in results])
        avg_gap = np.mean([r['gap'] for r in results])
        avg_summer_roi = np.mean([r['summer_roi'] for r in results])
        avg_weak_venue_roi = np.mean([r['weak_venue_roi'] for r in results])
        
        logger.info(f"\n{engine_name} 4年平均:")
        logger.info(f"  Test ROI: {avg_test_roi:.1f}%")
        logger.info(f"  Gap: {avg_gap:.1f}%")
        logger.info(f"  夏場ROI: {avg_summer_roi:.1f}%")
        logger.info(f"  苦手競馬場ROI: {avg_weak_venue_roi:.1f}%")
        
        return {
            'engine_name': engine_name,
            'avg_test_roi': avg_test_roi,
            'avg_gap': avg_gap,
            'avg_summer_roi': avg_summer_roi,
            'avg_weak_venue_roi': avg_weak_venue_roi,
            'yearly_results': results
        }
    
    return None


def main():
    """メイン処理"""
    logger.info("V22特徴量エンジン検証開始")
    logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # データ読み込み
    races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    # V15 Fixed検証
    v15_results = run_walk_forward_validation(
        races_df, pedigrees_df, corners_df, race_details_df,
        LeakFreeFeatureEngineerV15Fixed, "V15 Fixed"
    )
    
    # V22検証
    v22_results = run_walk_forward_validation(
        races_df, pedigrees_df, corners_df, race_details_df,
        LeakFreeFeatureEngineerV22, "V22"
    )
    
    # 比較
    if v15_results and v22_results:
        logger.info(f"\n{'='*60}")
        logger.info("モデル比較")
        logger.info(f"{'='*60}")
        logger.info(f"\n{'指標':<20} {'V15 Fixed':>12} {'V22':>12} {'差分':>10}")
        logger.info("-" * 56)
        logger.info(f"{'Test ROI':<20} {v15_results['avg_test_roi']:>11.1f}% {v22_results['avg_test_roi']:>11.1f}% {v22_results['avg_test_roi'] - v15_results['avg_test_roi']:>+9.1f}%")
        logger.info(f"{'Gap':<20} {v15_results['avg_gap']:>11.1f}% {v22_results['avg_gap']:>11.1f}% {v22_results['avg_gap'] - v15_results['avg_gap']:>+9.1f}%")
        logger.info(f"{'夏場ROI':<20} {v15_results['avg_summer_roi']:>11.1f}% {v22_results['avg_summer_roi']:>11.1f}% {v22_results['avg_summer_roi'] - v15_results['avg_summer_roi']:>+9.1f}%")
        logger.info(f"{'苦手競馬場ROI':<20} {v15_results['avg_weak_venue_roi']:>11.1f}% {v22_results['avg_weak_venue_roi']:>11.1f}% {v22_results['avg_weak_venue_roi'] - v15_results['avg_weak_venue_roi']:>+9.1f}%")
        
        # 判定
        logger.info(f"\n{'='*60}")
        logger.info("評価結果")
        logger.info(f"{'='*60}")
        
        roi_improved = v22_results['avg_test_roi'] > v15_results['avg_test_roi']
        summer_improved = v22_results['avg_summer_roi'] > v15_results['avg_summer_roi']
        weak_improved = v22_results['avg_weak_venue_roi'] > v15_results['avg_weak_venue_roi']
        gap_ok = v22_results['avg_gap'] < 30
        
        logger.info(f"  ROI改善: {'✅' if roi_improved else '❌'}")
        logger.info(f"  夏場ROI改善: {'✅' if summer_improved else '❌'}")
        logger.info(f"  苦手競馬場ROI改善: {'✅' if weak_improved else '❌'}")
        logger.info(f"  Gap < 30%: {'✅' if gap_ok else '❌'}")
        
        if roi_improved and gap_ok:
            logger.info("\n🎉 V22は採用推奨！")
        else:
            logger.info("\n⚠️ V22は要検討（V15 Fixedがベースライン維持）")
    
    logger.info("\n検証完了")


if __name__ == "__main__":
    main()
