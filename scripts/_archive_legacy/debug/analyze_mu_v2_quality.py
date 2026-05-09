#!/usr/bin/env python3
"""
μv2.0 特徴量品質分析スクリプト
全特徴量の詳細な品質チェック（欠損率、0の割合、分布など）
"""
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

sys.path.insert(0, str(Path(__file__).parent / 'keibaai' / 'src'))
from features.feature_engine import FeatureEngine

def analyze_feature_quality():
    """特徴量の品質を詳細に分析"""
    
    logging.info("=" * 100)
    logging.info("μv2.0 特徴量品質分析")
    logging.info("=" * 100)
    
    # データ読み込み
    logging.info("\n1. データ読み込み...")
    
    shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    shutuba_df = pd.read_parquet(shutuba_path)
    shutuba_df['race_date'] = pd.to_datetime(shutuba_df['race_date'])
    
    # racesデータを読み込んでshutubaに結合（距離などの情報を付与）
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    if races_path.exists():
        races_df = pd.read_parquet(races_path)
        # 必要なカラムのみ
        races_cols = ['race_id', 'distance_m', 'track_surface', 'weather', 'track_condition', 'venue']
        # 重複カラムを除外
        races_cols = [c for c in races_cols if c not in shutuba_df.columns or c == 'race_id']
        shutuba_df = shutuba_df.merge(races_df[races_cols], on='race_id', how='left')
    
    # 2024年1月-3月（3ヶ月分）
    test_df = shutuba_df[
        (shutuba_df['race_date'] >= '2024-01-01') &
        (shutuba_df['race_date'] <= '2024-03-31')
    ].copy()
    
    results_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance_fixed.parquet')
    results_df = pd.read_parquet(results_path)
    results_df['race_date'] = pd.to_datetime(results_df['race_date'])
    history_df = results_df[results_df['race_date'] < '2024-01-01'].copy()
    
    profiles_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')
    profiles_df = pd.read_parquet(profiles_path) if profiles_path.exists() else pd.DataFrame()
    
    pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else pd.DataFrame()
    
    logging.info(f"  テストデータ: {len(test_df):,}行、{test_df['race_id'].nunique()}レース")
    
    # 特徴量生成
    logging.info("\n2. 特徴量生成...")
    config_path = Path('keibaai/configs/features.yaml')
    engine = FeatureEngine(config_path)
    
    features_df = engine.generate_features(
        shutuba_df=test_df,
        results_history_df=history_df,
        horse_profiles_df=profiles_df,
        pedigree_df=pedigree_df
    )
    
    logging.info(f"  生成完了: {len(features_df):,}行 × {len(features_df.columns)}列")
    
    # μv2.0特徴量を抽出
    logging.info("\n3. μv2.0特徴量の特定...")
    
    # カテゴリごとの特徴量パターン
    mu_v2_patterns = {
        'カテゴリ1（安定性）': [
            'horse_finish_cv_last5', 'horse_finish_range_last5', 'horse_finish_trend_last5',
            'horse_top3_rate_last10', 'horse_experience_log',
            'jockey_finish_cv_last10', 'trainer_finish_cv_last10',
            'horse_consistency_score', 'jockey_consistency_score', 'trainer_consistency_score'
        ],
        'カテゴリ2（条件適性）': [
            'distance_win_rate', 'distance_avg_finish', 'surface_avg_finish', 'surface_win_rate',
            'venue_win_rate', 'venue_top3_rate', 'condition_avg_finish', 'condition_win_rate',
            'weather_win_rate', 'weather_avg_finish',
            'jockey_distance_win_rate', 'jockey_surface_win_rate', 'jockey_venue_win_rate',
            'dist_surf_win_rate'
        ],
        'カテゴリ3（ペース適性）': [
            'horse_running_style', 'horse_early_position',
            'horse_avg_last_3f', 'race_expected_pace',
            'slow_pace_affinity', 'fast_pace_affinity',
            'passing_order2_std', 'passing_order4_std', 'horse_closing_kick_avg'
        ],
        'カテゴリ4（相性）': [
            'jockey_horse_avg_finish', 'jockey_horse_win_rate',
            'trainer_horse_avg_finish', 'trainer_horse_win_rate',
            'jockey_trainer_win_rate', 'jockey_trainer_avg_finish',
            'jockey_new_avg_finish', 'jockey_継続_avg_finish'
        ],
        'カテゴリ5（血統）': [
            'sire_dist_avg_finish', 'sire_surf_avg_finish',
            'damsire_dist_avg_finish', 'damsire_surf_avg_finish'
        ],
        'カテゴリ6（レース間隔）': [
            'is_after_long_rest', 'is_after_mid_rest', 'is_consecutive_race',
            'weight_change_from_last', 'weight_change_pct'
        ]
    }
    
    # 実際に存在する特徴量をカウント
    category_results = {}
    all_mu_v2_features = []
    
    for category, expected_features in mu_v2_patterns.items():
        found_features = [f for f in expected_features if f in features_df.columns]
        category_results[category] = {
            'expected': len(expected_features),
            'found': len(found_features),
            'features': found_features,
            'missing': [f for f in expected_features if f not in features_df.columns]
        }
        all_mu_v2_features.extend(found_features)
    
    # カテゴリ別レポート
    logging.info("\n4. カテゴリ別の特徴量数:")
    logging.info("=" * 100)
    total_expected = 0
    total_found = 0
    
    for category, result in category_results.items():
        total_expected += result['expected']
        total_found += result['found']
        status = "✅" if result['found'] == result['expected'] else "⚠️"
        logging.info(f"{status} {category}: {result['found']}/{result['expected']}特徴量")
        if result['missing']:
            logging.info(f"   欠落: {', '.join(result['missing'][:5])}")
    
    logging.info("=" * 100)
    logging.info(f"合計: {total_found}/{total_expected}特徴量 ({total_found/total_expected*100:.1f}%)")
    
    # 品質分析
    logging.info("\n5. 品質分析（μv2.0特徴量のみ）:")
    logging.info("=" * 100)
    
    quality_report = []
    
    for feature in all_mu_v2_features:
        if feature not in features_df.columns:
            continue
        
        series = features_df[feature]
        
        # 基本統計
        missing_rate = series.isnull().sum() / len(series)
        zero_rate = (series == 0).sum() / len(series) if pd.api.types.is_numeric_dtype(series) else 0
        
        # 非欠損データのみで統計計算
        valid_data = series.dropna()
        
        quality_info = {
            'feature': feature,
            'missing_rate': missing_rate,
            'zero_rate': zero_rate,
            'unique_count': series.nunique(),
            'mean': valid_data.mean() if len(valid_data) > 0 and pd.api.types.is_numeric_dtype(series) else None,
            'std': valid_data.std() if len(valid_data) > 0 and pd.api.types.is_numeric_dtype(series) else None
        }
        
        quality_report.append(quality_info)
    
    # DataFrame化
    quality_df = pd.DataFrame(quality_report)
    
    # 問題のある特徴量を特定
    logging.info("\n【高欠損率特徴量】（欠損率 > 50%）:")
    high_missing = quality_df[quality_df['missing_rate'] > 0.5].sort_values('missing_rate', ascending=False)
    if len(high_missing) > 0:
        for _, row in high_missing.head(10).iterrows():
            logging.info(f"  ⚠️  {row['feature']}: 欠損率{row['missing_rate']:.1%}")
    else:
        logging.info("  ✅ なし")
    
    logging.info("\n【高0率特徴量】（0の割合 > 80%）:")
    high_zero = quality_df[quality_df['zero_rate'] > 0.8].sort_values('zero_rate', ascending=False)
    if len(high_zero) > 0:
        for _, row in high_zero.head(10).iterrows():
            logging.info(f"  ⚠️  {row['feature']}: 0の割合{row['zero_rate']:.1%}")
    else:
        logging.info("  ✅ なし")
    
    logging.info("\n【低分散特徴量】（ユニーク値 < 5）:")
    low_variance = quality_df[quality_df['unique_count'] < 5].sort_values('unique_count')
    if len(low_variance) > 0:
        for _, row in low_variance.head(10).iterrows():
            logging.info(f"  ⚠️  {row['feature']}: ユニーク値{row['unique_count']}個")
    else:
        logging.info("  ✅ なし")
    
    # サマリー統計
    logging.info("\n6. サマリー統計:")
    logging.info("=" * 100)
    logging.info(f"平均欠損率: {quality_df['missing_rate'].mean():.1%}")
    logging.info(f"平均0率: {quality_df['zero_rate'].mean():.1%}")
    logging.info(f"平均ユニーク値: {quality_df['unique_count'].mean():.0f}個")
    
    # 詳細レポートをCSVに保存
    output_path = Path('mu_v2_quality_report.csv')
    quality_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logging.info(f"\n詳細レポートを保存: {output_path}")
    
    # 馬の経験値別の分析
    logging.info("\n7. 馬の経験値別の品質分析:")
    logging.info("=" * 100)
    
    if 'horse_experience_log' in features_df.columns:
        # 経験値でセグメント化
        features_df['experience_segment'] = pd.cut(
            features_df['horse_experience_log'],
            bins=[-np.inf, 0, 1, 2, np.inf],
            labels=['新馬', '2-3走', '4-7走', '8走以上']
        )
        
        for segment in ['新馬', '2-3走', '4-7走', '8走以上']:
            segment_df = features_df[features_df['experience_segment'] == segment]
            if len(segment_df) == 0:
                continue
            
            # カテゴリ1の特徴量の欠損率
            cat1_features = category_results['カテゴリ1（安定性）']['features']
            segment_missing = segment_df[cat1_features].isnull().mean().mean()
            
            logging.info(f"{segment}: {len(segment_df)}頭、カテゴリ1平均欠損率{segment_missing:.1%}")
    
    logging.info("\n" + "=" * 100)
    logging.info("✅ 品質分析完了")
    logging.info("=" * 100)
    
    return True

if __name__ == '__main__':
    try:
        analyze_feature_quality()
    except Exception as e:
        logging.error(f"エラー: {e}", exc_info=True)
        sys.exit(1)
